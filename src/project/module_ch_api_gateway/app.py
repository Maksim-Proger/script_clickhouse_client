import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from project.module_ch_api_gateway.api.routers import clickhouse_router, auth_router, data_router, user_router
from project.module_ch_api_gateway.infrastructure.clickhouse_client import ClickHouseClient
from project.module_ch_api_gateway.infrastructure.db import DatabaseManager
from project.module_ch_api_gateway.infrastructure.nats_client import NatsInfrastructure
from project.module_ch_api_gateway.services.user_service import UserService

logger = logging.getLogger("ch-api-gateway")


def create_app(config: dict) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        async def on_db_connected():
            await app.state.user_service.seed_admin()
            await app.state.user_service.load_revoked_jtis()
            app.state.user_service.start_cleanup_loop()

        connected = await app.state.db.connect_safe()
        if connected:
            await on_db_connected()
        else:
            app.state.db.start_reconnect_loop(on_connect=on_db_connected)

        await app.state.nats_infra.connect()
        logger.info("action=nats_connect status=success")
        try:
            yield
        finally:
            app.state.user_service.stop_cleanup_loop()
            await app.state.nats_infra.close()
            logger.info("action=nats_disconnect status=success")
            await app.state.db.close()

            await app.state.ch_client.close()
            logger.info("action=ch_client_close status=success")

    app = FastAPI(lifespan=lifespan)

    app.state.config = config

    pg = config["postgres"]
    app.state.db = DatabaseManager(
        dsn=f"postgresql://{pg['user']}:{pg['password']}@{pg['host']}:{pg['port']}/{pg['database']}",
        min_size=pg.get("min_connections", 2),
        max_size=pg.get("max_connections", 10),
    )
    app.state.user_service = UserService(app.state.db)

    app.state.ch_client = ClickHouseClient(
        host=config["clickhouse"]["host"],
        port=config["clickhouse"]["http_port"],
        timeout_sec=config["clickhouse"]["timeout_sec"],
        user=config["clickhouse"].get("user", "default"),
        password=config["clickhouse"].get("password", ""),
    )
    app.state.nats_infra = NatsInfrastructure(url=config["nats"]["url"])

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config["cors"]["allow_origins"],
        allow_credentials=config["cors"]["allow_credentials"],
        allow_methods=config["cors"]["allow_methods"],
        allow_headers=config["cors"]["allow_headers"],
    )

    @app.exception_handler(Exception)
    async def global_exception_handler(request: Request, exc: Exception):
        logger.error("action=unhandled_exception error=%s path=%s", str(exc), request.url.path)
        return JSONResponse(
            status_code=500,
            content={"detail": "Internal server error"}
        )

    app.include_router(auth_router.router)
    app.include_router(user_router.router)
    app.include_router(clickhouse_router.router)
    app.include_router(data_router.router)

    return app
