from fastapi import FastAPI
from contextlib import asynccontextmanager
from fastapi.middleware.cors import CORSMiddleware
import logging

from .infrastructure.clickhouse_client import ClickHouseClient
from .infrastructure.nats_client import NatsInfrastructure
from .api.routers import clickhouse_router, auth_router, data_router

logger = logging.getLogger("ch-api-gateway")


def create_app(config: dict) -> FastAPI:
    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await app.state.nats_infra.connect()
        logger.info("action=nats_connect status=success")
        try:
            yield
        finally:
            await app.state.nats_infra.close()
            logger.info("action=nats_disconnect status=success")

    app = FastAPI(lifespan=lifespan)

    app.state.config = config

    app.state.ch_client = ClickHouseClient(
        host=config["clickhouse"]["host"],
        port=config["clickhouse"]["http_port"],
        timeout_sec=config["clickhouse"]["timeout_sec"]
    )
    app.state.nats_infra = NatsInfrastructure(url=config["nats"]["url"])

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config["cors"]["allow_origins"],
        allow_credentials=config["cors"]["allow_credentials"],
        allow_methods=config["cors"]["allow_methods"],
        allow_headers=config["cors"]["allow_headers"],
    )

    app.include_router(auth_router.router)
    app.include_router(clickhouse_router.router)
    app.include_router(data_router.router)

    return app