from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn
import logging

import project.module_ch_api_gateway.auth as auth
from project.module_ch_api_gateway.auth import create_access_token, get_current_user
from project.module_ch_api_gateway.handler import handle_dg_request, handle_ch_request, handle_web_data
from project.module_ch_api_gateway.nats_client import NatsClient
from project.utils.logging_formatter import setup_logging
from project.module_ch_api_gateway.ch_handler import CHReadFilters

logger = logging.getLogger("ch-client")

def main(config: dict) -> None:
    setup_logging("ch-client")
    logger.info("action=api_init status=starting host=%s port=%d",
                config["api"]["host"], config["api"]["port"])

    if "auth" in config:
        auth.SECRET_KEY = config["auth"].get("secret_key", auth.SECRET_KEY)
        auth.STATIC_API_TOKEN = config["auth"].get("static_token", auth.STATIC_API_TOKEN)

    nats_client = NatsClient(
        url=config["nats"]["url"],
        dg_subject=config["nats"]["dg_subject"]
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await nats_client.connect()
        logger.info("action=nats_connect status=success")

        try:
            yield
        finally:
            await nats_client.close()
            logger.info("action=nats_disconnect status=success")

    app = FastAPI(lifespan=lifespan)

    app.add_middleware(
        CORSMiddleware,
        allow_origins=config["cors"]["allow_origins"],
        allow_credentials=config["cors"]["allow_credentials"],
        allow_methods=config["cors"]["allow_methods"],
        allow_headers=config["cors"]["allow_headers"],
    )

    @app.post("/login")
    async def login(request: Request):
        data = await request.json()
        if data.get("login") == "admin" and data.get("password") == "admin":
            token = create_access_token({"sub": "admin"})
            return {"access_token": token, "token_type": "bearer"}
        raise HTTPException(status_code=401, detail="Invalid credentials")

    @app.post("/ch/read")
    async def ch_read(filters: CHReadFilters, user: dict = Depends(get_current_user)):
        result = await handle_ch_request(filters, config["clickhouse"])
        return JSONResponse(result)

    @app.post("/dg/request")
    async def dg_request(request: Request, user: dict = Depends(get_current_user)):
        data = await request.json()
        await handle_dg_request(nats_client, data)
        return JSONResponse({"status": "accepted"})

    @app.post("/data/receive")
    async def receive_data(request: Request, user: dict = Depends(get_current_user)):
        data = await request.json()
        await handle_web_data(nats_client, data)
        return JSONResponse({"status": "success"})

    uvicorn.run(
        app,
        host=config["api"]["host"],
        port=config["api"]["port"],
        reload=False
    )
