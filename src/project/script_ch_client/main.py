from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

import project.script_ch_client.auth as auth
from project.script_ch_client.auth import create_access_token, get_current_user
from project.script_ch_client.handler import handle_dg_request, handle_ch_request, handle_web_data
from project.script_ch_client.nats_client import NatsClient


def main(config: dict) -> None:
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
        try:
            yield
        finally:
            await nats_client.close()

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

    @app.get("/ch/read")
    async def ch_read(query: str, user: dict = Depends(get_current_user)):
        result = await handle_ch_request(query, config["clickhouse"])
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
