from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import uvicorn

from project.script_ch_client.handler import handle_dg_request, handle_ch_request
from project.script_ch_client.nats_client import NatsClient


def main(config: dict) -> None:
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

    @app.get("/dg/request")
    async def dg_request():
        await handle_dg_request(nats_client)
        return JSONResponse({"status": "accepted"})

    @app.get("/ch/read")
    async def ch_read(query: str):
        result = await handle_ch_request(
            query,
            config["clickhouse"]
        )
        return JSONResponse(result)

    # Новый POST-эндпоинт для получения данных из веб-интерфейса
    @app.post("/data/receive")
    async def receive_data(request: Request):
        data = await request.json()  # Получаем данные из тела запроса
        print("Полученные данные:", data)  # Выводим данные в консоль
        return JSONResponse({"status": "success", "received_data": data})

    uvicorn.run(
        app,
        host=config["api"]["host"],
        port=config["api"]["port"],
        reload=False
    )
