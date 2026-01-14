from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from project.script_ch_client.handler import handle_dg_request

app = FastAPI()

@app.get("/dg/request")
async def dg_request():
    await handle_dg_request()
    return JSONResponse({"status": "accepted"})


if __name__ == "__main__":
    uvicorn.run(
        "project.script_ch_client.main:app",
        host="127.0.0.1",
        port=8000,
        reload=True
    )
