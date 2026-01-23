from fastapi import FastAPI
from fastapi.responses import JSONResponse
import uvicorn

from project.script_ch_client.handler import handle_dg_request, handle_ch_request

app = FastAPI()

@app.get("/dg/request")
async def dg_request():
    await handle_dg_request()
    return JSONResponse({"status": "accepted"})

@app.get("/ch/read")
async def ch_read(query: str):
    result = await handle_ch_request(query)
    return JSONResponse(result)

def main() -> None:
    uvicorn.run(
        "project.script_ch_client.main:app",
        host="0.0.0.0",
        port=8000,
        reload=False
    )

if __name__ == "__main__":
    main()
