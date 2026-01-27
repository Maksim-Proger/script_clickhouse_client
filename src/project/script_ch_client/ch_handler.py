import httpx

CLICKHOUSE_HOST = "192.168.100.113"
CLICKHOUSE_HTTP_PORT = 8123

async def read_from_clickhouse(query: str) -> dict:
    url = f"http://{CLICKHOUSE_HOST}:{CLICKHOUSE_HTTP_PORT}/"

    sql = f"{query} FORMAT JSON"

    params = {
        "query": sql
    }

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
