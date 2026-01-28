import httpx

async def read_from_clickhouse(
    query: str,
    host: str,
    port: int,
    timeout_sec: int,
) -> dict:
    url = f"http://{host}:{port}/"
    sql = f"{query} FORMAT JSON"

    params = {"query": sql}

    async with httpx.AsyncClient(timeout=timeout_sec) as client:
        resp = await client.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
