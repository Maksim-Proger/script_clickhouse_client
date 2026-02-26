import httpx
import time
import logging

logger = logging.getLogger("ch-client")

class ClickHouseClient:
    def __init__(self, host: str, port: int, timeout_sec: int):
        self.url = f"http://{host}:{port}/"
        self.timeout = timeout_sec

    async def fetch_json(self, query: str) -> dict:
        sql = f"{query} FORMAT JSON"
        start_time = time.perf_counter()
        try:
            async with httpx.AsyncClient(timeout=self.timeout) as client:
                resp = await client.get(self.url, params={"query": sql})
                resp.raise_for_status()

                duration = time.perf_counter() - start_time
                logger.info("action=ch_query_success duration=%.2fs query=\"%s\"", duration, query[:100])
                return resp.json()
        except httpx.TimeoutException:
            logger.error("action=ch_query_timeout query=\"%s\"", query[:100])
            raise
        except Exception as e:
            logger.error("action=ch_query_failed error=%s", str(e))
            raise