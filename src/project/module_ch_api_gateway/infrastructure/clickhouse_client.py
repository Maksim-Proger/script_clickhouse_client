import logging
import time
import httpx

logger = logging.getLogger("ch-api-gateway")


class ClickHouseClient:
    def __init__(self, host: str, port: int, timeout_sec: int, user: str, password: str):
        self.url = f"http://{host}:{port}/"
        self.user = user
        self.password = password
        self.timeout_sec = timeout_sec
        self._client: httpx.AsyncClient | None = None

    def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=self.timeout_sec)
        return self._client

    async def fetch_json(self, query: str) -> dict:
        sql = f"{query} FORMAT JSON"
        start_time = time.perf_counter()
        try:
            resp = await self._get_client().get(
                self.url,
                params={"query": sql, "max_rows_to_read": 0},  # ← сохранили из оригинала
                auth=(self.user, self.password)
            )
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

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None