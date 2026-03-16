import logging
import time
import httpx
import base64

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
        import base64

        sql = f"{query} FORMAT JSON"
        start_time = time.perf_counter()

        auth_value = base64.b64encode(f"{self.user}:{self.password}".encode()).decode()
        headers = {
            "Authorization": f"Basic {auth_value}",
            "Content-Type": "application/octet-stream"
        }

        try:
            resp = await self._get_client().post(
                self.url,
                content=sql,
                headers=headers
            )

            resp.raise_for_status()

            duration = time.perf_counter() - start_time
            logger.info("action=ch_query_success duration=%.2fs query=\"%s\"", duration, query[:100])

            return resp.json()

        except httpx.TimeoutException:
            logger.error("action=ch_query_timeout query=\"%s\"", query[:100])
            raise

        except httpx.HTTPStatusError as e:
            logger.error("action=ch_query_failed status=%s error=%s", e.response.status_code, e.response.text)
            raise

        except Exception as e:
            logger.error("action=ch_query_failed error=%s", str(e))
            raise

    async def close(self):
        if self._client:
            await self._client.aclose()
            self._client = None
