from typing import Optional

import httpx


class BaseAsyncHttpClient:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout
        self._client: Optional[httpx.AsyncClient] = None

    async def connect(self) -> None:
        if not self._client:
            self._client = httpx.AsyncClient(timeout=self.timeout)

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    async def get(self, url: str, headers: dict | None = None) -> str:
        if not self._client:
            raise RuntimeError("HTTP client is not connected")

        resp = await self._client.get(url, headers=headers)
        resp.raise_for_status()
        return resp.text

    async def post(
            self,
            url: str,
            headers: dict | None = None,
            data: dict | None = None,
    ) -> str:
        if not self._client:
            raise RuntimeError("HTTP client is not connected")

        resp = await self._client.post(url, headers=headers, json=data)
        resp.raise_for_status()
        return resp.text
