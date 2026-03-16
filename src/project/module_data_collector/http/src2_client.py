from project.utils.http.async_client import BaseAsyncHttpClient


class DgClient(BaseAsyncHttpClient):
    def __init__(self, timeout: int = 10, verify_ssl: bool = True):
        super().__init__(timeout=timeout, verify_ssl=verify_ssl)

    async def fetch_data(self, url: str, headers: dict, payload: dict) -> str:
        return await self.post(url, headers=headers, data=payload)
