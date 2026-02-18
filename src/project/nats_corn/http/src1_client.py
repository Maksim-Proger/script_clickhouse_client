from project.utils.http.async_client import BaseAsyncHttpClient


class AbClient(BaseAsyncHttpClient):
    def __init__(self, url: str, timeout: int = 10):
        super().__init__(timeout=timeout)
        self.url = url

    async def get_data(self) -> str:
        return await self.get(self.url)
