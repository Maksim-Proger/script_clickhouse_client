from project.common.http.async_client import BaseAsyncHttpClient


class DgClient(BaseAsyncHttpClient):
    def __init__(self, config: dict, timeout: int = 10):
        super().__init__(timeout=timeout)
        self.url = config["dg_client"]["url"]
        self.headers = config["dg_client"]["headers"]
        self.payload = config["dg_client"]["payload"]

    async def get_data(self) -> str:
        return await self.post(
            self.url,
            headers=self.headers,
            data=self.payload,
        )
