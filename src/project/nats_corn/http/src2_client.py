from project.common.http.async_client import BaseAsyncHttpClient


class DgClient(BaseAsyncHttpClient):
    def __init__(self, config: dict, timeout: int = 10):
        super().__init__(timeout=timeout)
        self.url = config["dg_client"]["url"]
        self.headers = config["dg_client"]["headers"]
        self.payload = config["dg_client"]["payload"]

    async def get_data(self, ui_params: dict) -> str:
        final_payload = self.payload.copy()
        if ui_params:
            final_payload.update(ui_params)
            final_payload["action"] = "list"

        return await self.post(
            self.url,
            headers=self.headers,
            data=final_payload,
        )
