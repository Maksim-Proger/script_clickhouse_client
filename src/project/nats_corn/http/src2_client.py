from project.common.http.client import BaseHttpClient


class DgClient(BaseHttpClient):
    def __init__(self, config: dict):
        super().__init__()
        self.url = config["dg_client"]["url"]
        self.headers = config["dg_client"]["headers"]
        self.payload = config["dg_client"]["payload"]

    def get_data(self) -> str:
        return self.post(self.url, headers=self.headers, data=self.payload)

