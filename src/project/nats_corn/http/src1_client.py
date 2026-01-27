from project.common.http.client import BaseHttpClient


class AbClient(BaseHttpClient):
    def __init__(self, url: str):
        super().__init__()
        self.url = url

    def get_data(self) -> str:
        return self.get(self.url)

