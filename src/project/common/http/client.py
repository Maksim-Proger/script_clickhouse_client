import requests
from .utils import safe_parse_json

class BaseHttpClient:
    def __init__(self, timeout: int = 10):
        self.timeout = timeout

    def get(self, url: str, headers: dict | None = None) -> str:
        response = requests.get(url, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def post(self, url: str, headers: dict | None = None, data: dict | None = None) -> str:
        response = requests.post(url, headers=headers, json=data, timeout=self.timeout)
        response.raise_for_status()
        return response.text

    def get_json(self, url: str, headers: dict | None = None) -> dict | list | None:
        return safe_parse_json(self.get(url, headers=headers))

    def post_json(self, url: str, headers: dict | None = None, data: dict | None = None) -> dict | list | None:
        return safe_parse_json(self.post(url, headers=headers, data=data))
