from project.common.http.client import BaseHttpClient

# Для AB

AB_URL = "http://192.168.100.13/common_ipban"

class AbClient(BaseHttpClient):
    def get_data(self) -> str:
        return self.get(AB_URL)

