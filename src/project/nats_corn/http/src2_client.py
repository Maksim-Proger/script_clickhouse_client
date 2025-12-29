from project.common.http.client import BaseHttpClient

# Для DG

DG_URL = "http://192.168.100.13:3333/api/inspector/marks"
HEADERS = {
    "User-Arena": "first",
    "Content-Type": "application/json",
    "Authorization": "Bearer eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9..."
}
PAYLOAD = {
    "data": {"id": "201", "value": "1", "type": "shost"},
    "name": "feed-gen",
    "action": "list"
}

class DgClient(BaseHttpClient):
    def get_data(self) -> str:
        return self.post(DG_URL, headers=HEADERS, data=PAYLOAD)

