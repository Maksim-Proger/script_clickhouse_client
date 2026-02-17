from project.common.http.async_client import BaseAsyncHttpClient

class DgClient(BaseAsyncHttpClient):
    # Убираем сохранение конфига в self
    def __init__(self, timeout: int = 10):
        super().__init__(timeout=timeout)

    # Метод теперь универсальный
    async def fetch_data(self, url: str, headers: dict, payload: dict) -> str:
        # Используем метод post из базового класса BaseAsyncHttpClient
        return await self.post(url, headers=headers, data=payload)
