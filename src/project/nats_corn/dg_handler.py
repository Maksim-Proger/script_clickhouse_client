from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input


class DgHandler:
    def __init__(self, config: dict):
        self.client = DgClient(config)

    async def fetch(self, ui_params: dict) -> list[dict]:
        await self.client.connect()
        try:
            raw_data = await self.client.get_data(ui_params)
            return parse_input(raw_data, source="dosgate")
        finally:
            await self.client.close()
