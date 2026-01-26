import json

from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input

class DgHandler:
    def __init__(self, nc):
        self.client = DgClient()
        self.nc = nc

    async def handle(self) -> None:
        raw_data = self.client.get_data()
        ips = parse_input(raw_data,source="dosgate")

        for record in ips:
            await self.nc.publish(
                "ch.write.raw",
                json.dumps(record).encode()
            )

