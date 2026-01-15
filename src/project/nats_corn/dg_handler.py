import json

from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input

class DgHandler:
    def __init__(self, nc):
        self.client = DgClient()
        self.nc = nc

    def handle(self) -> None:
        raw_data = self.client.get_data()
        ips = parse_input(raw_data)

        payload = {
            "source": "DG",
            "ips": ips,
        }

        self.nc.publish(
            "ch.write.raw",
            json.dumps(payload).encode() # А зачем на тут json?
        )

