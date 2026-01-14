from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input


class DgHandler:
    def __init__(self):
        self.client = DgClient()

    def handle(self) -> None:
        try:
            raw_data = self.client.get_data()
            ips = parse_input(raw_data)
            print("DG:", ";".join(ips))
        except Exception as e:
            print(f"DG error: {e}")
