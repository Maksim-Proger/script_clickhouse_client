import json
from nats.aio.client import Client as NatsClientLib

class NatsClient:
    def __init__(self, url: str, dg_subject: str):
        self.url = url
        self.dg_subject = dg_subject

    async def publish_dg_load(self) -> None:
        nc = NatsClientLib()
        await nc.connect(self.url)

        payload = {"action": "load"}

        js = nc.jetstream()
        await js.publish(
            self.dg_subject,
            json.dumps(payload).encode()
        )

        await nc.close()
