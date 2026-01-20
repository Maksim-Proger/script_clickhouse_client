# import json
# from nats.aio.client import Client as NATS
#
#
# class NatsClient:
#     def __init__(self, url: str = "nats://localhost:4222"):
#         self.url = url
#
#     async def publish_dg_load(self) -> None:
#         nc = NATS()
#         await nc.connect(self.url)
#
#         payload = {
#             "action": "load"
#         }
#
#         await nc.publish(
#             "dg.load.command",
#             json.dumps(payload).encode()
#         )
#
#         await nc.close()

import json
from nats.aio.client import Client as NATS

class NatsClient:
    def __init__(self, url: str = "nats://localhost:4222"):
        self.url = url

    async def publish_dg_load(self) -> None:
        nc = NATS()
        await nc.connect(self.url)

        payload = {
            "action": "load"
        }

        js = nc.jetstream()  # подключаем JetStream
        await js.publish(
            "dg.load.command",
            json.dumps(payload).encode()
        )

        await nc.close()
