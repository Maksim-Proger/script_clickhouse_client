import asyncio
from nats.aio.client import Client as NATS
from project.nats_corn.dg_handler import DgHandler

class NatsDgConsumer:
    def __init__(self, nc: NATS, config: dict):
        self.nc = nc
        self.dg_handler = DgHandler(self.nc, config)
        self.subject = config["nats"]["dg_consumer"]["subject"]
        self.durable = config["nats"]["dg_consumer"]["durable"]

    async def start(self) -> None:
        js = self.nc.jetstream()  # подключаем JetStream

        async def callback(msg):
            await self.dg_handler.handle()
            await msg.ack()

        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=callback
        )

        await asyncio.Event().wait()

