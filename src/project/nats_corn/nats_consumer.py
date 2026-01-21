import asyncio

from nats.aio.client import Client as NATS
from project.nats_corn.dg_handler import DgHandler

class NatsDgConsumer:
    def __init__(self, nc: NATS):
        self.nc = nc
        self.dg_handler = DgHandler(self.nc)

    async def start(self) -> None:
        js = self.nc.jetstream()  # подключаем JetStream

        async def callback(msg):
            await self.dg_handler.handle()
            await msg.ack()

        await js.subscribe(
            subject="dg.load.command",
            durable="dg_consumer",
            cb=callback
        )

        await asyncio.Event().wait()
