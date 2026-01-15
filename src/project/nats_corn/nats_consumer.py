import asyncio

from nats.aio.client import Client as NATS
from project.nats_corn.dg_handler import DgHandler

class NatsDgConsumer:
    def __init__(self):
        self.nc = NATS()
        self.dg_handler = DgHandler(self.nc)

    async def start(self):
        await self.nc.connect("nats://localhost:4222")

        async def callback(msg):
            self.dg_handler.handle()

        await self.nc.subscribe("dg.load.command", cb=callback)
        await asyncio.Event().wait()

