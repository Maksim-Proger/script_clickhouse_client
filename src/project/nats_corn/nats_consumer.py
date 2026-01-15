import asyncio

from nats.aio.client import Client as NATS
from project.nats_corn.dg_handler import DgHandler

class NatsDgConsumer:
    def __init__(self, nc: NATS):
        self.nc = nc
        self.dg_handler = DgHandler(self.nc)

    async def start(self) -> None:
        async def callback(msg):
            # команда пришла → обрабатываем DG
            await self.dg_handler.handle()

        await self.nc.subscribe("dg.load.command", cb=callback)

        # держим consumer живым
        await asyncio.Event().wait()

