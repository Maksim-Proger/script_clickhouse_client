import asyncio
from nats.aio.client import Client as NATS
from project.nats_corn.dg_handler import DgHandler


class NatsDgConsumer:
    def __init__(self):
        self.dg_handler = DgHandler()

    async def start(self):
        nc = NATS()
        await nc.connect("nats://localhost:4222")

        async def callback(msg):
            print("DG load command received")
            self.dg_handler.handle()

        await nc.subscribe("dg.load.command", cb=callback)

        await asyncio.Event().wait()
