import asyncio
from nats.aio.client import Client as NATS

from project.nats_corn.dg_handler import DgHandler


class NatsDgConsumer:
    def __init__(self, nc: NATS, config: dict, shutdown_event: asyncio.Event):
        self.nc = nc
        self.dg_handler = DgHandler(self.nc, config)
        self.subject = config["nats"]["dg_consumer"]["subject"]
        self.durable = config["nats"]["dg_consumer"]["durable"]
        self.shutdown_event = shutdown_event
        self.is_shutting_down = False

    async def start(self) -> None:
        js = self.nc.jetstream()

        async def callback(msg):
            if self.shutdown_event.is_set():
                await msg.nak()
                return

            try:
                await self.dg_handler.handle()
                await msg.ack()
            except Exception:
                await msg.nak()

        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=callback
        )

        await self.shutdown_event.wait()
