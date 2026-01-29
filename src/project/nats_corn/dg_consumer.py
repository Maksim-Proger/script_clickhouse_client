import json
from nats.aio.client import Client as NATS

from project.nats_corn.dg_handler import DgHandler
from project.nats_corn.lifecycle import Lifecycle


class NatsDgConsumer:
    def __init__(
        self,
        nc: NATS,
        config: dict,
        lifecycle: Lifecycle,
    ):
        self.nc = nc
        self.handler = DgHandler(config)
        self.subject = config["nats"]["dg_consumer"]["subject"]
        self.durable = config["nats"]["dg_consumer"]["durable"]
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return

        try:
            payload = json.loads(msg.data.decode())

            if payload.get("action") != "load":
                await msg.ack()
                return

            records = await self.handler.fetch()

            for record in records:
                await self.nc.publish(
                    "ch.write.raw",
                    json.dumps(record).encode()
                )

            await msg.ack()
        except Exception:
            await msg.nak()

    async def start(self) -> None:
        js = self.nc.jetstream()

        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=self.handle_msg
        )

        await self.lifecycle.shutdown_event.wait()
