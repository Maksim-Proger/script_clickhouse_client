import json
import logging

from nats.aio.client import Client as NatsClient

from project.module_data_collector.dg_manager import DgSourceManager
from project.module_data_collector.lifecycle import Lifecycle

logger = logging.getLogger("data-collector")


class NatsDgConsumer:
    def __init__(
            self,
            nc: NatsClient,
            config: dict,
            lifecycle: Lifecycle,
            dg_manager: DgSourceManager,
    ):
        self.nc = nc
        self.dg_manager = dg_manager
        self.subject = config["nats"]["dg_consumer"]["subject"]
        self.durable = config["nats"]["dg_consumer"]["durable"]
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return

        try:
            payload = json.loads(msg.data.decode())
            action = payload.get("action")

            logger.info("action=msg_received subject=%s command=%s", self.subject, action)

            if action == "load":
                await self.dg_manager.run_manual(payload)

            await msg.ack()
        except Exception as e:
            logger.error("action=consumer_error subject=%s error=%s", self.subject, str(e))
            await msg.nak()

    async def start(self) -> None:
        js = self.nc.jetstream()

        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=self.handle_msg
        )

        await self.lifecycle.shutdown_event.wait()
