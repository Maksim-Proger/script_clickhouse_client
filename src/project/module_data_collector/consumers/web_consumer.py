import asyncio
import logging

from nats.aio.client import Client as NatsClient

from project.module_data_collector.dg_manager import _publish_records
from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import parse_input

logger = logging.getLogger("data-collector.web_consumer")


class NatsWebConsumer:
    def __init__(
            self,
            nc: NatsClient,
            config: dict,
            lifecycle: Lifecycle,
    ):
        self.nc = nc
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.subject = "data.received"
        self.durable = "web_consumer_durable"
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return
        try:
            logger.debug("action=web_data_received size=%d", len(msg.data))

            loop = asyncio.get_running_loop()
            records = await loop.run_in_executor(
                None,
                lambda: parse_input(
                    data=msg.data.decode(),
                    source="web_interface",
                    dt_format=self.dt_format,
                )
            )

            if records:
                await _publish_records(self.nc, records, self.lifecycle)

            await msg.ack()

        except Exception as e:
            logger.error("action=web_consumer_failed error=%s", str(e))
            await msg.nak()

    async def start(self) -> None:
        js = self.nc.jetstream()
        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=self.handle_msg
        )
        await self.lifecycle.shutdown_event.wait()
