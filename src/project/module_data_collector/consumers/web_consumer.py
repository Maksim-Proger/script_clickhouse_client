import asyncio
import json
import logging
from nats.aio.client import Client as NatsClient

from project.utils.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import build_manual_records
from project.module_data_collector.publisher import publish_records

logger = logging.getLogger("data-collector.web_consumer")

DEFAULT_SOURCE = "web_interface"


class NatsWebConsumer:
    def __init__(self, nc: NatsClient, config: dict, lifecycle: Lifecycle):
        self.nc = nc
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.subject = config["nats"]["web_consumer"]["subject"]
        self.durable = config["nats"]["web_consumer"]["durable"]
        self.raw_subject = config["nats"]["raw_subject"]
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return
        try:
            logger.debug("action=web_data_received size=%d", len(msg.data))

            try:
                payload = json.loads(msg.data.decode())
            except (ValueError, UnicodeDecodeError) as e:
                logger.error("action=web_message_broken error=%s", str(e))
                await msg.term()
                return

            if isinstance(payload, dict):
                items = payload.get("records")
                source = str(payload.get("source") or "").strip() or DEFAULT_SOURCE
                profile = str(payload.get("profile") or "").strip()
            else:
                items = payload
                source = DEFAULT_SOURCE
                profile = ""

            if not isinstance(items, list):
                logger.error("action=web_message_broken error=records is not a list")
                await msg.term()
                return

            records = await asyncio.to_thread(build_manual_records, items, source, profile, self.dt_format)

            if records:
                await publish_records(self.nc, self.raw_subject, records, self.lifecycle)

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
