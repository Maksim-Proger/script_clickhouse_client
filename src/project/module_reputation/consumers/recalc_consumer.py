import asyncio
import json
import logging
from nats.aio.client import Client as NatsClient
from typing import Optional

from project.module_reputation.core.reputation_job import ReputationJob

logger = logging.getLogger("reputation.recalc_consumer")

CONNECT_RETRY_SEC = 10


class NatsRecalcConsumer:
    def __init__(self, config: dict, job: ReputationJob):
        self.url = config["nats"]["url"]
        self.recalc_subject = config["nats"]["recalc_consumer"]["subject"]
        self.status_subject = config["nats"]["status_consumer"]["subject"]
        self.job = job
        self.nc: Optional[NatsClient] = None

    async def handle_recalc(self, msg) -> None:
        status = "started" if self.job.request_manual() else "busy"
        logger.info("action=recalc_requested status=%s", status)
        if msg.reply:
            await self.nc.publish(msg.reply, json.dumps({"status": status}).encode())

    async def handle_status(self, msg) -> None:
        if msg.reply:
            await self.nc.publish(msg.reply, json.dumps({"running": self.job.is_running}).encode())

    async def start(self) -> None:
        while self.nc is None:
            nc = NatsClient()
            try:
                await nc.connect(self.url, max_reconnect_attempts=-1)
                self.nc = nc
            except Exception as e:
                logger.warning("action=nats_connect_failed error=%r retry_in_sec=%d", e, CONNECT_RETRY_SEC)
                await asyncio.sleep(CONNECT_RETRY_SEC)

        await self.nc.subscribe(self.recalc_subject, cb=self.handle_recalc)
        await self.nc.subscribe(self.status_subject, cb=self.handle_status)
        logger.info(
            "action=recalc_consumer_init recalc_subject=%s status_subject=%s",
            self.recalc_subject, self.status_subject,
        )

    async def stop(self) -> None:
        if self.nc and self.nc.is_connected:
            await self.nc.close()
