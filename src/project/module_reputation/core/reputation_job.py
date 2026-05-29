import asyncio
import logging

from project.module_reputation.infrastructure.ch_client import ReputationCHClient
from project.module_reputation.lifecycle import Lifecycle

logger = logging.getLogger(__name__)


class ReputationJob:
    def __init__(self, client: ReputationCHClient, interval_hours: float, lifecycle: Lifecycle):
        self.client = client
        self.interval_sec = interval_hours * 3600
        self.lifecycle = lifecycle

    async def run(self) -> None:
        logger.info(
            "action=job_start interval_hours=%.1f",
            self.interval_sec / 3600,
        )

        while not self.lifecycle.is_shutting_down:
            try:
                logger.info("action=snapshot_start")
                count = await self.client.run_snapshot()
                logger.info("action=snapshot_done rows_written=%d", count)
            except Exception as e:
                logger.error("action=snapshot_failed error=%s", str(e))

            try:
                await asyncio.sleep(self.interval_sec)
            except asyncio.CancelledError:
                logger.info("action=job_cancelled")
                break
