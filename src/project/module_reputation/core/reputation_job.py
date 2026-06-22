import asyncio
import logging

from project.module_reputation.infrastructure.ch_client import ReputationCHClient
from project.module_reputation.lifecycle import Lifecycle

logger = logging.getLogger("reputation.job")


class ReputationJob:
    def __init__(
        self,
        client: ReputationCHClient,
        interval_hours: float,
        lifecycle: Lifecycle,
        retry_attempts: int,
        retry_delay_seconds: float,
    ):
        self.client = client
        self.interval_sec = interval_hours * 3600
        self.lifecycle = lifecycle
        self.retry_attempts = retry_attempts
        self.retry_delay_seconds = retry_delay_seconds

    async def _run_snapshot_with_retry(self) -> None:
        for attempt in range(1, self.retry_attempts + 1):
            try:
                logger.info("action=snapshot_start attempt=%d", attempt)
                count = await self.client.run_snapshot()
                logger.info("action=snapshot_done rows_written=%d attempt=%d", count, attempt)
                return
            except Exception as e:
                logger.error(
                    "action=snapshot_failed attempt=%d max_attempts=%d error=%s",
                    attempt, self.retry_attempts, str(e),
                )
                if attempt < self.retry_attempts:
                    await asyncio.sleep(self.retry_delay_seconds)

        logger.error("action=snapshot_exhausted message='All retry attempts failed, waiting for next cycle'")

    async def run(self) -> None:
        logger.info(
            "action=job_start interval_hours=%.1f retry_attempts=%d retry_delay_seconds=%.0f",
            self.interval_sec / 3600, self.retry_attempts, self.retry_delay_seconds,
        )

        while not self.lifecycle.is_shutting_down:
            await self._run_snapshot_with_retry()

            try:
                await asyncio.sleep(self.interval_sec)
            except asyncio.CancelledError:
                logger.info("action=job_cancelled")
                break

