import asyncio

from project.module_reputation.consumers.recalc_consumer import NatsRecalcConsumer
from project.module_reputation.core.reputation_job import ReputationJob
from project.module_reputation.infrastructure.ch_client import ReputationCHClient
from project.utils.lifecycle import Lifecycle
from project.utils.logging_formatter import setup_logging


def main(config: dict) -> None:
    logger = setup_logging("reputation")
    logger.info("action=process_start status=initializing")

    async def run() -> None:
        lifecycle = Lifecycle("reputation")
        lifecycle.install_signal_handlers()

        client = ReputationCHClient(config["clickhouse"], config["geo"])
        job = ReputationJob(
            client=client,
            interval_hours=config["job"]["interval_hours"],
            lifecycle=lifecycle,
            retry_attempts=config["job"]["retry_attempts"],
            retry_delay_seconds=config["job"]["retry_delay_seconds"],
        )

        consumer = NatsRecalcConsumer(config, job)

        tasks = [
            asyncio.create_task(job.run()),
            asyncio.create_task(consumer.start()),
        ]

        await lifecycle.shutdown_event.wait()

        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await consumer.stop()

        logger.info("action=process_stop status=clean")

    asyncio.run(run())
