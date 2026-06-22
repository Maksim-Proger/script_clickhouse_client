import asyncio

from project.module_reputation.core.reputation_job import ReputationJob
from project.module_reputation.infrastructure.ch_client import ReputationCHClient
from project.module_reputation.lifecycle import Lifecycle
from project.utils.logging_formatter import setup_logging


def main(config: dict) -> None:
    logger = setup_logging("reputation")
    logger.info("action=process_start status=initializing")

    async def run() -> None:
        lifecycle = Lifecycle()
        lifecycle.install_signal_handlers()

        client = ReputationCHClient(config["clickhouse"])
        job = ReputationJob(
            client=client,
            interval_hours=config["job"]["interval_hours"],
            lifecycle=lifecycle,
            retry_attempts=config["job"]["retry_attempts"],
            retry_delay_seconds=config["job"]["retry_delay_seconds"],
        )

        task = asyncio.create_task(job.run())

        await lifecycle.shutdown_event.wait()

        task.cancel()
        await asyncio.gather(task, return_exceptions=True)

        client.close()
        logger.info("action=process_stop status=clean")

    asyncio.run(run())
