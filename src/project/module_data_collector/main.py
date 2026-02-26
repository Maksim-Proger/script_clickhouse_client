import asyncio
from typing import Optional
from nats.aio.client import Client as NatsClient

from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.ab_producer import AbProducer
from project.module_data_collector.dg_manager import DgSourceManager
from project.module_data_collector.consumers.dg_consumer import NatsDgConsumer
from project.module_data_collector.consumers.web_consumer import NatsWebConsumer
from project.utils.logging_formatter import setup_logging


def main(config: dict) -> None:
    logger = setup_logging("nats-corn")
    logger.info("action=process_start status=initializing")

    async def run() -> None:
        lifecycle = Lifecycle()
        lifecycle.install_signal_handlers()

        nc: Optional[NatsClient] = NatsClient()
        await nc.connect(config["nats"]["url"])

        ab = AbProducer(nc, config, lifecycle)

        dg_manager = DgSourceManager(nc, config, lifecycle)
        dg_consumer = NatsDgConsumer(nc, config, lifecycle, dg_manager)

        web = NatsWebConsumer(nc, config, lifecycle)

        tasks = [
            asyncio.create_task(ab.start()),
            asyncio.create_task(dg_manager.start()),
            asyncio.create_task(dg_consumer.start()),
            asyncio.create_task(web.start()),
        ]

        await lifecycle.shutdown_event.wait()

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        await dg_manager.stop()
        await nc.close()

    asyncio.run(run())
