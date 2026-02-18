import asyncio
from typing import Optional
from nats.aio.client import Client as NatsClient

from project.nats_corn.lifecycle import Lifecycle
from project.nats_corn.ab_producer import AbProducer
from project.nats_corn.dg_manager import DgSourceManager
from project.nats_corn.dg_consumer import NatsDgConsumer
from project.nats_corn.web_consumer import NatsWebConsumer
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

        await nc.close()

    asyncio.run(run())
