import asyncio
from typing import Optional

from nats.aio.client import Client as NatsClient

from project.module_data_collector.ab_producer import AbProducer
from project.module_data_collector.consumers.pa_consumer import NatsPaConsumer
from project.module_data_collector.consumers.dg_consumer import NatsDgConsumer
from project.module_data_collector.consumers.web_consumer import NatsWebConsumer
from project.module_data_collector.dg_manager import DgSourceManager
from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.targeted_ab_producer import TargetedAbProducer
from project.utils.logging_formatter import setup_logging


def main(config: dict) -> None:

    logger = setup_logging("data-collector")
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
        pa_consumer = NatsPaConsumer(nc, config, lifecycle, dg_manager)

        tasks = [
            asyncio.create_task(ab.start()),
            asyncio.create_task(dg_manager.start()),
            asyncio.create_task(dg_consumer.start()),
            asyncio.create_task(web.start()),
            asyncio.create_task(pa_consumer.start()),
        ]

        try:
            if config.get("targeted_ab_client", {}).get("url"):
                targeted = TargetedAbProducer(nc, config, lifecycle)
                tasks.append(asyncio.create_task(targeted.start()))
            else:
                logger.info("action=targeted_ipban_skipped reason=not_configured")
        except Exception as e:
            logger.error("action=targeted_ipban_init_failed error=%s", str(e))


        await lifecycle.shutdown_event.wait()

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        await dg_manager.stop()
        await nc.close()

    asyncio.run(run())
