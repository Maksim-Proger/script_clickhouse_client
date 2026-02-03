import asyncio
from typing import Optional
from nats.aio.client import Client as NatsClient

from project.nats_corn.lifecycle import Lifecycle
from project.nats_corn.ab_producer import AbProducer
from project.nats_corn.dg_consumer import NatsDgConsumer
from project.nats_corn.web_consumer import NatsWebConsumer


def main(config: dict) -> None:
    async def run() -> None:
        lifecycle = Lifecycle()
        lifecycle.install_signal_handlers()

        nc: Optional[NatsClient] = NatsClient()
        await nc.connect(config["nats"]["url"])

        ab = AbProducer(nc, config, lifecycle)
        dg = NatsDgConsumer(nc, config, lifecycle)
        web = NatsWebConsumer(nc, config, lifecycle)

        tasks = [
            asyncio.create_task(ab.start()),
            asyncio.create_task(dg.start()),
            asyncio.create_task(web.start()),
        ]

        await lifecycle.shutdown_event.wait()

        for t in tasks:
            t.cancel()

        await asyncio.gather(*tasks, return_exceptions=True)

        await nc.close()

    asyncio.run(run())
