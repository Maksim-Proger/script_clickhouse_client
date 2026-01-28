import asyncio
import json
import signal
from typing import Optional

from nats.aio.client import Client as NatsClient

from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.nats_consumer import NatsDgConsumer
from project.nats_corn.parser.parser import parse_input


def main(config: dict) -> None:
    ab_interval = config["ab_client"]["interval"]
    ab_url = config["ab_client"]["url"]

    async def run() -> None:
        shutdown_event = asyncio.Event()
        is_shutting_down = False

        nc: Optional[NatsClient] = None

        async def shutdown():
            nonlocal is_shutting_down
            if is_shutting_down:
                return

            is_shutting_down = True

            if nc:
                try:
                    await nc.close()
                except Exception:
                    pass

            shutdown_event.set()

        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(  # type: ignore[arg-type]
                sig,
                lambda: asyncio.create_task(shutdown())
            )

        nc = NatsClient()
        await nc.connect(config.get("nats", {}).get("url", "nats://localhost:4222"))

        ab_client = AbClient(ab_url)
        await ab_client.connect()

        async def ab_loop():
            while not is_shutting_down:
                try:
                    raw_data_ab = await ab_client.get_data()

                    ips_ab = parse_input(
                        raw_data_ab,
                        source="ipban",
                        dt_format=config["parser"]["clickhouse_dt_format"]
                    )

                    for record in ips_ab:
                        if is_shutting_down:
                            break

                        await nc.publish(
                            "ch.write.raw",
                            json.dumps(record).encode()
                        )

                except Exception as e:
                    print(f"[NATS-CORN][AB] error: {e}")

                await asyncio.sleep(ab_interval)

        ab_task = asyncio.create_task(ab_loop())

        consumer = NatsDgConsumer(nc, config, shutdown_event)
        consumer_task = asyncio.create_task(consumer.start())

        await shutdown_event.wait()

        ab_task.cancel()
        consumer_task.cancel()

        await asyncio.gather(ab_task, consumer_task, return_exceptions=True)

        await ab_client.close()

    asyncio.run(run())
