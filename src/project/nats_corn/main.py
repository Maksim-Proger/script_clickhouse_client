import asyncio
import json
from nats.aio.client import Client as NatsClient
from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.nats_consumer import NatsDgConsumer
from project.nats_corn.parser.parser import parse_input


def main(config: dict) -> None:
    ab_interval = config["ab_client"]["interval"]
    ab_url = config["ab_client"]["url"]

    async def ab_loop(nc: NatsClient) -> None:
        ab_client = AbClient(ab_url)

        while True:
            try:
                raw_data_ab = ab_client.get_data()
                ips_ab = parse_input(
                    raw_data_ab,
                    source="ipban",
                    dt_format=config["parser"]["clickhouse_dt_format"]
                )

                for record in ips_ab:
                    await nc.publish(
                        "ch.write.raw",
                        json.dumps(record).encode()
                    )

            except Exception as e:
                # временно — stdout, позже заменю на logging
                print(f"[NATS-CORN][AB] error: {e}")

            await asyncio.sleep(ab_interval)

    async def _run() -> None:
        nc = NatsClient()
        await nc.connect(config.get("nats", {}).get("url", "nats://localhost:4222"))

        asyncio.create_task(ab_loop(nc))

        consumer = NatsDgConsumer(nc, config)
        await consumer.start()

    asyncio.run(_run())
