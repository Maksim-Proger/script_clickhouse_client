import asyncio
import json
from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.lifecycle import Lifecycle
from project.nats_corn.parser.parser import parse_input


class AbProducer:
    def __init__(self, nc, config: dict, lifecycle: Lifecycle):
        self.nc = nc
        self.interval = config["ab_client"]["interval"]
        self.url = config["ab_client"]["url"]
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.lifecycle = lifecycle

        self.client = AbClient(self.url)

    async def start(self) -> None:
        await self.client.connect()
        try:
            while not self.lifecycle.is_shutting_down:
                raw_data = await self.client.get_data()

                records = parse_input(
                    raw_data,
                    source="ipban",
                    dt_format=self.dt_format
                )

                for record in records:
                    if self.lifecycle.is_shutting_down:
                        break

                    # Отправляем данные из AB в NATS.
                    await self.nc.publish(
                        "ch.write.raw",
                        json.dumps(record).encode()
                    )

                await asyncio.sleep(self.interval)
        finally:
            await self.client.close()
