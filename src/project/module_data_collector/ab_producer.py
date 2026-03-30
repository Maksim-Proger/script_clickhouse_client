import asyncio
import logging

from project.module_data_collector.dg_manager import _publish_records
from project.module_data_collector.http.src1_client import AbClient
from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import parse_input

logger = logging.getLogger("data-collector")


class AbProducer:
    def __init__(self, nc, config: dict, lifecycle: Lifecycle):
        self.nc = nc
        self.interval = config["ab_client"]["interval"]
        self.url = config["ab_client"]["url"]
        self.timeout = config["ab_client"].get("timeout", 10)
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.lifecycle = lifecycle

        self.client = AbClient(self.url, timeout=self.timeout)

    async def start(self) -> None:
        await self.client.connect()

        logger.info("action=worker_init profile=ipban interval=%ds", self.interval)

        try:
            while not self.lifecycle.is_shutting_down:
                try:
                    raw_data = await self.client.get_data()

                    loop = asyncio.get_running_loop()
                    records = await loop.run_in_executor(
                        None,
                        lambda: parse_input(
                            raw_data,
                            source="ipban",
                            dt_format=self.dt_format,
                        )
                    )

                    if records:
                        await _publish_records(self.nc, records, self.lifecycle)

                except Exception as req_err:
                    logger.error("action=ipban_fetch_failed error=%s", str(req_err))

                await asyncio.sleep(self.interval)
        finally:
            await self.client.close()
            logger.info("action=worker_stopped profile=ipban")


