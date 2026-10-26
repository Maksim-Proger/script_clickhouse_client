import asyncio
import logging

from project.utils.http.async_client import BaseAsyncHttpClient
from project.utils.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import parse_targeted_pairs
from project.module_data_collector.publisher import publish_records

logger = logging.getLogger("data-collector.targeted_ab_producer")


class TargetedAbProducer:
    def __init__(self, nc, config: dict, lifecycle: Lifecycle):
        cfg = config["targeted_ab_client"]

        self.nc = nc
        self.url = cfg["url"]
        self.interval = cfg.get("interval", 60.0)
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.raw_subject = config["nats"]["raw_subject"]
        self.lifecycle = lifecycle

        self.client = BaseAsyncHttpClient(timeout=cfg.get("timeout", 10))

    async def start(self) -> None:
        await self.client.connect()

        logger.info("action=worker_init profile=targeted_ipban interval=%ds", self.interval)

        try:
            while not self.lifecycle.is_shutting_down:
                try:
                    raw_data = await self.client.get(self.url)
                    records, stats = await asyncio.to_thread(parse_targeted_pairs, raw_data, "ipban", self.dt_format)

                    if stats["skipped"]:
                        logger.warning(
                            "action=targeted_ipban_lines_skipped skipped=%d lines=%d samples=%s",
                            stats["skipped"], stats["lines"], stats["samples"],
                        )

                    if records:
                        await publish_records(self.nc, self.raw_subject, records, self.lifecycle)

                    logger.info(
                        "action=targeted_ipban_poll_done lines=%d records=%d skipped=%d empty=%d",
                        stats["lines"], stats["records"], stats["skipped"], stats["empty"],
                    )

                except Exception as req_err:
                    logger.error("action=targeted_ipban_fetch_failed error=%s", str(req_err))

                await asyncio.sleep(self.interval)
        finally:
            await self.client.close()
            logger.info("action=worker_stopped profile=targeted_ipban")
