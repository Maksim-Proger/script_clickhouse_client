import asyncio
import json
import logging

from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.http.src2_client import DgClient
from project.module_data_collector.parser.parser import parse_input

logger = logging.getLogger("data-collector")

_PUBLISH_BATCH_SIZE = 500

async def _publish_records(nc, records: list, lifecycle: Lifecycle, subject: str = "ch.writer.row") -> None:
    batch = []
    for record in records:
        if lifecycle.is_shutting_down:
            break
        batch.append(nc.publish(subject, json.dumps(record).encode()))
        if len(batch) >= _PUBLISH_BATCH_SIZE:
            await asyncio.gather(*batch)
            batch.clear()
    if batch:
        await asyncio.gather(*batch)

class DgSourceManager:
    def __init__(self, nc, config: dict, lifecycle: Lifecycle):
        self.nc = nc
        self.lifecycle = lifecycle

        self.defaults = config.get("dg_defaults", {})
        self.sources = {src["name"]: src for src in config.get("dg_sources", [])}
        self.dt_format = config.get("parser", {}).get("clickhouse_dt_format", "%Y-%m-%dT%H:%M:%S")

        dg_timeout = self.defaults.get("timeout", 10)
        self.client = DgClient(timeout=dg_timeout, verify_ssl=self.defaults.get("verify_ssl", False))

    async def _execute(self, name: str, url: str, headers: dict, payload: dict, filter_expired: bool = True):
        last_error = None

        for attempt in range(1, 4):
            try:
                logger.info("action=execute_request profile=%s attempt=%d", name, attempt)
                raw_data = await self.client.fetch_data(url, headers, payload)
                break
            except Exception as e:
                last_error = e
                logger.warning(
                    "action=request_failed profile=%s attempt=%d error=%s",
                    name, attempt, str(e)
                )
                if attempt < 3:
                    await asyncio.sleep(1)

        else:
            logger.error(
                "action=request_all_attempts_failed profile=%s error=%s",
                name, str(last_error)
            )
            raise last_error

        loop = asyncio.get_running_loop()
        records = await loop.run_in_executor(
            None,
            lambda: parse_input(
                raw_data,
                source="dosgate",
                profile=name,
                dt_format=self.dt_format,
                filter_expired=filter_expired,
            )
        )

        if not records:
            logger.warning("action=parse_empty profile=%s message='No records found in response'", name)
            return

        logger.info("action=publish_start profile=%s records=%d", name, len(records))

        # Отправляем нормализованные данные в ClickHouse loader через NATS
        await _publish_records(self.nc, records, self.lifecycle)

        logger.info("action=publish_done profile=%s records=%d", name, len(records))

    async def run_automated(self, name: str):
        cfg = self.sources.get(name)
        if not cfg:
            return

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": name,
            "data": cfg.get("payload_data", {})
        }

        await self._execute(
            name=name,
            url=self.defaults.get("url"),
            headers=self.defaults.get("headers"),
            payload=final_payload,
        )

    async def run_manual(self, payload_from_front: dict):
        ui_params = payload_from_front.get("params", {})

        profile_name = ui_params.get("name", "manual-request")
        front_data_filters = ui_params.get("data", {})
        filter_expired = ui_params.get("filter_expired", False)

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": profile_name,
            "data": front_data_filters
        }

        logger.info(
            "action=manual_request_start profile=%s data_keys=%s filter_expired=%s",
            profile_name,
            list(front_data_filters.keys()),
            filter_expired
        )

        await self._execute(
            name=profile_name,
            url=self.defaults.get("url"),
            headers=self.defaults.get("headers"),
            payload=final_payload,
            filter_expired=filter_expired
        )

    async def _worker_loop(self, name: str, interval: int):
        while not self.lifecycle.is_shutting_down:
            try:
                await self.run_automated(name)
            except Exception as e:
                logger.error("action=worker_loop_error profile=%s error=%s", name, str(e))
            await asyncio.sleep(interval)

    async def start(self):
        await self.client.connect()
        for name, cfg in self.sources.items():
            interval = cfg.get("schedule", 0)
            if interval > 0:
                logger.info("action=worker_init profile=%s interval=%ds", name, interval)
                asyncio.create_task(self._worker_loop(name, interval))

    async def stop(self):
        await self.client.close()
        logger.info("action=dg_manager_stopped")
