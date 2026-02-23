import asyncio
import json
import logging

from project.module_data_collector.http.src2_client import DgClient
from project.module_data_collector.parser.parser import parse_input

logger = logging.getLogger("data_collector")


class DgSourceManager:
    def __init__(self, nc, config: dict, lifecycle):
        self.nc = nc
        self.lifecycle = lifecycle
        self.client = DgClient()

        self.defaults = config.get("dg_defaults", {})

        self.sources = {src["name"]: src for src in config.get("dg_sources", [])}
        self.dt_format = config.get("parser", {}).get("clickhouse_dt_format", "%Y-%m-%d %H:%M:%S")

    async def _execute(self, name: str, url: str, headers: dict, payload: dict):
        try:
            logger.info("action=execute_request profile=%s", name)

            raw_data = await self.client.fetch_data(url, headers, payload)

            records = parse_input(
                raw_data,
                source="dosgate",
                profile=name,
                dt_format=self.dt_format
            )

            if not records:
                logger.warning("action=parse_empty profile=%s message='No records found in response'", name)

            for record in records:
                if self.lifecycle.is_shutting_down:
                    break
                # Отправляем нормализованные данные в ClickHouse loader через NATS
                await self.nc.publish("ch.write.raw", json.dumps(record).encode())

        except Exception as e:
            logger.error("action=request_failed profile=%s error=%s", name, str(e))

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
            payload=final_payload
        )

    async def run_manual(self, payload_from_front: dict):
        ui_params = payload_from_front.get("params", {})

        profile_name = ui_params.get("name", "manual-request")
        front_data_filters = ui_params.get("data", {})

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": profile_name,
            "data": front_data_filters
        }

        logger.info(
            "action=manual_request_start profile=%s data_keys=%s",
            profile_name,
            list(front_data_filters.keys())
        )

        await self._execute(
            name=profile_name,
            url=self.defaults.get("url"),
            headers=self.defaults.get("headers"),
            payload=final_payload
        )

    async def _worker_loop(self, name: str, interval: int):
        while not self.lifecycle.is_shutting_down:
            await self.run_automated(name)
            await asyncio.sleep(interval)

    async def start(self):
        await self.client.connect()
        for name, cfg in self.sources.items():
            interval = cfg.get("schedule", 0)
            if interval > 0:
                logger.info("action=worker_init profile=%s interval=%ds", name, interval)
                asyncio.create_task(self._worker_loop(name, interval))