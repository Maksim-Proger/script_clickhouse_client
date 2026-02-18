import asyncio
import json
import logging
import copy
from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input


logger = logging.getLogger("nats-corn")

class DgSourceManager:
    def __init__(self, nc, config: dict, lifecycle):
        self.nc = nc
        self.lifecycle = lifecycle
        self.client = DgClient()
        # Индексируем источники по имени для быстрого доступа
        self.sources = {src["name"]: src for src in config.get("dg_sources", [])}
        self.dt_format = config["parser"]["clickhouse_dt_format"]

    async def _execute(self, name: str, url: str, headers: dict, payload: dict):
        """Единый метод выполнения запроса и отправки результатов в NATS"""
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
                await self.nc.publish("ch.write.raw", json.dumps(record).encode())

        except Exception as e:
            logger.error("action=request_failed profile=%s error=%s", name, str(e))

    async def run_automated(self, name: str):
        """Логика для Ozon: берем всё из конфига без изменений"""
        cfg = self.sources.get(name)
        if cfg:
            await self._execute(name, cfg["url"], cfg["headers"], cfg["payload"])

    async def run_manual(self, payload_from_front: dict):
        """
        Логика для feed-gen:
        Берем шаблон из конфига и обновляем его данными с фронта,
        если они не пустые.
        """
        name = payload_from_front.get("name", "feed-gen")
        cfg = self.sources.get(name)

        if not cfg:
            logger.error("action=profile_not_found profile=%s", name)
            return

        final_payload = copy.deepcopy(cfg["payload"])
        front_data = payload_from_front.get("data", {})

        changed_keys = []
        for key, value in front_data.items():
            if isinstance(value, str) and value.strip() != "":
                final_payload["data"][key] = value.strip()
                changed_keys.append(key)

        logger.info(
            "action=manual_merge profile=%s changed_fields=%s",
            name,
            changed_keys if changed_keys else "none_all_defaults"
        )

        await self._execute(
            name=name,
            url=cfg["url"],
            headers=cfg["headers"],
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
