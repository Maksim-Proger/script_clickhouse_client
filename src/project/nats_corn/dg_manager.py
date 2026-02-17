import asyncio
import json
import copy
from project.nats_corn.http.src2_client import DgClient
from project.nats_corn.parser.parser import parse_input


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
            raw_data = await self.client.fetch_data(url, headers, payload)

            records = parse_input(
                raw_data,
                source="dosgate",
                profile=name,
                dt_format=self.dt_format
            )

            for record in records:
                if self.lifecycle.is_shutting_down:
                    break
                await self.nc.publish("ch.write.raw", json.dumps(record).encode())
        except Exception as e:
            print(f"Error executing request for '{name}': {e}")

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
            print(f"Manual request failed: profile '{name}' not found in config")
            return

        # 1. Берем за основу payload из конфига (там есть action, name и дефолты)
        final_payload = copy.deepcopy(cfg["payload"])

        # 2. Получаем данные из запроса фронтенда
        front_data = payload_from_front.get("data", {})

        # 3. Слияние: если во фронте поле не пустое — заменяем дефолт
        if "data" not in final_payload:
            final_payload["data"] = {}

        for key, value in front_data.items():
            # Проверяем, что значение — строка и она не пустая после trim
            if isinstance(value, str) and value.strip() != "":
                final_payload["data"][key] = value.strip()
            # Если это не строка (например, число), но оно передано — тоже берем
            elif value is not None and not isinstance(value, str):
                final_payload["data"][key] = value

        # Выполняем запрос с итоговым «склеенным» объектом
        await self._execute(
            name=name,
            url=cfg["url"],
            headers=cfg["headers"],
            payload=final_payload
        )

    async def _worker_loop(self, name: str, interval: int):
        """Цикл для автоматических задач"""
        while not self.lifecycle.is_shutting_down:
            await self.run_automated(name)
            await asyncio.sleep(interval)

    async def start(self):
        """Запуск фоновых воркеров при старте приложения"""
        await self.client.connect()
        for name, cfg in self.sources.items():
            interval = cfg.get("schedule", 0)
            if interval > 0:
                print(f"Starting background worker for '{name}' (every {interval}s)")
                asyncio.create_task(self._worker_loop(name, interval))