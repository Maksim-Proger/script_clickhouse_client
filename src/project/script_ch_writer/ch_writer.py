import asyncio
from clickhouse_driver import Client as CHClient
from typing import List, Tuple


class ClickHouseWriter:
    def __init__(self, cfg: dict):
        self.client = CHClient(
            host=cfg["host"],
            port=cfg["port"],
            database=cfg["database"],
            user=cfg["user"],
            password=cfg["password"],
        )

    async def write(self, batch: List[Tuple]) -> None:
        await asyncio.to_thread(
            self.client.execute,
            """
            INSERT INTO blocked_ips
            (blocked_at, ip_address, source, profile)
            VALUES
            """,
            batch
        )

    def close(self) -> None:
        self.client.disconnect()
