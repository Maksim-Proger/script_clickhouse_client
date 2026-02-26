import asyncio
import logging
from typing import List, Tuple
from clickhouse_driver import Client as CHClient

logger = logging.getLogger("ch-writer")


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
        batch_size = len(batch)
        try:
            await asyncio.to_thread(
                self.client.execute,
                """
                INSERT INTO blocked_ips
                (blocked_at, ip_address, source, profile)
                VALUES
                """,
                batch
            )
            logger.info("action=db_write_success rows_inserted=%d", batch_size)
        except Exception as e:
            logger.error("action=db_write_failed batch_size=%d error=%s", batch_size, str(e))
            raise

    def close(self) -> None:
        self.client.disconnect()
