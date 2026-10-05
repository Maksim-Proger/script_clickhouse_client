import asyncio
import logging
import random
from datetime import datetime
from typing import Optional

from clickhouse_driver import Client as CHClient

from project.utils.reputation_sql import build_scoring_sql

logger = logging.getLogger("ch-api-gateway.reputation_calcs")

_INSERT_SQL = """
              INSERT INTO feedgen.ip_reputation_calcs_data
              (calc_id, computed_at, ip_address, score, risk_level,
               events_count, max_5m_events, max_hour_events,
               active_5m_windows, active_hours, active_days,
               sources_count, first_seen, last_seen)
              VALUES \
              """

_COUNT_SQL = "SELECT count() FROM feedgen.ip_reputation_calcs WHERE calc_id = %(calc_id)s"

_DROP_PARTITION_SQL = "ALTER TABLE feedgen.ip_reputation_calcs_data DROP PARTITION %(calc_id)s"


class ReputationCalcClient:
    def __init__(self, cfg: dict):
        self._cfg = cfg

    def _client(self, host: str) -> CHClient:
        return CHClient(
            host=host,
            port=self._cfg["port"],
            database=self._cfg.get("database", "feedgen"),
            user=self._cfg["user"],
            password=self._cfg["password"],
            send_receive_timeout=self._cfg.get("stream_timeout_sec", 300),
        )

    def _execute_sync(self, host: str, sql: str, params=None):
        client = self._client(host)
        try:
            return client.execute(sql, params)
        finally:
            client.disconnect()

    def _calc_sync(self, calc_id: int, period_to: datetime, source: str, profile: Optional[str]) -> int:
        sql = build_scoring_sql("toDateTime(%(period_to)s)", by_source=True, by_profile=profile is not None)
        params = {"period_to": period_to, "source": source, "profile": profile}
        rows = self._execute_sync(self._cfg["host"], sql, params)
        if not rows:
            return 0

        rows = [(calc_id, *row[1:]) for row in rows]
        host = random.choice(self._cfg["write_hosts"])
        self._execute_sync(host, _INSERT_SQL, rows)
        logger.info("action=reputation_calc_insert_done id=%d rows=%d host=%s", calc_id, len(rows), host)
        return len(rows)

    async def now(self) -> datetime:
        result = await asyncio.to_thread(self._execute_sync, self._cfg["host"], "SELECT now()")
        return result[0][0]

    async def calc(self, calc_id: int, period_to: datetime, source: str, profile: Optional[str]) -> int:
        return await asyncio.to_thread(self._calc_sync, calc_id, period_to, source, profile)

    async def count(self, calc_id: int) -> int:
        result = await asyncio.to_thread(
            self._execute_sync, self._cfg["host"], _COUNT_SQL, {"calc_id": calc_id},
        )
        return int(result[0][0]) if result else 0

    async def clear(self, calc_id: int) -> None:
        for host in self._cfg["write_hosts"]:
            await asyncio.to_thread(self._execute_sync, host, _DROP_PARTITION_SQL, {"calc_id": calc_id})
