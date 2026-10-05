import asyncio
import logging
import random
from typing import Optional

from clickhouse_driver import Client as CHClient

from project.utils.reputation_sql import build_scoring_sql

logger = logging.getLogger("reputation.ch_client")

_SCORING_SQL = build_scoring_sql()

_INSERT_SQL = """
              INSERT INTO feedgen.ip_reputation_snapshots_data
              (run_id, computed_at, ip_address, score, risk_level,
               events_count, max_5m_events, max_hour_events,
               active_5m_windows, active_hours, active_days,
               sources_count, first_seen, last_seen)
              VALUES \
              """


class ReputationCHClient:
    def __init__(self, cfg: dict):
        self._cfg = cfg
        self._client: Optional[CHClient] = None

    def _get_client(self) -> CHClient:
        if self._client is None:
            self._client = CHClient(
                host=self._cfg["host"],
                port=self._cfg["port"],
                database=self._cfg["database"],
                user=self._cfg["user"],
                password=self._cfg["password"],
            )
        return self._client

    def _create_write_client(self) -> CHClient:
        write_hosts = self._cfg["write_hosts"]
        chosen_host = random.choice(write_hosts)
        logger.info("action=write_client_init target_write_host=%s", chosen_host)
        return CHClient(
            host=chosen_host,
            port=self._cfg["port"],
            database=self._cfg["database"],
            user=self._cfg["user"],
            password=self._cfg["password"],
        )

    def _run_snapshot_sync(self) -> int:
        read_client = self._get_client()

        logger.info("action=scoring_query_start")
        rows = read_client.execute(_SCORING_SQL)

        if not rows:
            logger.warning("action=scoring_empty message='Query returned 0 candidates'")
            return 0

        logger.info("action=scoring_query_done candidates=%d", len(rows))

        write_client = self._create_write_client()
        try:
            write_client.execute(_INSERT_SQL, rows)
        finally:
            write_client.disconnect()

        logger.info("action=snapshot_insert_done rows=%d", len(rows))
        return len(rows)

    async def run_snapshot(self) -> int:
        return await asyncio.to_thread(self._run_snapshot_sync)

    def close(self) -> None:
        if self._client:
            self._client.disconnect()
            self._client = None
            logger.info("action=ch_client_close status=ok")
