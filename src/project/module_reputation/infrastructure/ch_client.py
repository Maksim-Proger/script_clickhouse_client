import asyncio
import logging
import random
from clickhouse_driver import Client as CHClient
from project.module_reputation.infrastructure.geo_matrix import GeoMatrixBuilder

from project.utils.geo import CountryResolver, prepare_geo
from project.utils.reputation_sql import build_scoring_sql, window_where

logger = logging.getLogger("reputation.ch_client")

_SCORING_SQL = build_scoring_sql()

_INSERT_SQL = """
              INSERT INTO feedgen.ip_reputation_snapshots_data
              (run_id, computed_at, ip_address, score, risk_level,
               events_count, max_5m_events, max_hour_events,
               active_5m_windows, active_hours, active_days,
               sources_count, first_seen, last_seen,
               base_score, geo_coefficient, geo_bonus, geo_country, geo_category,
               geo_matrix_version, geo_version)
              VALUES \
              """


class ReputationCHClient:
    def __init__(self, cfg: dict, geo_cfg: dict):
        self._cfg = cfg
        self._geo_cfg = geo_cfg
        self._matrix = GeoMatrixBuilder(geo_cfg)

    def _create_client(self) -> CHClient:
        chosen_host = random.choice(self._cfg["write_hosts"])
        logger.info("action=client_init target_host=%s", chosen_host)
        return CHClient(
            host=chosen_host,
            port=self._cfg["port"],
            database=self._cfg["database"],
            user=self._cfg["user"],
            password=self._cfg["password"],
            send_receive_timeout=self._cfg.get("send_receive_timeout", 900),
        )

    def _run_snapshot_sync(self) -> int:
        client = self._create_client()
        try:
            now_utc = client.execute("SELECT now()")[0][0]

            with CountryResolver(self._geo_cfg["country_db_path"]) as resolver:
                self._matrix.ensure(client, resolver, now_utc)
                geo = prepare_geo(client, resolver, now_utc, window_where(), {}, self._matrix.rules_version)

            logger.info("action=scoring_query_start matrix_version=%s", geo["matrix_version"] or "none")
            rows = client.execute(_SCORING_SQL, {"now_utc": now_utc, **geo})

            if not rows:
                logger.warning("action=scoring_empty message='Query returned 0 candidates'")
                return 0

            logger.info("action=scoring_query_done candidates=%d", len(rows))
            client.execute(_INSERT_SQL, rows)
        finally:
            client.disconnect()

        logger.info("action=snapshot_insert_done rows=%d", len(rows))
        return len(rows)

    async def run_snapshot(self) -> int:
        return await asyncio.to_thread(self._run_snapshot_sync)
