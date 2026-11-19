import asyncio
import logging
import random
from collections import Counter
from clickhouse_driver import Client as CHClient
from project.module_reputation.infrastructure.geo_matrix import GeoMatrixBuilder

from project.utils.geo import CountryResolver, prepare_geo
from project.utils.reputation_sql import build_snapshot_sql, window_where

logger = logging.getLogger("reputation.ch_client")

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

_RUN_DATE_SQL = (
    "SELECT toDate(computed_at) FROM feedgen.ip_reputation_snapshots_data "
    "WHERE run_id = %(run_id)s LIMIT 1"
)

_OLD_DATES_SQL = (
    "SELECT DISTINCT toDate(computed_at) FROM feedgen.ip_reputation_snapshots_data "
    "WHERE toDate(computed_at) < %(keep_from)s"
)

_DROP_PARTITION_SQL = "ALTER TABLE feedgen.ip_reputation_snapshots_data DROP PARTITION %(day)s"


def _set_name(run_id: str) -> str:
    if run_id.startswith("preset:"):
        return run_id.split(":")[1]
    return "general"


class ReputationCHClient:
    def __init__(self, cfg: dict, geo_cfg: dict, presets: list[dict], cleanup_enabled: bool):
        self._cfg = cfg
        self._geo_cfg = geo_cfg
        self._matrix = GeoMatrixBuilder(geo_cfg)
        self._snapshot_sql = build_snapshot_sql(presets)
        self._cleanup_enabled = cleanup_enabled

    def _create_client(self, host: str = None) -> CHClient:
        chosen_host = host or random.choice(self._cfg["write_hosts"])
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
            rows = client.execute(self._snapshot_sql, {"now_utc": now_utc, **geo})

            if not rows:
                logger.warning("action=scoring_empty message='Query returned 0 candidates'")
                return 0

            counts = Counter(_set_name(row[0]) for row in rows)
            for name, count in sorted(counts.items()):
                logger.info("action=scoring_set_done set=%s rows=%d", name, count)
            client.execute(_INSERT_SQL, rows)
            logger.info("action=snapshot_insert_done rows=%d", len(rows))

            if self._cleanup_enabled:
                self._clean_old_snapshots(client, rows)
        finally:
            client.disconnect()

        return len(rows)

    def _clean_old_snapshots(self, writer: CHClient, rows: list) -> None:
        try:
            general_run_id = next(row[0] for row in rows if not row[0].startswith("preset:"))
            keep_from = writer.execute(_RUN_DATE_SQL, {"run_id": general_run_id})[0][0]
        except Exception as e:
            logger.error("action=snapshot_cleanup_failed step=run_date error=%s", str(e))
            return

        for host in self._cfg["write_hosts"]:
            try:
                self._drop_old_partitions(host, keep_from)
            except Exception as e:
                logger.error("action=snapshot_cleanup_failed host=%s error=%s", host, str(e))

    def _drop_old_partitions(self, host: str, keep_from) -> None:
        client = self._create_client(host)
        try:
            days = [day for (day,) in client.execute(_OLD_DATES_SQL, {"keep_from": keep_from})]
            for day in days:
                client.execute(_DROP_PARTITION_SQL, {"day": day})
                logger.info("action=snapshot_partition_dropped host=%s day=%s", host, day)
        finally:
            client.disconnect()

    async def run_snapshot(self) -> int:
        return await asyncio.to_thread(self._run_snapshot_sync)
