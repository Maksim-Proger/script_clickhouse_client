import hashlib
import json
import logging
import uuid
from clickhouse_driver import Client as CHClient
from datetime import datetime, timedelta, timezone

from project.utils.geo import CountryResolver, naive_utc, resolve_missing_ips, select_matrix
from project.utils.reputation_sql import (
    BLOCKED_IPS_TABLE,
    GEO_CACHE_TABLE,
    GEO_MATRIX_RUNS_TABLE,
    GEO_MATRIX_TABLE,
    window_where,
)

logger = logging.getLogger("reputation.geo_matrix")

MATRIX_DAYS = 28

_RULE_KEYS = (
    "max_bonus", "min_ips", "min_prefixes", "high_share",
    "high_ip_share", "elevated_share", "noticeable_share", "neutral",
)

_MATRIX_COLUMNS = (
    "matrix_version, geo_version, computed_at, period_start, period_end, "
    "country_iso, violations, unique_ips, prefixes24, active_days, "
    "ipban_violations, dosgate_violations, ipban_unique_ips, dosgate_unique_ips, "
    "missing_geo_ips, decode_error_ips, unmapped_ips, "
    "violation_share, ip_share, category, geo_coefficient, geo_bonus"
)

_UNMAPPED_INDEX = 16

_MATRIX_SQL = f"""
WITH
    toDateTime(%(window_end)s, 'UTC') AS end_utc,
    end_utc - INTERVAL {MATRIX_DAYS} DAY AS start_utc,
    %(neutral)s AS neutral,
    per_ip AS (
        SELECT ip_address, count() AS violations,
               countIf(source = 'ipban') AS ipban_violations,
               countIf(source = 'dosgate') AS dosgate_violations,
               uniqExactState(toDate(blocked_at, 'UTC')) AS day_state
        FROM {BLOCKED_IPS_TABLE}
        WHERE blocked_at >= start_utc AND blocked_at < end_utc
        GROUP BY ip_address
    ),
    geo AS (
        SELECT ip_address, country_iso, lookup_status
        FROM {GEO_CACHE_TABLE} FINAL
        WHERE geo_version = %(geo_version)s
    ),
    by_country AS (
        SELECT coalesce(nullIf(g.country_iso, ''), 'ZZ') AS country_iso,
               sum(p.violations) AS violations, count() AS unique_ips,
               uniqExact(bitShiftRight(toUInt32(p.ip_address), 8)) AS prefixes24,
               uniqExactMerge(p.day_state) AS active_days,
               sum(p.ipban_violations) AS ipban_violations,
               sum(p.dosgate_violations) AS dosgate_violations,
               countIf(p.ipban_violations > 0) AS ipban_unique_ips,
               countIf(p.dosgate_violations > 0) AS dosgate_unique_ips,
               countIf(coalesce(g.lookup_status, '') = 'missing') AS missing_geo_ips,
               countIf(coalesce(g.lookup_status, '') = 'decode_error') AS decode_error_ips,
               countIf(coalesce(g.lookup_status, '') NOT IN
                       ('known', 'missing', 'decode_error')) AS unmapped_ips
        FROM per_ip AS p
        LEFT ANY JOIN geo AS g ON p.ip_address = g.ip_address
        GROUP BY country_iso
    ),
    with_totals AS (
        SELECT *,
               country_iso != 'ZZ' AND NOT has(neutral, country_iso) AS eligible,
               sumIf(violations, country_iso != 'ZZ' AND NOT has(neutral, country_iso))
                   OVER () AS eligible_violations,
               sumIf(unique_ips, country_iso != 'ZZ' AND NOT has(neutral, country_iso))
                   OVER () AS eligible_unique_ips
        FROM by_country
    ),
    shares AS (
        SELECT *,
               if(eligible, violations / greatest(eligible_violations, 1), 0.0)
                   AS violation_share,
               if(eligible, unique_ips / greatest(eligible_unique_ips, 1), 0.0)
                   AS ip_share
        FROM with_totals
    ),
    classified AS (
        SELECT *, multiIf(
            country_iso = 'ZZ', 'unknown',
            has(neutral, country_iso), 'neutral',
            unique_ips < %(min_ips)s OR prefixes24 < %(min_prefixes)s, 'insufficient',
            violation_share >= %(high_share)s AND ip_share >= %(high_ip_share)s, 'high',
            violation_share >= %(elevated_share)s, 'elevated',
            violation_share >= %(noticeable_share)s, 'noticeable',
            'rare'
        ) AS category
        FROM shares
    ),
    weighted AS (
        SELECT *, multiIf(category = 'high', 1.0, category = 'elevated', 0.6,
                          category = 'noticeable', 0.2, 0.0) AS geo_coefficient
        FROM classified
    )
SELECT %(matrix_version)s AS matrix_version, %(geo_version)s AS geo_version,
       toDateTime(%(computed_at)s, 'UTC') AS computed_at,
       start_utc AS period_start, end_utc AS period_end,
       country_iso, violations, unique_ips, prefixes24, active_days,
       ipban_violations, dosgate_violations, ipban_unique_ips, dosgate_unique_ips,
       missing_geo_ips, decode_error_ips, unmapped_ips,
       violation_share, ip_share, category, geo_coefficient,
       round(%(max_bonus)s * geo_coefficient, 1) AS geo_bonus
FROM weighted
ORDER BY violations DESC, country_iso
"""

_INSERT_MATRIX_SQL = f"INSERT INTO {GEO_MATRIX_TABLE} ({_MATRIX_COLUMNS}) VALUES"

_INSERT_RUN_SQL = (
    f"INSERT INTO {GEO_MATRIX_RUNS_TABLE} "
    "(matrix_version, geo_version, computed_at, period_start, period_end, "
    "rules_version, rules_json, max_bonus, expected_countries, status, updated_at) VALUES"
)

_VISIBLE_SQL = f"SELECT count() FROM {GEO_MATRIX_TABLE} FINAL WHERE matrix_version = %(matrix_version)s"

_CACHE_VERSIONS_SQL = f"SELECT DISTINCT geo_version FROM {GEO_CACHE_TABLE}"

_USED_VERSIONS_SQL = (
    f"SELECT DISTINCT geo_version FROM {GEO_MATRIX_RUNS_TABLE} "
    "WHERE computed_at >= toDateTime(%(now_utc)s, 'UTC') - INTERVAL 8 DAY"
)

_DROP_CACHE_SQL = f"ALTER TABLE {GEO_CACHE_TABLE} DROP PARTITION %(geo_version)s"


class GeoMatrixBuilder:
    def __init__(self, geo_cfg: dict):
        rules = {key: geo_cfg[key] for key in _RULE_KEYS}
        rules["neutral"] = sorted(str(c).upper() for c in rules["neutral"])
        self.rules = rules
        self.rules_json = json.dumps(rules, sort_keys=True)
        self.rules_version = hashlib.sha256(self.rules_json.encode()).hexdigest()[:16]

    def ensure(self, client: CHClient, resolver: CountryResolver, now_utc: datetime) -> None:
        window_end = naive_utc(now_utc).replace(hour=0, minute=0, second=0, microsecond=0)
        current = select_matrix(client, now_utc, self.rules_version)
        if current and current["geo_version"] == resolver.version and current["period_end"] == window_end:
            return

        try:
            self._build(client, resolver, now_utc, window_end)
        except Exception as e:
            logger.error("action=geo_matrix_failed error=%s", str(e))
            return

        self._cleanup_cache(client, resolver, now_utc)

    def _build(self, client: CHClient, resolver: CountryResolver, now_utc: datetime, window_end: datetime) -> None:
        matrix_version = str(uuid.uuid4())
        run = (
            matrix_version, resolver.version, now_utc,
            window_end - timedelta(days=MATRIX_DAYS), window_end,
            self.rules_version, self.rules_json, float(self.rules["max_bonus"]),
        )
        logger.info("action=geo_matrix_start matrix_version=%s geo_version=%s period_end=%s",
                    matrix_version, resolver.version, window_end)

        added = resolve_missing_ips(client, resolver, window_where(days=MATRIX_DAYS), {"now_utc": window_end})
        logger.info("action=geo_cache_filled added=%d", added)

        self._write_run(client, run, 0, "building")
        try:
            rows = client.execute(_MATRIX_SQL, {
                **self.rules,
                "window_end": window_end,
                "computed_at": now_utc,
                "matrix_version": matrix_version,
                "geo_version": resolver.version,
            })
            if not rows:
                raise RuntimeError("No blocked IPs in the matrix window")
            unmapped = sum(row[_UNMAPPED_INDEX] for row in rows)
            if unmapped:
                raise RuntimeError(f"{unmapped} IPs have no geo cache entry")

            client.execute(_INSERT_MATRIX_SQL, rows)
            visible = client.execute(_VISIBLE_SQL, {"matrix_version": matrix_version})[0][0]
            if visible != len(rows):
                raise RuntimeError(f"Matrix rows written {len(rows)}, visible {visible}")
        except Exception:
            try:
                self._write_run(client, run, 0, "failed")
            except Exception as e:
                logger.error("action=geo_matrix_fail_save_failed error=%s", str(e))
            raise

        self._write_run(client, run, len(rows), "ready")
        logger.info("action=geo_matrix_ready matrix_version=%s countries=%d", matrix_version, len(rows))

    @staticmethod
    def _write_run(client: CHClient, run: tuple, expected_countries: int, status: str) -> None:
        client.execute(_INSERT_RUN_SQL, [(*run, expected_countries, status, datetime.now(timezone.utc))])

    @staticmethod
    def _cleanup_cache(client: CHClient, resolver: CountryResolver, now_utc: datetime) -> None:
        try:
            cached = {version for (version,) in client.execute(_CACHE_VERSIONS_SQL)}
            used = {version for (version,) in client.execute(_USED_VERSIONS_SQL, {"now_utc": now_utc})}
            for version in cached - used - {resolver.version}:
                client.execute(_DROP_CACHE_SQL, {"geo_version": version})
                logger.info("action=geo_cache_version_dropped geo_version=%s", version)
        except Exception as e:
            logger.error("action=geo_cache_cleanup_failed error=%s", str(e))
