import hashlib
import logging
import maxminddb
from clickhouse_driver import Client as CHClient
from datetime import datetime, timezone
from typing import Optional

from project.utils.reputation_sql import (
    BLOCKED_IPS_TABLE,
    GEO_CACHE_TABLE,
    GEO_MATRIX_RUNS_TABLE,
    GEO_MATRIX_TABLE,
)

logger = logging.getLogger("utils.geo")

UNKNOWN_COUNTRY = "ZZ"

_INSERT_BATCH = 100_000

_MISSING_IPS_SQL = f"""
WITH toDateTime(%(now_utc)s, 'UTC') AS ts_now
SELECT DISTINCT ip_address
FROM {BLOCKED_IPS_TABLE}
WHERE {{where}}
  AND ip_address NOT IN (
      SELECT ip_address FROM {GEO_CACHE_TABLE} WHERE geo_version = %(geo_version)s
  )
"""

_INSERT_CACHE_SQL = (
    f"INSERT INTO {GEO_CACHE_TABLE} "
    "(geo_version, ip_address, country_iso, lookup_status, updated_at) VALUES"
)

_SELECT_MATRIX_SQL = f"""
SELECT r.matrix_version, r.geo_version, r.max_bonus, r.computed_at, r.period_end
FROM {GEO_MATRIX_RUNS_TABLE} AS r FINAL
INNER JOIN (
    SELECT matrix_version, count() AS visible_countries
    FROM {GEO_MATRIX_TABLE} FINAL
    GROUP BY matrix_version
) AS m ON r.matrix_version = m.matrix_version
WHERE r.status = 'ready'
  AND r.expected_countries > 0
  AND m.visible_countries = r.expected_countries
  AND r.computed_at <= toDateTime(%(now_utc)s, 'UTC')
  AND r.computed_at >= toDateTime(%(now_utc)s, 'UTC') - INTERVAL 7 DAY
  AND r.period_end <= toDateTime(%(now_utc)s, 'UTC')
  {{rules}}
ORDER BY r.computed_at DESC, r.matrix_version DESC
LIMIT 1
"""

NO_MATRIX = {"matrix_version": "", "geo_version": "", "max_bonus": 0.0}


def naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def file_version(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


class CountryResolver:
    def __init__(self, path: str):
        self.version = file_version(path)
        self._reader = maxminddb.open_database(path)

    def __enter__(self) -> "CountryResolver":
        return self

    def __exit__(self, *exc) -> None:
        self.close()

    def close(self) -> None:
        self._reader.close()

    def lookup(self, ip) -> tuple[str, str]:
        try:
            record = self._reader.get(ip)
        except Exception:
            return UNKNOWN_COUNTRY, "decode_error"
        if not record:
            return UNKNOWN_COUNTRY, "missing"
        country = record.get("country") if isinstance(record, dict) else None
        iso = country.get("iso_code") if isinstance(country, dict) else None
        if iso is None:
            return UNKNOWN_COUNTRY, "missing"
        if not isinstance(iso, str) or len(iso) != 2 or not iso.isalpha():
            return UNKNOWN_COUNTRY, "decode_error"
        return iso.upper(), "known"


def resolve_missing_ips(client: CHClient, resolver: CountryResolver, where: str, params: dict) -> int:
    rows = client.execute(
        _MISSING_IPS_SQL.format(where=where),
        {**params, "geo_version": resolver.version},
    )
    if not rows:
        return 0

    updated_at = datetime.now(timezone.utc)
    data = [(resolver.version, ip, *resolver.lookup(ip), updated_at) for (ip,) in rows]
    for i in range(0, len(data), _INSERT_BATCH):
        client.execute(_INSERT_CACHE_SQL, data[i:i + _INSERT_BATCH])
    return len(data)


def select_matrix(client: CHClient, now_utc: datetime, rules_version: Optional[str] = None) -> Optional[dict]:
    rules = "AND r.rules_version = %(rules_version)s" if rules_version is not None else ""
    rows = client.execute(
        _SELECT_MATRIX_SQL.format(rules=rules),
        {"now_utc": now_utc, "rules_version": rules_version},
    )
    if not rows:
        return None
    matrix_version, geo_version, max_bonus, computed_at, period_end = rows[0]
    return {
        "matrix_version": matrix_version,
        "geo_version": geo_version,
        "max_bonus": float(max_bonus),
        "computed_at": naive_utc(computed_at),
        "period_end": naive_utc(period_end),
    }


def prepare_geo(client: CHClient,
                resolver: CountryResolver,
                now_utc: datetime,
                where: str,
                params: dict,
                rules_version: Optional[str] = None) -> dict:
    matrix = select_matrix(client, now_utc, rules_version)
    if matrix is None:
        logger.warning("action=geo_matrix_missing message='Scoring without geo bonus'")
        return dict(NO_MATRIX)

    age_hours = (naive_utc(now_utc) - matrix["computed_at"]).total_seconds() / 3600
    logger.info("action=geo_matrix_selected matrix_version=%s age_hours=%.1f",
                matrix["matrix_version"], age_hours)

    if matrix["geo_version"] == resolver.version:
        added = resolve_missing_ips(client, resolver, where, {**params, "now_utc": now_utc})
        logger.info("action=geo_cache_filled added=%d", added)
    else:
        logger.warning(
            "action=geo_version_mismatch matrix_geo_version=%s file_geo_version=%s "
            "message='New IPs stay without country until the matrix is rebuilt'",
            matrix["geo_version"], resolver.version,
        )

    return {key: matrix[key] for key in NO_MATRIX}
