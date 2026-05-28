import asyncio
import logging
from typing import Optional

from clickhouse_driver import Client as CHClient

logger = logging.getLogger(__name__)


_CREATE_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS feedgen.ip_reputation_snapshots
(
    run_id            String,
    computed_at       DateTime,
    ip_address        IPv4,
    score             Float32,
    risk_level        LowCardinality(String),
    events_count      UInt64,
    max_5m_events     UInt64,
    max_hour_events   UInt64,
    active_5m_windows UInt64,
    active_hours      UInt64,
    active_days       UInt64,
    sources_count     UInt8,
    first_seen        DateTime,
    last_seen         DateTime
)
ENGINE = MergeTree
PARTITION BY toDate(computed_at)
ORDER BY (run_id, score, ip_address)
TTL computed_at + INTERVAL 14 DAY
"""

_SCORING_SQL = """
WITH
    now() AS ts_now,
    concat('iprep_', toString(toUnixTimestamp(ts_now))) AS reputation_run_id,

    base AS (
        SELECT
            ip_address,
            source,
            blocked_at,
            toStartOfInterval(blocked_at, INTERVAL 5 minute) AS w5,
            toStartOfHour(blocked_at)                         AS wh,
            toDate(blocked_at)                                AS wd
        FROM feedgen.blocked_ips
        WHERE blocked_at >= ts_now - INTERVAL 7 DAY
    ),

    by_ip AS (
        SELECT
            ip_address,
            count()         AS N,
            uniqExact(source) AS S,
            uniqExact(w5)   AS W5,
            uniqExact(wh)   AS WH,
            uniqExact(wd)   AS WD,
            min(blocked_at) AS first_seen,
            max(blocked_at) AS last_seen
        FROM base
        GROUP BY ip_address
    ),

    m5 AS (
        SELECT ip_address, max(c) AS M5
        FROM (
            SELECT ip_address, w5, count() AS c
            FROM base
            GROUP BY ip_address, w5
        )
        GROUP BY ip_address
    ),

    mh AS (
        SELECT ip_address, max(c) AS MH
        FROM (
            SELECT ip_address, wh, count() AS c
            FROM base
            GROUP BY ip_address, wh
        )
        GROUP BY ip_address
    ),

    scored AS (
        SELECT
            reputation_run_id AS run_id,
            ts_now            AS computed_at,
            b.ip_address      AS ip_address,

            round(
                100 * (
                    0.35 * greatest(
                        least(1, log(1 + M5) / log(101)),
                        0.7  * least(1, log(1 + MH) / log(501)),
                        0.4  * least(1, log(1 + N)  / log(5001))
                    )
                    + 0.35 * (
                        0.3 * least(1, log(1 + W5) / log(501))
                        + 0.4 * least(1, log(1 + WH) / log(169))
                        + 0.3 * least(1, log(1 + WD) / log(8))
                    )
                    + 0.15 * if(S > 1, 1, 0)
                    + 0.15 * (0.25 + 0.75 * exp(-dateDiff('hour', last_seen, ts_now) / 72))
                ),
                1
            ) AS score,

            N, M5, MH, W5, WH, WD, S,
            first_seen,
            last_seen
        FROM by_ip AS b
        INNER JOIN m5 USING ip_address
        INNER JOIN mh USING ip_address
    )

SELECT
    run_id,
    computed_at,
    ip_address,
    score,
    multiIf(
        score >= 80, 'critical',
        score >= 60, 'high',
        score >= 40, 'bad',
        score >= 20, 'suspicious',
        'low'
    )              AS risk_level,
    N              AS events_count,
    M5             AS max_5m_events,
    MH             AS max_hour_events,
    W5             AS active_5m_windows,
    WH             AS active_hours,
    WD             AS active_days,
    S              AS sources_count,
    first_seen,
    last_seen
FROM scored
WHERE score >= 20
ORDER BY score DESC, events_count DESC
LIMIT 100000
"""

_INSERT_SQL = """
INSERT INTO feedgen.ip_reputation_snapshots
    (run_id, computed_at, ip_address, score, risk_level,
     events_count, max_5m_events, max_hour_events,
     active_5m_windows, active_hours, active_days,
     sources_count, first_seen, last_seen)
VALUES
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

    def _ensure_table_sync(self) -> None:
        self._get_client().execute(_CREATE_TABLE_DDL)
        logger.info("action=ensure_table status=ok table=ip_reputation_snapshots")

    def _run_snapshot_sync(self) -> int:
        client = self._get_client()

        logger.info("action=scoring_query_start")
        rows = client.execute(_SCORING_SQL)

        if not rows:
            logger.warning("action=scoring_empty message='Query returned 0 candidates'")
            return 0

        logger.info("action=scoring_query_done candidates=%d", len(rows))
        client.execute(_INSERT_SQL, rows)
        logger.info("action=snapshot_insert_done rows=%d", len(rows))
        return len(rows)

    async def ensure_table(self) -> None:
        await asyncio.to_thread(self._ensure_table_sync)

    async def run_snapshot(self) -> int:
        return await asyncio.to_thread(self._run_snapshot_sync)

    def close(self) -> None:
        if self._client:
            self._client.disconnect()
            self._client = None
            logger.info("action=ch_client_close status=ok")

