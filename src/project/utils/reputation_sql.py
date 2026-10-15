BLOCKED_IPS_TABLE = "feedgen.blocked_ips_data"
GEO_CACHE_TABLE = "feedgen.ip_geo_cache_data"
GEO_MATRIX_TABLE = "feedgen.geo_reputation_matrix_data"
GEO_MATRIX_RUNS_TABLE = "feedgen.geo_reputation_matrix_runs_data"

_SCORING_TEMPLATE = """
WITH
    toDateTime(%(now_utc)s, 'UTC') AS ts_now,
    concat('iprep_', toString(toUnixTimestamp(ts_now))) AS reputation_run_id,
    base AS (
        SELECT ip_address, source, blocked_at,
               toStartOfInterval(blocked_at, INTERVAL 5 minute) AS w5,
               toStartOfHour(blocked_at) AS wh, toDate(blocked_at) AS wd
        FROM {blocked_ips}
        WHERE {where}
    ),
    by_ip AS (
        SELECT ip_address, count() AS N, uniqExact(source) AS S,
               uniqExact(w5) AS W5, uniqExact(wh) AS WH, uniqExact(wd) AS WD,
               min(blocked_at) AS first_seen, max(blocked_at) AS last_seen
        FROM base GROUP BY ip_address
    ),
    m5 AS (
        SELECT ip_address, max(c) AS M5
        FROM (SELECT ip_address, w5, count() AS c FROM base GROUP BY ip_address, w5)
        GROUP BY ip_address
    ),
    mh AS (
        SELECT ip_address, max(c) AS MH
        FROM (SELECT ip_address, wh, count() AS c FROM base GROUP BY ip_address, wh)
        GROUP BY ip_address
    ),
    scored AS (
        SELECT b.ip_address AS ip_address, round(100 * (
            0.35 * greatest(
                least(1, log(1 + M5) / log(101)),
                0.7 * least(1, log(1 + MH) / log(501)),
                0.4 * least(1, log(1 + N) / log(5001))
            )
            + 0.35 * (
                0.3 * least(1, log(1 + W5) / log(501))
                + 0.4 * least(1, log(1 + WH) / log(169))
                + 0.3 * least(1, log(1 + WD) / log(8))
            )
            + 0.15 * if(S > 1, 1, 0)
            + 0.15 * (0.25 + 0.75 * exp(-dateDiff('hour', last_seen, ts_now) / 72))
        ), 1) AS base_score, N, M5, MH, W5, WH, WD, S, first_seen, last_seen
        FROM by_ip AS b
        INNER JOIN m5 ON b.ip_address = m5.ip_address
        INNER JOIN mh ON b.ip_address = mh.ip_address
    ),
    candidates AS (
        SELECT * FROM scored WHERE base_score >= 20 - %(max_bonus)s
    ),
    geo AS (
        SELECT ip_address, country_iso, lookup_status
        FROM {geo_cache} FINAL WHERE geo_version = %(geo_version)s
    ),
    country_matrix AS (
        SELECT country_iso, category, geo_coefficient, geo_bonus
        FROM {geo_matrix} FINAL
        WHERE matrix_version = %(matrix_version)s AND geo_version = %(geo_version)s
    ),
    enriched AS (
        SELECT s.ip_address AS ip_address, s.base_score AS base_score,
               s.N AS N, s.M5 AS M5, s.MH AS MH, s.W5 AS W5, s.WH AS WH,
               s.WD AS WD, s.S AS S, s.first_seen AS first_seen, s.last_seen AS last_seen,
               coalesce(nullIf(g.country_iso, ''), 'ZZ') AS geo_country,
               coalesce(g.lookup_status, '') AS lookup_status,
               coalesce(m.geo_coefficient, 0.0) AS country_coefficient,
               coalesce(m.geo_bonus, 0.0) AS country_bonus,
               coalesce(nullIf(m.category, ''), 'insufficient') AS country_category
        FROM candidates AS s
        LEFT ANY JOIN geo AS g ON s.ip_address = g.ip_address
        LEFT ANY JOIN country_matrix AS m ON g.country_iso = m.country_iso
    ),
    adjusted AS (
        SELECT *, multiIf(
            %(matrix_version)s = '', 'unavailable',
            geo_country = 'ZZ' OR lookup_status != 'known', 'unknown',
            country_category
        ) AS geo_category,
        if(geo_category IN ('high', 'elevated', 'noticeable'),
           greatest(0.0, least(1.0, country_coefficient)), 0.0) AS geo_coefficient,
        if(geo_category IN ('high', 'elevated', 'noticeable'),
           greatest(0.0, least(%(max_bonus)s, country_bonus)), 0.0) AS geo_bonus
        FROM enriched
    ),
    final_scores AS (
        SELECT *, round(least(100.0, base_score + geo_bonus), 1) AS score
        FROM adjusted
    )
SELECT reputation_run_id AS run_id, ts_now AS computed_at, ip_address, score,
       multiIf(score >= 80, 'critical', score >= 60, 'high', score >= 40, 'bad',
               score >= 20, 'suspicious', 'low') AS risk_level,
       N AS events_count, M5 AS max_5m_events, MH AS max_hour_events,
       W5 AS active_5m_windows, WH AS active_hours, WD AS active_days,
       S AS sources_count, first_seen, last_seen,
       base_score, geo_coefficient, geo_bonus, geo_country, geo_category,
       %(matrix_version)s AS geo_matrix_version, %(geo_version)s AS geo_version
FROM final_scores
WHERE score >= 20
ORDER BY score DESC, events_count DESC, ip_address
LIMIT 100000
"""


def window_where(days: int = 7, by_source: bool = False, by_profile: bool = False) -> str:
    where = f"blocked_at >= ts_now - INTERVAL {int(days)} DAY AND blocked_at < ts_now"
    if by_source:
        where += " AND source = %(source)s"
    if by_profile:
        where += " AND profile = %(profile)s"
    return where


def build_scoring_sql(by_source: bool = False, by_profile: bool = False) -> str:
    return _SCORING_TEMPLATE.format(
        blocked_ips=BLOCKED_IPS_TABLE,
        geo_cache=GEO_CACHE_TABLE,
        geo_matrix=GEO_MATRIX_TABLE,
        where=window_where(by_source=by_source, by_profile=by_profile),
    )
