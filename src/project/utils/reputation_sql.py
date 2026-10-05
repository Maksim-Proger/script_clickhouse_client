_SCORING_TEMPLATE = """
               WITH {ts_now} AS ts_now, concat('iprep_', toString(toUnixTimestamp(ts_now))) AS reputation_run_id, base AS (
                   SELECT
                   ip_address, source, blocked_at, toStartOfInterval(blocked_at, INTERVAL 5 minute) AS w5, toStartOfHour(blocked_at) AS wh, toDate(blocked_at) AS wd
                   FROM feedgen.blocked_ips
                   WHERE {where}
                   ), by_ip AS (
                   SELECT
                   ip_address, count () AS N, uniqExact(source) AS S, uniqExact(w5) AS W5, uniqExact(wh) AS WH, uniqExact(wd) AS WD, min (blocked_at) AS first_seen, max (blocked_at) AS last_seen
                   FROM base
                   GROUP BY ip_address
                   ), m5 AS (
                   SELECT ip_address, max (c) AS M5
                   FROM (
                   SELECT ip_address, w5, count () AS c
                   FROM base
                   GROUP BY ip_address, w5
                   )
                   GROUP BY ip_address
                   ), mh AS (
                   SELECT ip_address, max (c) AS MH
                   FROM (
                   SELECT ip_address, wh, count () AS c
                   FROM base
                   GROUP BY ip_address, wh
                   )
                   GROUP BY ip_address
                   ), scored AS (
                   SELECT
                   reputation_run_id AS run_id, ts_now AS computed_at, b.ip_address AS ip_address, round(
                   100 * (
                   0.35 * greatest(
                   least(1, log(1 + M5) / log(101)), 0.7 * least(1, log(1 + MH) / log(501)), 0.4 * least(1, log(1 + N) / log(5001))
                   )
                   + 0.35 * (
                   0.3 * least(1, log(1 + W5) / log(501))
                   + 0.4 * least(1, log(1 + WH) / log(169))
                   + 0.3 * least(1, log(1 + WD) / log(8))
                   )
                   + 0.15 * if(S > 1, 1, 0)
                   + 0.15 * (0.25 + 0.75 * exp(-dateDiff('hour', last_seen, ts_now) / 72))
                   ), 1
                   ) AS score, N, M5, MH, W5, WH, WD, S, first_seen, last_seen
                   FROM by_ip AS b
                   INNER JOIN m5 ON b.ip_address = m5.ip_address
                   INNER JOIN mh ON b.ip_address = mh.ip_address
                   )

               SELECT run_id,
                      computed_at,
                      ip_address,
                      score,
                      multiIf(
                              score >= 80, 'critical',
                              score >= 60, 'high',
                              score >= 40, 'bad',
                              score >= 20, 'suspicious',
                              'low'
                      )  AS risk_level,
                      N  AS events_count,
                      M5 AS max_5m_events,
                      MH AS max_hour_events,
                      W5 AS active_5m_windows,
                      WH AS active_hours,
                      WD AS active_days,
                      S  AS sources_count,
                      first_seen,
                      last_seen
               FROM scored
               WHERE score >= 20
               ORDER BY score DESC, events_count DESC LIMIT 100000 \
               """


def build_scoring_sql(ts_now: str = "now()", by_source: bool = False, by_profile: bool = False) -> str:
    where = "blocked_at >= ts_now - INTERVAL 7 DAY"
    if by_source:
        where += " AND source = %(source)s"
    if by_profile:
        where += " AND profile = %(profile)s"
    return _SCORING_TEMPLATE.format(ts_now=ts_now, where=where)
