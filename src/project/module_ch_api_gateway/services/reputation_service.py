import asyncio
import logging

logger = logging.getLogger("ch-api-gateway.reputation")

_READ_LAST_SNAPSHOT_SQL = """
                          SELECT ip_address,
                                 score,
                                 risk_level,
                                 events_count,
                                 max_5m_events,
                                 max_hour_events,
                                 active_5m_windows,
                                 active_hours,
                                 active_days,
                                 sources_count,
                                 first_seen,
                                 last_seen,
                                 computed_at
                          FROM feedgen.ip_reputation_snapshots
                          WHERE (computed_at, run_id) = (SELECT computed_at, run_id
                                                         FROM feedgen.ip_reputation_snapshots
                                                         ORDER BY computed_at DESC, run_id DESC
                              LIMIT 1
                              )
                          ORDER BY score DESC \
                          """

_INT_FIELDS = (
    "events_count", "max_5m_events", "max_hour_events",
    "active_5m_windows", "active_hours", "active_days", "sources_count",
)


class ReputationService:
    def __init__(self, ch_client, geoip_client):
        self.ch_client = ch_client
        self.geoip_client = geoip_client

    async def get_reputation(self) -> list[dict]:
        try:
            result = await self.ch_client.fetch_json(_READ_LAST_SNAPSHOT_SQL)
            records: list[dict] = result.get("data", [])
        except Exception as e:
            logger.error("action=reputation_fetch_failed error=%s", str(e))
            return []

        if not records:
            return []

        for record in records:
            for field in _INT_FIELDS:
                if field in record and record[field] is not None:
                    record[field] = int(record[field])

        enriched = await asyncio.to_thread(self.geoip_client.enrich_batch, records)

        logger.info("action=reputation_enriched total=%d", len(enriched))
        return enriched
