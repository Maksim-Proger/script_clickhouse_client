import asyncio
import logging

logger = logging.getLogger("ch-api-gateway.reputation")

_LATEST_SNAPSHOT_WHERE = """
    WHERE (computed_at, run_id) = (
        SELECT computed_at, run_id
        FROM feedgen.ip_reputation_snapshots
        ORDER BY computed_at DESC, run_id DESC
        LIMIT 1
    )
"""

_INT_FIELDS = (
    "events_count", "max_5m_events", "max_hour_events",
    "active_5m_windows", "active_hours", "active_days", "sources_count",
)


def _build_snapshot_query(page: int, page_size: int) -> str:
    offset = (page - 1) * page_size
    return f"""
        SELECT ip_address, score, risk_level, events_count, max_5m_events, max_hour_events,
               active_5m_windows, active_hours, active_days, sources_count, first_seen, last_seen, computed_at
        FROM feedgen.ip_reputation_snapshots
        {_LATEST_SNAPSHOT_WHERE}
        ORDER BY score DESC
        LIMIT {page_size} OFFSET {offset}
    """


def _build_count_query() -> str:
    return f"SELECT count() as total FROM feedgen.ip_reputation_snapshots {_LATEST_SNAPSHOT_WHERE}"


class ReputationService:
    def __init__(self, ch_client, geoip_client):
        self.ch_client = ch_client
        self.geoip_client = geoip_client

    async def get_reputation(self, page: int = 1, page_size: int = 100) -> dict:
        try:
            data_res = await self.ch_client.fetch_json(_build_snapshot_query(page, page_size))
            records: list[dict] = data_res.get("data", [])

            count_res = await self.ch_client.fetch_json(_build_count_query())
            total = int(count_res["data"][0]["total"])
        except Exception as e:
            logger.error("action=reputation_fetch_failed error=%s", str(e))
            return {"data": [], "total": 0, "page": page, "page_size": page_size, "total_pages": 1}

        if not records:
            return {"data": [], "total": total, "page": page, "page_size": page_size, "total_pages": 1}

        for record in records:
            for field in _INT_FIELDS:
                if field in record and record[field] is not None:
                    record[field] = int(record[field])

        enriched = await asyncio.to_thread(self.geoip_client.enrich_batch, records)

        logger.info("action=reputation_enriched count=%d page=%d page_size=%d total=%d", len(enriched), page, page_size, total)

        return {
            "data": enriched,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
        }

