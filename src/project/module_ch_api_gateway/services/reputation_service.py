import asyncio
import ipaddress
import logging

from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.search_session import SessionExpiredError

logger = logging.getLogger("ch-api-gateway.reputation")

_LATEST_SNAPSHOT_PREDICATE = """
    (computed_at, run_id) = (
        SELECT computed_at, run_id
        FROM feedgen.ip_reputation_snapshots
        ORDER BY computed_at DESC, run_id DESC
        LIMIT 1
    )
""".strip()

_SELECT_COLS = (
    "ip_address, score, risk_level, events_count, max_5m_events, max_hour_events, "
    "active_5m_windows, active_hours, active_days, sources_count, first_seen, last_seen, computed_at"
)

_INT_FIELDS = (
    "events_count", "max_5m_events", "max_hour_events",
    "active_5m_windows", "active_hours", "active_days", "sources_count",
)

_FETCH_ALL_CAP = 200_000


def _safe_cidr(value: str) -> str:
    v = value.strip()
    if "/" in v:
        return str(ipaddress.ip_network(v, strict=False))
    return f"{ipaddress.ip_address(v)}/32"


def _build_where(filters: ReputationFilters) -> str:
    conditions = [_LATEST_SNAPSHOT_PREDICATE]

    if filters.score_from is not None:
        conditions.append(f"score >= {float(filters.score_from)}")
    if filters.score_to is not None:
        conditions.append(f"score <= {float(filters.score_to)}")
    if filters.ip:
        conditions.append(f"isIPAddressInRange(ip_address, '{_safe_cidr(filters.ip)}')")

    return "WHERE " + " AND ".join(conditions)


def _build_page_query(where: str, page: int, page_size: int) -> str:
    offset = (page - 1) * page_size
    return (
        f"SELECT {_SELECT_COLS} FROM feedgen.ip_reputation_snapshots "
        f"{where} ORDER BY score DESC LIMIT {page_size} OFFSET {offset}"
    )


def _build_count_query(where: str) -> str:
    return f"SELECT count() as total FROM feedgen.ip_reputation_snapshots {where}"


def _build_all_query(where: str) -> str:
    return (
        f"SELECT {_SELECT_COLS} FROM feedgen.ip_reputation_snapshots "
        f"{where} ORDER BY score DESC LIMIT {_FETCH_ALL_CAP}"
    )


def _coerce_ints(records: list[dict]) -> list[dict]:
    for record in records:
        for field in _INT_FIELDS:
            if field in record and record[field] is not None:
                record[field] = int(record[field])
    return records


def _has_geo_filter(filters: ReputationFilters) -> bool:
    return bool(filters.asn) or bool(filters.country)


def _apply_geo_filter(records: list[dict], filters: ReputationFilters) -> list[dict]:
    if filters.asn:
        asn_set = set(filters.asn)
        if filters.asn_exclude:
            records = [r for r in records if r.get("asn_number") not in asn_set]
        else:
            records = [r for r in records if r.get("asn_number") in asn_set]

    if filters.country:
        country_set = {c.strip().upper() for c in filters.country if c.strip()}
        if filters.country_exclude:
            records = [r for r in records if (r.get("country") or "").upper() not in country_set]
        else:
            records = [r for r in records if (r.get("country") or "").upper() in country_set]

    return records


def _page_slice(rows: list, page: int, page_size: int) -> list:
    start = (page - 1) * page_size
    return rows[start:start + page_size]


class ReputationService:
    def __init__(self, ch_client, geoip_client, sessions):
        self.ch_client = ch_client
        self.geoip_client = geoip_client
        self.sessions = sessions

    async def _enrich(self, records: list[dict]) -> list[dict]:
        return await asyncio.to_thread(self.geoip_client.enrich_batch, records)

    async def _load_filtered(self, where: str, filters: ReputationFilters) -> list[dict]:
        res = await self.ch_client.fetch_json(_build_all_query(where))
        records = await self._enrich(_coerce_ints(res.get("data", [])))
        return _apply_geo_filter(records, filters)

    @staticmethod
    def _envelope(data: list, total: int, page: int, page_size: int, search_id: str = None) -> dict:
        env = {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
        }
        if search_id is not None:
            env["search_id"] = search_id
        return env

    async def get_reputation(self, filters: ReputationFilters, user: str) -> dict:
        page, page_size = filters.page, filters.page_size

        if filters.search_id:
            rows = self.sessions.get(user, filters.search_id)
            if rows is None:
                raise SessionExpiredError(filters.search_id)
            return self._envelope(_page_slice(rows, page, page_size), len(rows), page, page_size,
                                  search_id=filters.search_id)

        where = _build_where(filters)

        try:
            if _has_geo_filter(filters):
                rows = await self._load_filtered(where, filters)
                search_id = self.sessions.create(user, rows)
                logger.info("action=reputation_search_created search_id=%s total=%d", search_id, len(rows))
                return self._envelope(_page_slice(rows, page, page_size), len(rows), page, page_size,
                                      search_id=search_id)

            data_res = await self.ch_client.fetch_json(_build_page_query(where, page, page_size))
            page_rows = _coerce_ints(data_res.get("data", []))

            count_res = await self.ch_client.fetch_json(_build_count_query(where))
            total = int(count_res["data"][0]["total"])

            page_rows = await self._enrich(page_rows)
        except Exception as e:
            logger.error("action=reputation_fetch_failed error=%s", str(e))
            return self._envelope([], 0, page, page_size)

        return self._envelope(page_rows, total, page, page_size)

    async def export_reputation(self, filters: ReputationFilters, user: str) -> dict:
        if filters.search_id:
            rows = self.sessions.get(user, filters.search_id)
            if rows is None:
                raise SessionExpiredError(filters.search_id)
            records = [{"ip_address": r["ip_address"]} for r in rows] if filters.only_ip else rows
            return {"data": records, "total": len(records)}

        where = _build_where(filters)

        try:
            if _has_geo_filter(filters):
                records = await self._load_filtered(where, filters)
            else:
                res = await self.ch_client.fetch_json(_build_all_query(where))
                records = _coerce_ints(res.get("data", []))
                if not filters.only_ip:
                    records = await self._enrich(records)

            if filters.only_ip:
                records = [{"ip_address": r["ip_address"]} for r in records]
        except Exception as e:
            logger.error("action=reputation_export_failed error=%s", str(e))
            return {"data": [], "total": 0}

        logger.info("action=reputation_export count=%d only_ip=%s", len(records), filters.only_ip)
        return {"data": records, "total": len(records)}
