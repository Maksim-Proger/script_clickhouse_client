import asyncio
import ipaddress
import json
import logging
from typing import Optional

from project.module_ch_api_gateway.services.clickhouse_service import build_exclude_conditions
from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.feed_list_service import (
    CHUNK_SIZE,
    SessionExpiredError,
    SourceUnavailableError,
    check_source_size,
)

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


def _safe_cidr(value: str) -> str:
    v = value.strip()
    if "/" in v:
        return str(ipaddress.ip_network(v, strict=False))
    return f"{ipaddress.ip_address(v)}/32"


def _build_where(filters: ReputationFilters, exclude_lists: Optional[list[dict]] = None) -> str:
    conditions = [_LATEST_SNAPSHOT_PREDICATE]

    if filters.score_from is not None:
        conditions.append(f"score >= {float(filters.score_from)}")
    if filters.score_to is not None:
        conditions.append(f"score <= {float(filters.score_to)}")
    if filters.ip:
        conditions.append(f"isIPAddressInRange(IPv4NumToString(ip_address), '{_safe_cidr(filters.ip)}')")

    conditions.extend(build_exclude_conditions(exclude_lists))

    return "WHERE " + " AND ".join(conditions)


def _build_page_query(where: str, page: int, page_size: int) -> str:
    offset = (page - 1) * page_size
    return (
        f"SELECT {_SELECT_COLS} FROM feedgen.ip_reputation_snapshots "
        f"{where} ORDER BY score DESC, ip_address LIMIT {page_size} OFFSET {offset}"
    )


def _build_count_query(where: str) -> str:
    return f"SELECT count() as total FROM feedgen.ip_reputation_snapshots {where}"


def _coerce_ints(records: list[dict]) -> list[dict]:
    for record in records:
        for field in _INT_FIELDS:
            if field in record and record[field] is not None:
                record[field] = int(record[field])
    return records


def _has_geo_filter(filters: ReputationFilters) -> bool:
    return bool(filters.asn) or bool(filters.country)


def needs_session(filters: ReputationFilters) -> bool:
    return _has_geo_filter(filters) or bool(filters.exclude_list_ids)


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


def _only_ip_row(raw: str) -> str:
    return json.dumps({"ip_address": json.loads(raw)["ip_address"]}, ensure_ascii=False)


class ReputationService:
    def __init__(self, ch_client, geoip_client, stream_client):
        self.ch_client = ch_client
        self.geoip_client = geoip_client
        self.stream_client = stream_client

    async def _enrich(self, records: list[dict]) -> list[dict]:
        return await asyncio.to_thread(self.geoip_client.enrich_batch, records)

    async def iter_snapshot_rows(self,
                                 filters: ReputationFilters,
                                 chunk_size: int,
                                 enrich: bool = True,
                                 exclude_lists: Optional[list[dict]] = None):
        query = (
            f"SELECT {_SELECT_COLS} FROM feedgen.ip_reputation_snapshots "
            f"{_build_where(filters, exclude_lists)} ORDER BY score DESC, ip_address"
        )
        async for chunk in self.stream_client.iter_rows(query, chunk_size):
            raw = _coerce_ints(chunk)
            if not enrich:
                yield raw
                continue
            filtered = _apply_geo_filter(await self._enrich(raw), filters)
            if filtered:
                yield filtered

    async def count_snapshot(self, filters: ReputationFilters) -> int:
        try:
            res = await self.ch_client.fetch_json(_build_count_query(_build_where(filters)))
            return int(res["data"][0]["total"])
        except Exception as e:
            logger.error("action=reputation_count_failed error=%s", str(e))
            raise SourceUnavailableError()

    @staticmethod
    def _envelope(data: list, total: int, page: int, page_size: int, search_id: str = None) -> dict:
        env = {
            "data": data,
            "total": total,
            "page": page,
            "page_size": page_size,
            "total_pages": None if total is None
            else (total + page_size - 1) // page_size if total > 0 else 1,
        }
        if search_id is not None:
            env["search_id"] = search_id
        return env

    async def get_reputation(self,
                             filters: ReputationFilters,
                             user: str,
                             feed_service,
                             exclude_lists: Optional[list[dict]] = None) -> dict:

        page, page_size = filters.page, filters.page_size

        try:
            if filters.search_id:
                result = await feed_service.get_search_page(user, filters.search_id, page, page_size)
                if result is None:
                    raise SessionExpiredError(filters.search_id)
                return self._envelope(result["data"], result["total"], page, page_size,
                                      search_id=filters.search_id)

            if needs_session(filters):
                check_source_size(await self.count_snapshot(filters))
                built = await feed_service.build_reputation_search(
                    user, self, filters, exclude_lists,
                )
                result = await feed_service.get_search_page(user, built["search_id"], page, page_size)
                return self._envelope(result["data"] if result else [], built["total"], page, page_size,
                                      search_id=built["search_id"])

            where = _build_where(filters)

            data_res = await self.ch_client.fetch_json(_build_page_query(where, page, page_size))
            page_rows = _coerce_ints(data_res.get("data", []))

            if page == 1:
                count_res = await self.ch_client.fetch_json(_build_count_query(where))
                total = int(count_res["data"][0]["total"])
            else:
                total = None

            page_rows = await self._enrich(page_rows)
        except (SessionExpiredError, SourceUnavailableError, ValueError):
            raise
        except Exception as e:
            logger.error("action=reputation_fetch_failed error=%s", str(e))
            raise SourceUnavailableError()

        return self._envelope(page_rows, total, page, page_size)

    async def start_export(self,
                           filters: ReputationFilters,
                           user: str,
                           feed_service,
                           exclude_lists: Optional[list[dict]] = None) -> tuple:
        try:
            if filters.search_id:
                total = await feed_service.get_search_total(user, filters.search_id)
                if total is None:
                    raise SessionExpiredError(filters.search_id)
                return filters.search_id, total

            if needs_session(filters):
                check_source_size(await self.count_snapshot(filters))
                built = await feed_service.build_reputation_search(
                    user, self, filters, exclude_lists,
                )
                return built["search_id"], built["total"]

            total = await self.count_snapshot(filters)
            check_source_size(total)
            return None, total
        except (SessionExpiredError, SourceUnavailableError, ValueError):
            raise
        except Exception as e:
            logger.error("action=reputation_export_failed error=%s", str(e))
            raise SourceUnavailableError()

    async def iter_export_chunks(self, filters: ReputationFilters, user: str, feed_service, search_id):
        logger.info("action=reputation_export search_id=%s only_ip=%s user=%s", search_id, filters.only_ip, user)

        if search_id:
            async for chunk in feed_service.iter_search_rows(user, search_id):
                yield [_only_ip_row(row) for row in chunk] if filters.only_ip else chunk
            return

        async for rows in self.iter_snapshot_rows(filters, CHUNK_SIZE, enrich=not filters.only_ip):
            if filters.only_ip:
                rows = [{"ip_address": r["ip_address"]} for r in rows]
            yield await asyncio.to_thread(
                lambda batch=rows: [json.dumps(r, ensure_ascii=False, default=str) for r in batch]
            )
