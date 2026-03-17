import logging
import re
import asyncio

from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters
from project.module_ch_api_gateway.infrastructure.clickhouse_client import ClickHouseClient

logger = logging.getLogger("ch-api-gateway")

_IP_RE = re.compile(
    r"^(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)"
    r"(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}$"
)

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$")


def _escape_str(value: str) -> str:
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _safe_ip(value: str) -> str:
    if not _IP_RE.match(value):
        raise ValueError(f"Некорректный IP-адрес: {value!r}")
    return value


def _safe_date(value: str) -> str:
    if not _DATE_RE.match(value):
        raise ValueError(f"Некорректное значение даты: {value!r}")
    return value


class ClickHouseService:
    def __init__(self, client: ClickHouseClient):
        self.client = client

    @staticmethod
    def _build_conditions(filters: CHReadFilters) -> list:
        conditions = []

        if filters.blocked_at:
            conditions.append(f"toDate(blocked_at) = '{_safe_date(filters.blocked_at)}'")

        if filters.period:
            p_from = filters.period.get("from")
            p_to = filters.period.get("to")
            if p_from:
                conditions.append(f"blocked_at >= '{_safe_date(p_from)}'")
            if p_to:
                conditions.append(f"blocked_at <= '{_safe_date(p_to)}'")

        if filters.ip:
            conditions.append(f"ip_address = '{_safe_ip(filters.ip)}'")

        if filters.source:
            conditions.append(f"source = '{_escape_str(filters.source)}'")

        if filters.profile:
            conditions.append(f"profile = '{_escape_str(filters.profile)}'")

        return conditions

    @staticmethod
    def _build_blocked_ips_query(filters: CHReadFilters) -> str:
        conditions = ClickHouseService._build_conditions(filters)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        offset = (filters.page - 1) * filters.page_size
        return (
            f"SELECT * FROM feedgen.blocked_ips {where_clause} "
            f"ORDER BY blocked_at DESC "
            f"LIMIT {filters.page_size} OFFSET {offset}"
        )

    @staticmethod
    def _build_count_query(filters: CHReadFilters) -> str:
        conditions = ClickHouseService._build_conditions(filters)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return f"SELECT count() as total FROM feedgen.blocked_ips {where_clause}"

    @staticmethod
    def _build_deduplicated_query(filters: CHSimpleFilters) -> str:
        p_from = filters.period.get("from")
        p_to = filters.period.get("to")

        if not p_from or not p_to:
            raise ValueError("CHSimpleFilters.period должен содержать ключи 'from' и 'to'")

        conditions = [
            f"profile = '{_escape_str(filters.profile)}'",
            f"blocked_at >= '{_safe_date(p_from)}'",
            f"blocked_at <= '{_safe_date(p_to)}'",
        ]
        where_clause = f"WHERE {' AND '.join(conditions)}"
        return (
            f"SELECT ip_address, min(blocked_at) as first_detected, source, profile "
            f"FROM feedgen.blocked_ips {where_clause} "
            f"GROUP BY ip_address, source, profile "
            f"ORDER BY first_detected DESC LIMIT 500"
        )

    async def get_blocked_ips(self, filters: CHReadFilters):
        data_task = self.client.fetch_json(self._build_blocked_ips_query(filters))
        count_task = self.client.fetch_json(self._build_count_query(filters))

        try:
            data_res, count_res = await asyncio.gather(data_task, count_task)
            data = data_res.get("data", [])
            total = int(count_res["data"][0]["total"])
        except Exception as e:
            logger.error("action=ch_fetch_failed error=%s", str(e))
            data, total = [], 0

        return {
            "data": data,
            "total": total,
            "page": filters.page,
            "page_size": filters.page_size,
            "total_pages": (total + filters.page_size - 1) // filters.page_size if total > 0 else 1
        }

    async def get_simple_ips(self, filters: CHSimpleFilters) -> list:
        query = ClickHouseService._build_deduplicated_query(filters)
        result = await self.client.fetch_json(query)
        return result.get("data", [])
