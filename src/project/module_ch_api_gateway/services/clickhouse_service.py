import logging
import re

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
            if filters.period.from_date:
                conditions.append(f"blocked_at >= '{_safe_date(filters.period.from_date)}'")
            if filters.period.to_date:
                conditions.append(f"blocked_at <= '{_safe_date(filters.period.to_date)}'")

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
            f"SELECT * FROM `feedgen`.`blocked_ips` {where_clause} "
            f"ORDER BY blocked_at DESC "
            f"LIMIT {filters.page_size} OFFSET {offset}"
        )

    @staticmethod
    def _build_count_query(filters: CHReadFilters) -> str:
        conditions = ClickHouseService._build_conditions(filters)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return f"SELECT count() as total FROM `feedgen`.`blocked_ips` {where_clause}"

    @staticmethod
    def _build_deduplicated_query(filters: CHSimpleFilters) -> str:
        p_from = filters.period.from_date
        p_to = filters.period.to_date

        if not p_from or not p_to:
            raise ValueError("В фильтре периода должны быть указаны даты 'from' и 'to'")

        conditions = [
            f"profile = '{_escape_str(filters.profile)}'",
            f"blocked_at >= '{_safe_date(p_from)}'",
            f"blocked_at <= '{_safe_date(p_to)}'",
        ]

        if filters.ip:
            conditions.append(f"ip_address = '{_safe_ip(filters.ip)}'")

        where_clause = f"WHERE {' AND '.join(conditions)}"
        return (
            f"SELECT ip_address, min(blocked_at) as first_detected, source, profile "
            f"FROM `feedgen`.`blocked_ips` {where_clause} "
            f"GROUP BY ip_address, source, profile "
            f"ORDER BY first_detected DESC LIMIT 500"
        )

    async def get_blocked_ips(self, filters: CHReadFilters):
        try:
            data_query = self._build_blocked_ips_query(filters)
            data_res = await self.client.fetch_json(data_query)
            data = data_res.get("data", [])

            count_query = self._build_count_query(filters)
            count_res = await self.client.fetch_json(count_query)
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

    @staticmethod
    def _build_export_query(filters: CHReadFilters) -> str:
        conditions = ClickHouseService._build_conditions(filters)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        return (
            f"SELECT * FROM `feedgen`.`blocked_ips` {where_clause} "
            f"ORDER BY blocked_at DESC "
            f"LIMIT 1000000"
        )

    @staticmethod
    def _build_export_unique_query(filters: CHReadFilters) -> str:
        conditions = ClickHouseService._build_conditions(filters)
        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return (
            f"SELECT ip_address, min(blocked_at) as first_detected, source, profile "
            f"FROM `feedgen`.`blocked_ips` {where_clause} "
            f"GROUP BY ip_address, source, profile "
            f"ORDER BY first_detected DESC "
            f"LIMIT 1000000"
        )

    async def get_export_ips(self, filters: CHReadFilters) -> list:
        try:
            if filters.unique_ips:
                query = self._build_export_unique_query(filters)
            else:
                query = self._build_export_query(filters)
            result = await self.client.fetch_json(query)
            return result.get("data", [])
        except Exception as e:
            logger.error("action=ch_export_failed error=%s", str(e))
            return []

