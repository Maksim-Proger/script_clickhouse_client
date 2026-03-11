from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters


class ClickHouseService:
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_conditions(filters: CHReadFilters) -> list:
        conditions = []
        if filters.blocked_at: conditions.append(f"toDate(blocked_at) = '{filters.blocked_at}'")
        if filters.period:
            p_from, p_to = filters.period.get("from"), filters.period.get("to")
            if p_from: conditions.append(f"blocked_at >= '{p_from}'")
            if p_to: conditions.append(f"blocked_at <= '{p_to}'")
        if filters.ip: conditions.append(f"ip_address = '{filters.ip}'")
        if filters.source: conditions.append(f"source = '{filters.source}'")
        if filters.profile: conditions.append(f"profile = '{filters.profile}'")
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
        conditions = [
            f"profile = '{filters.profile}'",
            f"blocked_at >= '{filters.period['from']}'",
            f"blocked_at <= '{filters.period['to']}'"
        ]
        where_clause = f"WHERE {' AND '.join(conditions)}"
        return (
            f"SELECT ip_address, min(blocked_at) as first_detected, source, profile "
            f"FROM feedgen.blocked_ips {where_clause} "
            f"GROUP BY ip_address, source, profile "
            f"ORDER BY first_detected DESC LIMIT 500"
        )

    async def get_blocked_ips(self, filters: CHReadFilters):
        data = await self.client.fetch_json(self._build_blocked_ips_query(filters))
        count = await self.client.fetch_json(self._build_count_query(filters))
        total = int(count["data"][0]["total"])
        return {
            "data": data["data"],
            "total": total,
            "page": filters.page,
            "page_size": filters.page_size,
            "total_pages": (total + filters.page_size - 1) // filters.page_size
        }

    async def get_simple_ips(self, filters: CHSimpleFilters):
        query = ClickHouseService._build_deduplicated_query(filters)
        return await self.client.fetch_json(query)
