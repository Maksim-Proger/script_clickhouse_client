from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters


class ClickHouseService:
    def __init__(self, client):
        self.client = client

    @staticmethod
    def _build_blocked_ips_query(filters: CHReadFilters) -> str:
        conditions = []
        if filters.blocked_at: conditions.append(f"toDate(blocked_at) = '{filters.blocked_at}'")
        if filters.period:
            p_from, p_to = filters.period.get("from"), filters.period.get("to")
            if p_from: conditions.append(f"blocked_at >= '{p_from}'")
            if p_to: conditions.append(f"blocked_at <= '{p_to}'")
        if filters.ip: conditions.append(f"ip_address = '{filters.ip}'")
        if filters.source: conditions.append(f"source = '{filters.source}'")
        if filters.profile: conditions.append(f"profile = '{filters.profile}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return f"SELECT * FROM feedgen.blocked_ips {where_clause} ORDER BY blocked_at DESC LIMIT 500"

    @staticmethod
    def _build_deduplicated_query(filters: CHSimpleFilters) -> str:
        conditions = [
            f"profile = '{filters.profile}'",
            f"blocked_at >= '{filters.period['from']}'",
            f"blocked_at <= '{filters.period['to']}'"
        ]
        where_clause = f"WHERE {' AND '.join(conditions)}"
        return f"SELECT ip_address, min(blocked_at) as first_detected, source, profile FROM feedgen.blocked_ips {where_clause} GROUP BY ip_address, source, profile ORDER BY first_detected DESC LIMIT 500"

    async def get_blocked_ips(self, filters: CHReadFilters):
        query = ClickHouseService._build_blocked_ips_query(filters)
        return await self.client.fetch_json(query)

    async def get_simple_ips(self, filters: CHSimpleFilters):
        query = ClickHouseService._build_deduplicated_query(filters)
        return await self.client.fetch_json(query)