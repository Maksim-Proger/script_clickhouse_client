from typing import Optional, Dict
from pydantic import BaseModel
from project.module_ch_api_gateway.ch_handler import read_from_clickhouse


class CHSimpleFilters(BaseModel):
    profile: str
    period: Dict[str, str]

def build_deduplicated_ips_query(filters: CHSimpleFilters) -> str:
    conditions = [
        f"profile = '{filters.profile}'",
        f"blocked_at >= '{filters.period['from']}'",
        f"blocked_at <= '{filters.period['to']}'"
    ]

    where_clause = f"WHERE {' AND '.join(conditions)}"

    return f"""
    SELECT 
        ip_address,
        min(blocked_at) as first_detected,
        source,
        profile
    FROM feedgen.blocked_ips 
    {where_clause} 
    GROUP BY 
        ip_address, 
        source, 
        profile
    ORDER BY first_detected DESC 
    LIMIT 500
    """

async def handle_ch_simple_request(filters: CHSimpleFilters, ch_cfg: dict) -> dict:
    query = build_deduplicated_ips_query(filters)
    return await read_from_clickhouse(
        query=query,
        host=ch_cfg["host"],
        port=ch_cfg["http_port"],
        timeout_sec=ch_cfg["timeout_sec"]
    )