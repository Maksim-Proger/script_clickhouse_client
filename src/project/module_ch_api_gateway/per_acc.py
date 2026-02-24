from typing import Optional, Dict
from pydantic import BaseModel
from project.module_ch_api_gateway.ch_handler import read_from_clickhouse


# Модель данных: только те параметры, которые придут с нового фронтенда
class CHSimpleFilters(BaseModel):
    profile: str
    period: Optional[Dict[str, str]] = None


def build_deduplicated_ips_query(filters: CHSimpleFilters) -> str:
    # Собираем список условий сразу (List Literal), фильтруя те, что None
    raw_conditions = [
        f"profile = '{filters.profile}'",
        f"blocked_at >= '{filters.period.get('from')}'" if filters.period and filters.period.get('from') else None,
        f"blocked_at <= '{filters.period.get('to')}'" if filters.period and filters.period.get('to') else None,
    ]

    # Очищаем список от None значений
    conditions = [c for c in raw_conditions if c is not None]

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # SQL запрос с корректным GROUP BY (устраняем ошибку 500)
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
    """
    Выполняет запрос к ClickHouse, используя общую функцию транспорта из ch_handler.py
    """
    query = build_deduplicated_ips_query(filters)
    return await read_from_clickhouse(
        query=query,
        host=ch_cfg["host"],
        port=ch_cfg["http_port"],
        timeout_sec=ch_cfg["timeout_sec"]
    )