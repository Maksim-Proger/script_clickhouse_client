from typing import Optional, Dict
from pydantic import BaseModel
from ch_handler import read_from_clickhouse


# Модель данных: только те параметры, которые придут с нового фронтенда
class CHSimpleFilters(BaseModel):
    profile: str
    period: Optional[Dict[str, str]] = None


def build_deduplicated_ips_query(filters: CHSimpleFilters) -> str:
    conditions = []

    # Фильтрация по профилю (из тела POST-запроса)
    conditions.append(f"profile = '{filters.profile}'")

    # Обработка периода (из словаря period: {"from": "...", "to": "..."})
    if filters.period:
        p_from = filters.period.get("from")
        p_to = filters.period.get("to")
        if p_from:
            conditions.append(f"blocked_at >= '{p_from}'")
        if p_to:
            conditions.append(f"blocked_at <= '{p_to}'")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""

    # SQL запрос с дедупликацией:
    # Схлопываем миллионы строк по ip_address и находим время ПЕРВОЙ блокировки
    return f"""
    SELECT 
        ip_address,
        min(blocked_at) as first_detected,
        argMin(source, blocked_at) as source,
        argMin(profile, blocked_at) as profile
    FROM feedgen.blocked_ips 
    {where_clause} 
    GROUP BY ip_address 
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