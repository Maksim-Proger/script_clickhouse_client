import httpx
import time
import logging
from pydantic import BaseModel, Field
from typing import Optional


logger = logging.getLogger("ch-client")

class PeriodFilter(BaseModel):
    from_date: Optional[str] = Field(None, alias="from")
    to_date: Optional[str] = Field(None, alias="to")

class CHReadFilters(BaseModel):
    blocked_at: Optional[str] = None
    period: Optional[dict] = None
    ip: Optional[str] = None
    source: Optional[str] = None
    profile: Optional[str] = None


def build_blocked_ips_query(filters: CHReadFilters) -> str:
    conditions = []

    # 1. Фильтр по точной дате/времени (если пришло в blocked_at)
    if filters.blocked_at:
        conditions.append(f"blocked_at = '{filters.blocked_at}'")

    # 2. Фильтр по периоду (из вашего JS объекта period)
    if filters.period:
        p_from = filters.period.get("from")
        p_to = filters.period.get("to")
        if p_from:
            conditions.append(f"blocked_at >= '{p_from}'")
        if p_to:
            conditions.append(f"blocked_at <= '{p_to}'")

    # 3. Остальные фильтры
    if filters.ip:
        conditions.append(f"ip_address = '{filters.ip}'")
    if filters.source:
        conditions.append(f"source = '{filters.source}'")
    if filters.profile:
        conditions.append(f"profile = '{filters.profile}'")

    where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    return f"SELECT * FROM feedgen.blocked_ips {where_clause} ORDER BY blocked_at DESC LIMIT 500"

async def read_from_clickhouse(
        query: str,
        host: str,
        port: int,
        timeout_sec: int,
) -> dict:
    url = f"http://{host}:{port}/"
    sql = f"{query} FORMAT JSON"

    start_time = time.perf_counter()
    try:
        async with httpx.AsyncClient(timeout=timeout_sec) as client:
            resp = await client.get(url, params={"query": sql})
            resp.raise_for_status()

            duration = time.perf_counter() - start_time
            logger.info("action=ch_query_success duration=%.2fs query=\"%s\"", duration, query[:100])
            return resp.json()
    except httpx.TimeoutException:
        logger.error("action=ch_query_timeout query=\"%s\"", query[:100])
        raise
    except Exception as e:
        logger.error("action=ch_query_failed error=%s", str(e))
        raise