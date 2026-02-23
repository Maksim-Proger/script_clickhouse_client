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
    date: Optional[str] = None
    blocked_at: Optional[str] = None
    period: Optional[PeriodFilter] = None
    ip: Optional[str] = None
    source: Optional[str] = None
    profile: Optional[str] = None

def build_blocked_ips_query(filters: CHReadFilters) -> str:
    conditions = []

    exact_date = filters.date or filters.blocked_at
    if exact_date:
        if " " in exact_date:
            conditions.append(f"blocked_at = '{exact_date}'")
        else:
            conditions.append(f"toDate(blocked_at) = '{exact_date}'")

    if filters.period:
        if filters.period.from_date:
            conditions.append(f"blocked_at >= '{filters.period.from_date}'")
        if filters.period.to_date:
            conditions.append(f"blocked_at <= '{filters.period.to_date}'")

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