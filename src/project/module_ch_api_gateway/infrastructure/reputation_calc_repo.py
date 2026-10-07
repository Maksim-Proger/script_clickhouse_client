import asyncpg
from datetime import datetime
from typing import Optional

from project.module_ch_api_gateway.infrastructure.db import DatabaseManager


class ReputationCalcRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db

    @property
    def is_connected(self) -> bool:
        return self.db.is_connected

    async def create_calc(self, source: str, profile: Optional[str], created_by: str) -> asyncpg.Record:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO reputation_calcs (source, profile, created_by)
                VALUES ($1, $2, $3)
                RETURNING *
                """,
                source, profile, created_by,
            )

    async def set_period(self, calc_id: int, period_from: datetime, period_to: datetime) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE reputation_calcs SET period_from = $2, period_to = $3 WHERE id = $1",
                calc_id, period_from, period_to,
            )

    async def finish_calc(self, calc_id: int, row_count: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE reputation_calcs SET status = 'ready', row_count = $2, last_error = NULL, "
                "finished_at = now() WHERE id = $1",
                calc_id, row_count,
            )

    async def fail_calc(self, calc_id: int, error: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE reputation_calcs SET status = 'failed', row_count = 0, last_error = $2, "
                "finished_at = now() WHERE id = $1",
                calc_id, error[:1000],
            )

    async def get_calc(self, calc_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow("SELECT * FROM reputation_calcs WHERE id = $1", calc_id)

    async def list_calcs(self, page: int, page_size: int) -> tuple[list[asyncpg.Record], int]:
        async with self.db.pool.acquire() as conn:
            total = await conn.fetchval("SELECT count(*) FROM reputation_calcs")
            rows = await conn.fetch(
                "SELECT * FROM reputation_calcs ORDER BY id DESC LIMIT $1 OFFSET $2",
                page_size, (page - 1) * page_size,
            )
        return rows, int(total)

    async def get_building_calcs(self) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch("SELECT id FROM reputation_calcs WHERE status = 'building'")

    async def delete_calc(self, calc_id: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM reputation_calcs WHERE id = $1 AND status != 'building'",
                calc_id,
            )
