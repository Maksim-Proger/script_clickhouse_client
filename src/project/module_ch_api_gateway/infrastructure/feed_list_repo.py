import logging
from typing import Optional

import asyncpg

from datetime import datetime
from project.module_ch_api_gateway.infrastructure.db import DatabaseManager

logger = logging.getLogger("ch-api-gateway.feed_lists")

_ITEM_COLUMNS = [
    "list_id", "version", "value", "value_type", "value_net", "score", "risk_level",
    "asn", "country", "source", "first_seen", "last_seen",
]

_ITEM_SELECT = (
    "value, value_type, score, risk_level, asn, country, "
    "source, first_seen, last_seen, created_at"
)


class FeedListRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db

    @property
    def is_connected(self) -> bool:
        return self.db.is_connected

    async def create_list(
            self,
            name: str,
            description: str,
            created_by: str,
            source_type: str,
            source_filters: str,
            status: str,
    ) -> asyncpg.Record:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                INSERT INTO feed_lists
                    (name, description, created_by, source_type, source_filters, status, version, item_count)
                VALUES ($1, $2, $3, $4, $5::jsonb, $6, 1, 0)
                RETURNING *
                """,
                name, description, created_by, source_type, source_filters, status,
            )

    async def insert_items(self, list_id: int, version: int, items: list[tuple]) -> None:
        if not items:
            return
        async with self.db.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "feed_list_items",
                records=[(list_id, version, *item) for item in items],
                columns=_ITEM_COLUMNS,
            )

    async def finalize_list(self, list_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE feed_lists l
                SET status            = 'pending_sync',
                    item_count        = (SELECT count(*) FROM feed_list_items i
                                         WHERE i.list_id = l.id AND i.version = l.version),
                    updated_at        = now(),
                    last_error        = NULL,
                    mirror_cursor     = NULL,
                    mirror_updated_at = NULL,
                    sync_attempts     = 0,
                    next_attempt_at   = now()
                WHERE l.id = $1
                RETURNING *
                """,
                list_id,
            )

    async def fail_list(self, list_id: int, error: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET status = 'failed', last_error = $2, updated_at = now() WHERE id = $1",
                list_id, error[:1000],
            )

    async def fail_stale_lists(self) -> int:
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                rows = await conn.fetch(
                    "UPDATE feed_lists SET status = 'failed', "
                    "last_error = 'Сборка прервана перезапуском сервиса', updated_at = now() "
                    "WHERE status = 'creating' "
                    "RETURNING id, version"
                )
                for r in rows:
                    await conn.execute(
                        "DELETE FROM feed_list_items WHERE list_id = $1 AND version = $2",
                        r["id"], r["version"],
                    )
            if rows:
                logger.warning(
                    "action=feed_lists_stale_failed count=%d ids=%s",
                    len(rows), ",".join(str(r["id"]) for r in rows),
                )
            return len(rows)

    async def get_lists_for_mirror_sync(self, limit: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, version, item_count, mirror_cursor, mirror_updated_at, sync_attempts
                FROM feed_lists
                WHERE status = 'pending_sync'
                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                ORDER BY next_attempt_at NULLS FIRST
                LIMIT $1
                """,
                limit,
            )

    async def start_mirror_sync(self, list_id: int, mirror_updated_at: datetime) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET mirror_updated_at = $2, mirror_cursor = NULL WHERE id = $1",
                list_id, mirror_updated_at,
            )

    async def save_mirror_cursor(self, list_id: int, cursor: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET mirror_cursor = $2 WHERE id = $1",
                list_id, cursor,
            )

    async def schedule_mirror_retry(self, list_id: int, error: str, attempts: int, delay_minutes: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET sync_attempts = $2, last_error = $3, "
                "next_attempt_at = now() + make_interval(mins => $4), updated_at = now() "
                "WHERE id = $1",
                list_id, attempts, error[:1000], delay_minutes,
            )

    async def mark_sync_failed(self, list_id: int, error: str, attempts: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET status = 'sync_failed', sync_attempts = $2, "
                "last_error = $3, next_attempt_at = NULL, updated_at = now() WHERE id = $1",
                list_id, attempts, error[:1000],
            )

    async def activate_list(self, list_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                "UPDATE feed_lists SET status = 'active', last_error = NULL, "
                "next_attempt_at = NULL, updated_at = now() WHERE id = $1 RETURNING *",
                list_id,
            )

    async def list_catalog(
            self,
            search: Optional[str],
            status: Optional[str],
            page: int,
            page_size: int,
    ) -> tuple[list[asyncpg.Record], int]:
        conditions, args = ["status <> 'deleting'"], []

        if search:
            args.append(f"%{search}%")
            conditions.append(f"name ILIKE ${len(args)}")
        if status:
            args.append(status)
            conditions.append(f"status = ${len(args)}")

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""

        async with self.db.pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT count(*) FROM feed_lists {where}", *args)
            rows = await conn.fetch(
                f"SELECT * FROM feed_lists {where} "
                f"ORDER BY updated_at DESC LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
                *args, page_size, (page - 1) * page_size,
            )
        return rows, int(total)

    async def get_list(self, list_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM feed_lists WHERE id = $1 AND status <> 'deleting'", list_id
            )

    async def get_lists_by_ids(self, list_ids: list[int]) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT id, name, version, status, item_count FROM feed_lists "
                "WHERE id = ANY($1::int[]) AND status <> 'deleting'",
                list_ids,
            )

    async def get_items_page(self, list_id: int, version: int, page: int, page_size: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                f"""
                SELECT {_ITEM_SELECT}
                FROM feed_list_items
                WHERE list_id = $1 AND version = $2
                ORDER BY value
                LIMIT $3 OFFSET $4
                """,
                list_id, version, page_size, (page - 1) * page_size,
            )

    async def iter_items(self,
                         list_id: int,
                         version: int,
                         chunk_size: int = 50_000,
                         after_value: str = ""):
        last_value = after_value
        while True:
            async with self.db.pool.acquire() as conn:
                rows = await conn.fetch(
                    f"""
                    SELECT {_ITEM_SELECT}
                    FROM feed_list_items
                    WHERE list_id = $1 AND version = $2 AND value > $3
                    ORDER BY value
                    LIMIT $4
                    """,
                    list_id, version, last_value, chunk_size,
                )
            if not rows:
                return
            yield rows
            last_value = rows[-1]["value"]

    async def set_status(self, list_id: int, status: str) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                "UPDATE feed_lists SET status = $2, updated_at = now() WHERE id = $1 RETURNING *",
                list_id, status,
            )

    async def mark_for_deletion(self, list_id: int, grace_minutes: int) -> bool:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                "UPDATE feed_lists SET status = 'deleting', sync_attempts = 0, last_error = NULL, "
                "next_attempt_at = now() + make_interval(mins => $2), updated_at = now() "
                "WHERE id = $1",
                list_id, grace_minutes,
            )
            return result.split()[-1] == "1"

    async def get_lists_for_deletion(self, limit: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT id, version, sync_attempts
                FROM feed_lists
                WHERE status = 'deleting' AND next_attempt_at <= now()
                ORDER BY next_attempt_at
                LIMIT $1
                """,
                limit,
            )

    async def delete_items_chunk(self, list_id: int, limit: int) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM feed_list_items WHERE ctid IN ("
                "SELECT ctid FROM feed_list_items WHERE list_id = $1 LIMIT $2"
                ")",
                list_id, limit,
            )
            return int(result.split()[-1])

    async def continue_deletion(self, list_id: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET next_attempt_at = now() WHERE id = $1", list_id
            )

    async def purge_list(self, list_id: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute("DELETE FROM feed_lists WHERE id = $1", list_id)

    async def delete_items(self, list_id: int, version: int) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM feed_list_items WHERE list_id = $1 AND version = $2",
                list_id,
                version
            )
            return int(result.split()[-1])

    async def create_search_session(
            self,
            search_id: str,
            owner: str,
            kind: str,
            filters: str,
            ttl_minutes: int,
    ) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO search_sessions (search_id, owner, kind, filters, expires_at)
                VALUES ($1, $2, $3, $4::jsonb, now() + make_interval(mins => $5))
                """,
                search_id, owner, kind, filters, ttl_minutes,
            )

    async def add_search_rows(self, search_id: str, start_seq: int, rows: list[str]) -> None:
        if not rows:
            return
        async with self.db.pool.acquire() as conn:
            await conn.copy_records_to_table(
                "search_session_rows",
                records=[(search_id, start_seq + i, row) for i, row in enumerate(rows)],
                columns=["search_id", "seq", "row"],
            )

    async def finish_search_session(self, search_id: str, total: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE search_sessions SET total = $2 WHERE search_id = $1",
                search_id, total,
            )

    async def get_search_session(self, search_id: str, owner: str, ttl_minutes: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM search_sessions WHERE search_id = $1 AND owner = $2 AND expires_at > now()",
                search_id, owner,
            )
            if row:
                await conn.execute(
                    "UPDATE search_sessions SET expires_at = now() + make_interval(mins => $2) WHERE search_id = $1",
                    search_id, ttl_minutes,
                )
            return row

    async def get_search_rows(self, search_id: str, start_seq: int, count: int) -> list[str]:
        async with self.db.pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT row FROM search_session_rows "
                "WHERE search_id = $1 AND seq >= $2 AND seq < $3 ORDER BY seq",
                search_id, start_seq, start_seq + count,
            )
        return [r["row"] for r in rows]

    async def evict_owner_sessions(self, owner: str, keep: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                """
                DELETE FROM search_sessions WHERE search_id IN (
                    SELECT search_id FROM search_sessions
                    WHERE owner = $1 ORDER BY created_at DESC OFFSET $2
                )
                """,
                owner, keep,
            )

    async def delete_search_session(self, search_id: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute("DELETE FROM search_sessions WHERE search_id = $1", search_id)

    async def cleanup_search_sessions(self) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute("DELETE FROM search_sessions WHERE expires_at < now()")
            count = int(result.split()[-1])
            if count > 0:
                logger.info("action=search_sessions_cleanup deleted=%d", count)
            return count
