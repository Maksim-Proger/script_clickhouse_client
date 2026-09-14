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

_CATALOG_SELECT = """
    SELECT l.id, l.name, l.description, l.created_by, l.created_at, l.updated_at,
           l.status, l.source_type, l.current_version, l.last_error,
           coalesce(cur.item_count, 0) AS item_count,
           cur.source_filters,
           EXISTS (SELECT 1 FROM feed_list_versions h
                   WHERE h.list_id = l.id AND h.status = 'history') AS has_history,
           b.version AS pending_version,
           b.status  AS pending_status
    FROM feed_lists l
    LEFT JOIN feed_list_versions cur
           ON cur.list_id = l.id AND cur.version = l.current_version
    LEFT JOIN feed_list_versions b
           ON b.list_id = l.id AND b.status IN ('building', 'pending_sync')
"""


class FeedListRepository:
    def __init__(self, db: DatabaseManager):
        self.db = db

    @property
    def is_connected(self) -> bool:
        return self.db.is_connected

    async def create_list(self,
                          conn,
                          name: str,
                          description: str,
                          created_by: str,
                          source_type: str) -> asyncpg.Record:
        return await conn.fetchrow(
            """
            INSERT INTO feed_lists (name, description, created_by, source_type)
            VALUES ($1, $2, $3, $4)
            RETURNING *
            """,
            name, description, created_by, source_type,
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

    async def finalize_version(self, list_id: int, version: int) -> int:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchval(
                """
                UPDATE feed_list_versions v
                SET status            = 'pending_sync',
                    item_count        = (SELECT count(*) FROM feed_list_items i
                                         WHERE i.list_id = v.list_id AND i.version = v.version),
                    last_error        = NULL,
                    mirror_cursor     = NULL,
                    mirror_updated_at = NULL,
                    sync_attempts     = 0,
                    next_attempt_at   = now()
                WHERE v.list_id = $1 AND v.version = $2
                RETURNING item_count
                """,
                list_id, version,
            )

    async def get_versions_to_sync(self, limit: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT list_id, version, item_count, mirror_cursor, mirror_updated_at, sync_attempts
                FROM feed_list_versions
                WHERE status = 'pending_sync'
                  AND (next_attempt_at IS NULL OR next_attempt_at <= now())
                ORDER BY next_attempt_at NULLS FIRST
                LIMIT $1
                """,
                limit,
            )

    async def start_mirror_sync(self, list_id: int, version: int, mirror_updated_at: datetime) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET mirror_updated_at = $3, mirror_cursor = NULL "
                "WHERE list_id = $1 AND version = $2",
                list_id, version, mirror_updated_at,
            )

    async def save_mirror_cursor(self, list_id: int, version: int, cursor: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET mirror_cursor = $3 WHERE list_id = $1 AND version = $2",
                list_id, version, cursor,
            )

    async def schedule_mirror_retry(self, list_id: int, version: int, error: str,
                                    attempts: int, delay_minutes: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET sync_attempts = $3, last_error = $4, "
                "next_attempt_at = now() + make_interval(mins => $5) "
                "WHERE list_id = $1 AND version = $2",
                list_id, version, attempts, error[:1000], delay_minutes,
            )

    async def list_catalog(
            self,
            search: Optional[str],
            status: Optional[str],
            page: int,
            page_size: int,
    ) -> tuple[list[asyncpg.Record], int]:
        conditions, args = ["l.status <> 'deleting'"], []

        if search:
            args.append(f"%{search}%")
            conditions.append(f"l.name ILIKE ${len(args)}")
        if status:
            args.append(status)
            conditions.append(f"l.status = ${len(args)}")

        where = f"WHERE {' AND '.join(conditions)}"

        async with self.db.pool.acquire() as conn:
            total = await conn.fetchval(f"SELECT count(*) FROM feed_lists l {where}", *args)
            rows = await conn.fetch(
                f"{_CATALOG_SELECT} {where} "
                f"ORDER BY l.updated_at DESC LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
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
                "SELECT id, name, current_version AS version, status FROM feed_lists "
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

    async def mark_for_deletion(self, list_id: int, grace_minutes: int) -> None:
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE feed_lists SET status = 'deleting', updated_at = now() WHERE id = $1",
                    list_id,
                )
                await conn.execute(
                    "UPDATE feed_list_versions SET status = 'deleting', "
                    "next_attempt_at = now() + make_interval(mins => $2) WHERE list_id = $1",
                    list_id, grace_minutes,
                )

    async def get_versions_to_delete(self, limit: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT list_id, version
                FROM feed_list_versions
                WHERE status = 'deleting' AND next_attempt_at <= now()
                ORDER BY next_attempt_at
                LIMIT $1
                """,
                limit,
            )

    async def delete_items_chunk(self, list_id: int, version: int, status: str, limit: int) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM feed_list_items WHERE ctid IN (
                    SELECT i.ctid FROM feed_list_items i
                    WHERE i.list_id = $1 AND i.version = $2
                      AND EXISTS (SELECT 1 FROM feed_list_versions v
                                  WHERE v.list_id = $1 AND v.version = $2 AND v.status = $3)
                    LIMIT $4
                )
                """,
                list_id, version, status, limit,
            )
            return int(result.split()[-1])

    async def delete_items(self, list_id: int, version: int) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM feed_list_items WHERE list_id = $1 AND version = $2",
                list_id,
                version
            )
            return int(result.split()[-1])

    async def create_version(self,
                             conn,
                             list_id: int,
                             version: int,
                             status: str,
                             build_kind: str,
                             source_filters: str,
                             created_by: str) -> asyncpg.Record:
        return await conn.fetchrow(
            """
            INSERT INTO feed_list_versions
                (list_id, version, status, build_kind, source_filters, created_by)
            VALUES ($1, $2, $3, $4, $5::jsonb, $6)
            RETURNING *
            """,
            list_id, version, status, build_kind, source_filters, created_by,
        )

    async def next_version_number(self, conn, list_id: int) -> int:
        await conn.execute("SELECT id FROM feed_lists WHERE id = $1 FOR UPDATE", list_id)
        return await conn.fetchval(
            "SELECT coalesce(max(version), 0) + 1 FROM feed_list_versions WHERE list_id = $1",
            list_id,
        )

    async def get_version(self, list_id: int, version: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT * FROM feed_list_versions WHERE list_id = $1 AND version = $2",
                list_id, version,
            )

    async def find_active_build(self, list_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT version, status FROM feed_list_versions "
                "WHERE list_id = $1 AND status IN ('building', 'pending_sync')",
                list_id,
            )

    async def get_history(self, list_id: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                """
                SELECT version, created_at, item_count, created_by, build_kind
                FROM feed_list_versions
                WHERE list_id = $1 AND status = 'history'
                ORDER BY version DESC
                """,
                list_id,
            )

    async def get_catalog_row(self, list_id: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                f"{_CATALOG_SELECT} WHERE l.id = $1 AND l.status <> 'deleting'", list_id
            )

    async def copy_items(self, list_id: int, from_version: int, to_version: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO feed_list_items
                    (list_id, version, value, value_type, value_net, score, risk_level,
                     asn, country, source, first_seen, last_seen)
                SELECT list_id, $3, value, value_type, value_net, score, risk_level,
                       asn, country, source, first_seen, last_seen
                FROM feed_list_items
                WHERE list_id = $1 AND version = $2
                """,
                list_id, from_version, to_version,
            )

    async def merge_items(self, list_id: int, version: int, items: list[tuple]) -> None:
        if not items:
            return
        async with self.db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "CREATE TEMP TABLE staging_items "
                    "(LIKE feed_list_items INCLUDING DEFAULTS) ON COMMIT DROP"
                )
                await conn.copy_records_to_table(
                    "staging_items",
                    records=[(list_id, version, *item) for item in items],
                    columns=_ITEM_COLUMNS,
                )
                await conn.execute(
                    """
                    INSERT INTO feed_list_items
                        (list_id, version, value, value_type, value_net, score,
                         risk_level, asn, country, source, first_seen, last_seen)
                    SELECT list_id, version, value, value_type, value_net, score,
                           risk_level, asn, country, source, first_seen, last_seen
                    FROM staging_items
                    ON CONFLICT (list_id, version, value) DO UPDATE SET
                        score      = coalesce(EXCLUDED.score, feed_list_items.score),
                        risk_level = coalesce(EXCLUDED.risk_level, feed_list_items.risk_level),
                        asn        = coalesce(EXCLUDED.asn, feed_list_items.asn),
                        country    = coalesce(EXCLUDED.country, feed_list_items.country),
                        last_seen  = greatest(feed_list_items.last_seen, EXCLUDED.last_seen)
                    """
                )

    async def start_restore(self, list_id: int, version: int) -> Optional[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                UPDATE feed_list_versions
                SET status = 'building', purge_after = NULL, mirror_cursor = NULL,
                    mirror_updated_at = NULL, sync_attempts = 0, last_error = NULL
                WHERE list_id = $1 AND version = $2 AND status = 'history'
                RETURNING blob
                """,
                list_id, version,
            )

    async def revert_to_history(self, list_id: int, version: int, delay_minutes: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET status = 'history', next_attempt_at = NULL, "
                "purge_after = now() + make_interval(mins => $3) "
                "WHERE list_id = $1 AND version = $2",
                list_id, version, delay_minutes,
            )

    async def mark_version_deleting(self, list_id: int, version: int, delay_minutes: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET status = 'deleting', "
                "next_attempt_at = now() + make_interval(mins => $3) "
                "WHERE list_id = $1 AND version = $2",
                list_id, version, delay_minutes,
            )

    async def clear_purge_after(self, list_id: int, version: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_list_versions SET purge_after = NULL WHERE list_id = $1 AND version = $2",
                list_id, version,
            )

    async def delete_version_record(self, list_id: int, version: int) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "DELETE FROM feed_list_versions WHERE list_id = $1 AND version = $2",
                list_id, version,
            )

    async def set_list_error(self, list_id: int, error: str) -> None:
        async with self.db.pool.acquire() as conn:
            await conn.execute(
                "UPDATE feed_lists SET last_error = $2, updated_at = now() WHERE id = $1",
                list_id, error[:1000],
            )

    async def get_versions_to_purge(self, limit: int) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT list_id, version FROM feed_list_versions "
                "WHERE status = 'history' AND purge_after <= now() LIMIT $1",
                limit,
            )

    async def get_building_versions(self) -> list[asyncpg.Record]:
        async with self.db.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT list_id, version FROM feed_list_versions WHERE status = 'building'"
            )

    async def delete_empty_deleting_lists(self) -> int:
        async with self.db.pool.acquire() as conn:
            result = await conn.execute(
                """
                DELETE FROM feed_lists l
                WHERE l.status = 'deleting'
                  AND NOT EXISTS (SELECT 1 FROM feed_list_versions v WHERE v.list_id = l.id)
                """
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
