import asyncio
import asyncpg
import ipaddress
import json
import logging
import uuid
from contextlib import aclosing
from datetime import datetime, timezone
from typing import Any, Optional

from project.module_ch_api_gateway.infrastructure.feed_list_archive import dump_version, load_version
from project.module_ch_api_gateway.infrastructure.feed_list_mirror_client import FeedListMirrorClient
from project.module_ch_api_gateway.infrastructure.feed_list_repo import FeedListRepository

logger = logging.getLogger("ch-api-gateway.feed_lists")

MAX_EXCLUDE_LISTS = 20
MAX_SOURCE_ROWS = 1_000_000
MAX_MANUAL_VALUES = 1_000_000
DEFAULT_PERIOD_DAYS = 7

CHUNK_SIZE = 50_000
SEARCH_TTL_MINUTES = 15
SEARCH_SESSIONS_PER_USER = 10
CLEANUP_INTERVAL = 60

DELETION_GRACE_MINUTES = 60
DELETION_CHUNK = 50_000
DELETION_CHUNKS_PER_TICK = 5

MIRROR_SYNC_INTERVAL = 15
MIRROR_SYNC_BATCH = 5
MIRROR_COUNT_RETRY_DELAY = 2.0
MIRROR_BACKOFF_MINUTES = (1, 2, 5, 10, 30)
MIRROR_MAX_ATTEMPTS = 10
HISTORY_DEPTH = 3

_DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")

_ITEM_FIELDS = (
    "value", "value_type", "value_net", "score", "risk_level",
    "asn", "country", "source", "first_seen", "last_seen",
)


class SessionExpiredError(Exception):
    pass


class ListBusyError(Exception):
    pass


class ListArchivedError(Exception):
    pass


class SourceUnavailableError(Exception):
    pass


def check_source_size(total: int) -> None:
    if total > MAX_SOURCE_ROWS:
        raise ValueError(
            f"Под фильтр подпадает слишком много записей: {total}, максимум {MAX_SOURCE_ROWS}. Уточните фильтры"
        )


def _parse_dt(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        raw = value.split(".")[0]
        for fmt in _DT_FORMATS:
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
            except ValueError:
                pass
    return None


def _to_tuples(items: list[dict]) -> list[tuple]:
    return [tuple(item[field] for field in _ITEM_FIELDS) for item in items]


def _rows_to_tuples(rows: list, row_to_item) -> list[tuple]:
    return _to_tuples([item for item in (row_to_item(r) for r in rows) if item])


def _rows_to_json(rows: list[dict]) -> list[str]:
    return [json.dumps(r, ensure_ascii=False, default=str) for r in rows]


def _mirror_range(value: str, value_type: str) -> tuple[int, int]:
    if value_type == "cidr":
        net = ipaddress.ip_network(value, strict=False)
        return int(net.network_address), int(net.broadcast_address)
    addr = int(ipaddress.ip_address(value))
    return addr, addr


def _items_to_mirror_rows(rows: list, list_id: int, version: int, updated_at) -> list[tuple]:
    return [
        (
            list_id,
            version,
            r["value"],
            r["value_type"],
            *_mirror_range(r["value"], r["value_type"]),
            updated_at,
        )
        for r in rows
    ]


def _mirror_backoff_minutes(attempts: int) -> int:
    idx = min(attempts, len(MIRROR_BACKOFF_MINUTES)) - 1
    return MIRROR_BACKOFF_MINUTES[idx]


def _reputation_row_to_item(row: dict) -> Optional[dict]:
    ip = row.get("ip_address")
    if not ip:
        return None
    score = row.get("score")
    asn = row.get("asn_number")
    return {
        "value": ip,
        "value_type": "ip",
        "value_net": ipaddress.ip_interface(ip),
        "score": float(score) if score is not None else None,
        "risk_level": row.get("risk_level"),
        "asn": int(asn) if asn is not None else None,
        "country": row.get("country"),
        "source": "reputation",
        "first_seen": _parse_dt(row.get("first_seen")),
        "last_seen": _parse_dt(row.get("last_seen")),
    }


def _blocked_row_to_item(row: dict) -> Optional[dict]:
    ip = row.get("ip_address")
    if not ip:
        return None
    return {
        "value": ip,
        "value_type": "ip",
        "value_net": ipaddress.ip_interface(ip),
        "score": None,
        "risk_level": None,
        "asn": None,
        "country": None,
        "source": row.get("source") or "blocked_ips",
        "first_seen": _parse_dt(row.get("first_detected") or row.get("blocked_at")),
        "last_seen": _parse_dt(row.get("last_detected") or row.get("blocked_at")),
    }


def build_items_from_values(values: list[str]) -> list[tuple]:
    if len(values) > MAX_MANUAL_VALUES:
        raise ValueError(f"Слишком много значений, максимум {MAX_MANUAL_VALUES}")
    items: dict[str, dict] = {}
    invalid: list[str] = []
    for raw in values:
        value = raw.strip()
        if not value:
            continue
        try:
            if "/" in value:
                net = ipaddress.ip_network(value, strict=False)
                if net.version != 4:
                    raise ValueError(value)
                value, value_type = str(net), "cidr"
            else:
                addr = ipaddress.ip_address(value)
                if addr.version != 4:
                    raise ValueError(value)
                value, value_type = str(addr), "ip"
        except ValueError:
            invalid.append(value)
            continue
        if value not in items:
            items[value] = {
                "value": value,
                "value_type": value_type,
                "value_net": ipaddress.ip_interface(value),
                "score": None,
                "risk_level": None,
                "asn": None,
                "country": None,
                "source": "manual",
                "first_seen": None,
                "last_seen": None,
            }

    if invalid:
        preview = ", ".join(invalid[:5])
        raise ValueError(f"Некорректные значения, поддерживаются только IPv4 ({len(invalid)} шт.): {preview}")

    return _to_tuples(list(items.values()))


class FeedListService:
    def __init__(self,
                 repo: FeedListRepository,
                 mirror: FeedListMirrorClient):
        self.repo = repo
        self.mirror = mirror
        self._background_tasks: set[asyncio.Task] = set()

    @property
    def is_available(self) -> bool:
        return self.repo.is_connected

    async def resolve_exclude_lists(self, list_ids: list[int]) -> Optional[list[dict]]:
        if not list_ids:
            return None
        if len(list_ids) > MAX_EXCLUDE_LISTS:
            raise ValueError(f"Можно исключить не более {MAX_EXCLUDE_LISTS} списков за раз")

        rows = await self.repo.get_lists_by_ids(list_ids)
        found = {r["id"] for r in rows}
        missing = [i for i in list_ids if i not in found]
        if missing:
            raise ValueError(f"Списки не найдены: {missing}")

        inactive = [r["name"] for r in rows if r["status"] != "active" or r["version"] is None]
        if inactive:
            raise ValueError(f"Списки не активны и не могут применяться как исключения: {', '.join(inactive)}")

        return [{"id": r["id"], "version": r["version"], "name": r["name"]} for r in rows]

    async def sync_mirror(self, row) -> bool:
        list_id, version = row["list_id"], row["version"]
        cursor = row["mirror_cursor"]
        updated_at = row["mirror_updated_at"]

        try:
            if cursor is None:
                await self.mirror.clear_version(list_id, version)
                updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                await self.repo.start_mirror_sync(list_id, version, updated_at)
                cursor = ""

            host = self.mirror.pick_write_host()

            async for chunk in self.repo.iter_items(list_id, version, after_value=cursor):
                token = f"{list_id}:{version}:{updated_at}:{cursor}"
                mirror_rows = await asyncio.to_thread(
                    _items_to_mirror_rows, chunk, list_id, version, updated_at
                )
                await self.mirror.insert_rows(host, mirror_rows, token)
                cursor = chunk[-1]["value"]
                await self.repo.save_mirror_cursor(list_id, version, cursor)

            ch_count = await self.mirror.count(list_id, version)
            if ch_count != row["item_count"]:
                await asyncio.sleep(MIRROR_COUNT_RETRY_DELAY)
                ch_count = await self.mirror.count(list_id, version)

            if ch_count != row["item_count"]:
                raise ValueError(
                    f"Зеркало собрано не полностью: в списке {row['item_count']}, "
                    f"в ClickHouse {ch_count}"
                )

            await self.publish_version(list_id, version)
            logger.info(
                "action=feed_list_mirror_synced id=%d version=%d rows=%d host=%s",
                list_id, version, ch_count, host,
            )
            return True

        except Exception as e:
            attempts = row["sync_attempts"] + 1
            if attempts >= MIRROR_MAX_ATTEMPTS:
                await self.fail_version(list_id, version, str(e))
                logger.error(
                    "action=feed_list_mirror_gave_up id=%d attempts=%d error=%s",
                    list_id, attempts, str(e),
                )
            else:
                delay = _mirror_backoff_minutes(attempts)
                await self.repo.schedule_mirror_retry(list_id, version, str(e), attempts, delay)
                logger.warning(
                    "action=feed_list_mirror_attempt_failed id=%d attempts=%d "
                    "retry_in_min=%d error=%s",
                    list_id, attempts, delay, str(e),
                )
            return False

    async def delete_list(self, list_id: int) -> None:
        await self.repo.mark_for_deletion(list_id, DELETION_GRACE_MINUTES)

    async def publish_version(self, list_id: int, version: int) -> None:
        card = await self.repo.get_list(list_id)
        if card is None:
            return
        prev = card["current_version"]

        blob = None
        if prev is not None and prev < version:
            async with self.repo.db.pool.acquire() as conn:
                blob = await dump_version(conn, list_id, prev)

        async with self.repo.db.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE feed_lists SET current_version = $2, last_error = NULL, "
                    "updated_at = now() WHERE id = $1",
                    list_id, version,
                )
                await conn.execute(
                    "UPDATE feed_list_versions SET status = 'ready', published_at = now(), "
                    "blob = NULL, blob_size = NULL, purge_after = NULL, "
                    "next_attempt_at = NULL, last_error = NULL "
                    "WHERE list_id = $1 AND version = $2",
                    list_id, version,
                )
                if prev is not None and prev < version:
                    await conn.execute(
                        "UPDATE feed_list_versions SET status = 'history', "
                        "blob = $3, blob_size = $4, "
                        "purge_after = now() + make_interval(mins => $5) "
                        "WHERE list_id = $1 AND version = $2",
                        list_id, prev, blob, len(blob), DELETION_GRACE_MINUTES,
                    )
                if prev is not None and prev > version:
                    await conn.execute(
                        "UPDATE feed_list_versions SET status = 'deleting', "
                        "next_attempt_at = now() + make_interval(mins => $3) "
                        "WHERE list_id = $1 AND version > $2",
                        list_id, version, DELETION_GRACE_MINUTES,
                    )
                await self._rotate_history(conn, list_id)

    async def _rotate_history(self, conn, list_id: int) -> None:
        await conn.execute(
            """
            UPDATE feed_list_versions
            SET status = 'deleting', next_attempt_at = now() + make_interval(mins => $3)
            WHERE list_id = $1 AND status = 'history'
              AND version NOT IN (
                  SELECT version FROM feed_list_versions
                  WHERE list_id = $1 AND status = 'history'
                  ORDER BY version DESC LIMIT $2
              )
            """,
            list_id, HISTORY_DEPTH, DELETION_GRACE_MINUTES,
        )

    async def fail_version(self, list_id: int, version: int, error: str) -> None:
        v = await self.repo.get_version(list_id, version)
        if v is not None and v["blob"] is not None:
            await self.repo.revert_to_history(list_id, version, DELETION_GRACE_MINUTES)
        else:
            await self.repo.mark_version_deleting(list_id, version, 0)
        await self.repo.set_list_error(list_id, error)

    async def fail_stale_versions(self) -> None:
        for row in await self.repo.get_building_versions():
            await self.fail_version(row["list_id"], row["version"], "Сборка прервана перезапуском сервиса")

    async def purge_version(self, row) -> None:
        list_id, version = row["list_id"], row["version"]
        try:
            await self.mirror.clear_version(list_id, version)
            for _ in range(DELETION_CHUNKS_PER_TICK):
                deleted = await self.repo.delete_items_chunk(list_id, version, "history", DELETION_CHUNK)
                if deleted < DELETION_CHUNK:
                    await self.repo.clear_purge_after(list_id, version)
                    return
        except Exception as e:
            logger.warning("action=feed_list_version_purge_failed id=%d version=%d error=%s",
                           list_id, version, str(e))

    async def delete_version(self, row) -> None:
        list_id, version = row["list_id"], row["version"]
        try:
            await self.mirror.clear_version(list_id, version)
            for _ in range(DELETION_CHUNKS_PER_TICK):
                deleted = await self.repo.delete_items_chunk(list_id, version, "deleting", DELETION_CHUNK)
                if deleted < DELETION_CHUNK:
                    await self.repo.delete_version_record(list_id, version)
                    return
        except Exception as e:
            logger.warning("action=feed_list_version_delete_failed id=%d version=%d error=%s",
                           list_id, version, str(e))

    async def _guard_editable(self, list_id: int) -> asyncpg.Record:
        card = await self.repo.get_list(list_id)
        if card is None:
            raise LookupError("Список не найден")
        if card["status"] == "archived":
            raise ListArchivedError("Список в архиве, верните его в активные, чтобы изменить")
        if await self.repo.find_active_build(list_id):
            raise ListBusyError("Список сейчас обновляется, дождитесь завершения")
        return card

    def _run_build(self, list_id: int, version: int, builder,
                   base_count: int = 0, empty_error: str = "Выборка пуста") -> None:
        async def runner():
            try:
                await builder()
                count = await self.repo.count_items(list_id, version)
                if count <= base_count:
                    logger.info("action=feed_list_version_skipped id=%d version=%d items=%d reason=no_new_items",
                                list_id, version, count)
                    await self.fail_version(list_id, version, empty_error)
                    return
                await self.repo.finalize_version(list_id, version, count)
            except Exception as e:
                logger.error("action=feed_list_build_failed id=%d version=%d error=%s",
                             list_id, version, str(e))
                await self.fail_version(list_id, version, str(e))

        task = asyncio.create_task(runner())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)

    async def append_to_list(self, list_id: int, source_filters: dict, builder, created_by: str) -> dict:
        card = await self._guard_editable(list_id)
        base = card["current_version"]
        if base is None:
            raise ValueError("У списка нет актуальной версии, изменение недоступно")
        base_version = await self.repo.get_version(list_id, base)

        try:
            async with self.repo.db.pool.acquire() as conn:
                async with conn.transaction():
                    new = await self.repo.next_version_number(conn, list_id)
                    await self.repo.create_version(
                        conn, list_id, new, "building", "append",
                        json.dumps(source_filters, ensure_ascii=False, default=str), created_by,
                    )
        except asyncpg.UniqueViolationError:
            raise ListBusyError("Список сейчас обновляется, дождитесь завершения")

        async def build():
            await self.repo.copy_items(list_id, base, new)
            await builder(list_id, new)

        self._run_build(list_id, new, build, base_version["item_count"], "Новых адресов нет, версия не создана")
        return self.serialize_list(await self.repo.get_catalog_row(list_id))

    async def restore_version(self, list_id: int, version: int) -> dict:
        await self._guard_editable(list_id)
        try:
            started = await self.repo.start_restore(list_id, version)
        except asyncpg.UniqueViolationError:
            raise ListBusyError("Список сейчас обновляется, дождитесь завершения")
        if started is None:
            raise LookupError("Версия не найдена")

        async def build():
            await self.repo.delete_items(list_id, version)
            async with self.repo.db.pool.acquire() as conn:
                await load_version(conn, list_id, version, started["blob"])

        self._run_build(list_id, version, build)
        return self.serialize_list(await self.repo.get_catalog_row(list_id))

    async def create_manual(self, name: str, description: str, created_by: str, values: list[str]) -> dict:
        items = await asyncio.to_thread(build_items_from_values, values)
        if not items:
            raise ValueError("Выборка пуста, список не создан")

        async with self.repo.db.pool.acquire() as conn:
            async with conn.transaction():
                row = await self.repo.create_list(
                    conn, name.strip(), description.strip(), created_by, "manual",
                )
                await self.repo.create_version(
                    conn, row["id"], 1, "building", "create",
                    json.dumps({"source": "manual", "values_count": len(items)}, ensure_ascii=False),
                    created_by,
                )

        try:
            await self.repo.insert_items(row["id"], 1, items)
            await self.repo.finalize_version(row["id"], 1, len(items))
        except Exception as e:
            await self.fail_version(row["id"], 1, str(e))
            raise

        logger.info(
            "action=feed_list_created id=%d name=%s source_type=manual items=%d created_by=%s",
            row["id"], row["name"], len(items), created_by,
        )
        return self.serialize_list(await self.repo.get_catalog_row(row["id"]))

    async def create_background(
            self,
            name: str,
            description: str,
            created_by: str,
            source_type: str,
            source_filters: dict,
            builder,
    ) -> dict:
        async with self.repo.db.pool.acquire() as conn:
            async with conn.transaction():
                row = await self.repo.create_list(
                    conn, name.strip(), description.strip(), created_by, source_type,
                )
                await self.repo.create_version(
                    conn, row["id"], 1, "building", "create",
                    json.dumps(source_filters, ensure_ascii=False, default=str), created_by,
                )
        list_id = row["id"]

        self._run_build(list_id, 1, lambda: builder(list_id, 1))
        logger.info(
            "action=feed_list_build_started id=%d name=%s source_type=%s created_by=%s",
            list_id, row["name"], source_type, created_by,
        )
        return self.serialize_list(await self.repo.get_catalog_row(list_id))

    async def build_from_ch(self,
                            list_id: int,
                            version: int,
                            ch_service,
                            filters,
                            exclude_lists: Optional[list[dict]] = None,
                            merge: bool = False) -> None:
        insert = self.repo.merge_items if merge else self.repo.insert_items
        async for rows in ch_service.iter_unique_ip_rows(filters, CHUNK_SIZE, exclude_lists):
            tuples = await asyncio.to_thread(_rows_to_tuples, rows, _blocked_row_to_item)
            await insert(list_id, version, tuples)

    async def build_from_reputation_rows(self,
                                         list_id: int,
                                         version: int,
                                         rows: list[dict],
                                         merge: bool = False) -> None:
        if not rows:
            return
        tuples = await asyncio.to_thread(_rows_to_tuples, rows, _reputation_row_to_item)
        insert = self.repo.merge_items if merge else self.repo.insert_items
        await insert(list_id, version, tuples)

    async def build_from_reputation_snapshot(self,
                                             list_id: int,
                                             version: int,
                                             reputation_service,
                                             filters,
                                             exclude_lists: Optional[list[dict]] = None,
                                             merge: bool = False) -> None:
        async for rows in reputation_service.iter_snapshot_rows(
                filters, CHUNK_SIZE, exclude_lists=exclude_lists):
            await self.build_from_reputation_rows(list_id, version, rows, merge=merge)

    async def build_search(self,
                           owner: str,
                           kind: str,
                           filters,
                           row_source) -> dict:
        search_id = uuid.uuid4().hex
        await self.repo.evict_owner_sessions(owner, SEARCH_SESSIONS_PER_USER - 1)
        await self.repo.create_search_session(
            search_id, owner, kind,
            json.dumps(filters.model_dump(exclude_none=True), ensure_ascii=False, default=str),
            SEARCH_TTL_MINUTES,
        )

        seq = 0
        async with aclosing(row_source) as rows_stream:
            async for rows in rows_stream:
                seq += len(rows)
                if seq > MAX_SOURCE_ROWS:
                    raise ValueError(f"Выборка превысила лимит {MAX_SOURCE_ROWS} записей, уточните фильтры")

                payload = await asyncio.to_thread(_rows_to_json, rows)
                await self.repo.add_search_rows(search_id, seq - len(rows), payload)

        await self.repo.finish_search_session(search_id, seq)
        logger.info("action=search_built search_id=%s kind=%s total=%d owner=%s", search_id, kind, seq, owner)
        return {"search_id": search_id, "total": seq}

    async def build_ch_search(self,
                              owner: str,
                              ch_service,
                              filters,
                              exclude_lists: Optional[list[dict]],
                              kind: str) -> dict:
        if kind == "export":
            rows = ch_service.iter_export_rows(filters, CHUNK_SIZE, exclude_lists)
        else:
            rows = ch_service.iter_read_rows(filters, CHUNK_SIZE, exclude_lists)
        return await self.build_search(owner, kind, filters, rows)

    async def build_reputation_search(self,
                                      owner: str,
                                      reputation_service,
                                      filters,
                                      exclude_lists: Optional[list[dict]]) -> dict:
        rows = reputation_service.iter_snapshot_rows(filters, CHUNK_SIZE, exclude_lists=exclude_lists)
        return await self.build_search(owner, "reputation", filters, rows)

    async def build_from_session(self, list_id: int, version: int, owner: str, search_id: str, row_to_item) -> None:
        session = await self.repo.get_search_session(search_id, owner, SEARCH_TTL_MINUTES)
        if session is None:
            raise ValueError("Результат поиска устарел, список не создан")

        total = session["total"]
        seq = 0
        while seq < total:
            rows = await self.repo.get_search_rows(search_id, seq, CHUNK_SIZE)
            if not rows:
                raise ValueError("Результат поиска устарел, список не создан")

            tuples = await asyncio.to_thread(
                _rows_to_tuples, rows, lambda raw: row_to_item(json.loads(raw))
            )
            await self.repo.insert_items(list_id, version, tuples)
            seq += len(rows)

    async def build_from_reputation_session(self, list_id: int, version: int, owner: str, search_id: str) -> None:
        await self.build_from_session(list_id, version, owner, search_id, _reputation_row_to_item)

    async def get_search_total(self, owner: str, search_id: str) -> Optional[int]:
        session = await self.repo.get_search_session(search_id, owner, SEARCH_TTL_MINUTES)
        return None if session is None else session["total"]

    async def get_search_page(self, owner: str, search_id: str, page: int, page_size: int) -> Optional[dict]:
        session = await self.repo.get_search_session(search_id, owner, SEARCH_TTL_MINUTES)
        if session is None:
            return None
        rows = await self.repo.get_search_rows(search_id, (page - 1) * page_size, page_size)
        return {
            "data": [json.loads(r) for r in rows],
            "total": session["total"],
            "search_id": search_id,
        }

    async def iter_search_rows(self, owner: str, search_id: str, chunk_size: int = CHUNK_SIZE):
        session = await self.repo.get_search_session(search_id, owner, SEARCH_TTL_MINUTES)
        if session is None:
            return
        seq = 0
        total = session["total"]
        while seq < total:
            rows = await self.repo.get_search_rows(search_id, seq, chunk_size)
            if not rows:
                return
            yield rows
            seq += len(rows)

    @staticmethod
    def serialize_list(row) -> dict:
        data = dict(row)
        pending_version = data.pop("pending_version", None)
        pending_status = data.pop("pending_status", None)
        data["busy"] = pending_version is not None
        data["pending"] = (
            {"version": pending_version, "status": pending_status}
            if pending_version is not None else None
        )
        if isinstance(data.get("source_filters"), str):
            try:
                data["source_filters"] = json.loads(data["source_filters"])
            except (ValueError, TypeError):
                pass
        return data


async def search_cleanup_loop(repo: FeedListRepository, interval: int = CLEANUP_INTERVAL) -> None:
    while True:
        try:
            await asyncio.sleep(interval)
            if repo.is_connected:
                await repo.cleanup_search_sessions()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=search_cleanup_error error=%s", str(e))


async def mirror_sync_loop(service: "FeedListService", interval: int = MIRROR_SYNC_INTERVAL) -> None:
    while True:
        try:
            await asyncio.sleep(interval)
            if not service.is_available:
                continue

            for row in await service.repo.get_versions_to_sync(MIRROR_SYNC_BATCH):
                await service.sync_mirror(row)

            for row in await service.repo.get_versions_to_purge(MIRROR_SYNC_BATCH):
                await service.purge_version(row)

            for row in await service.repo.get_versions_to_delete(MIRROR_SYNC_BATCH):
                await service.delete_version(row)

            await service.repo.delete_empty_deleting_lists()

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=mirror_sync_loop_error error=%s", str(e))
