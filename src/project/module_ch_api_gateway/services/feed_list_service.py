import asyncio
import ipaddress
import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

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

_DT_FORMATS = ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d")

_ITEM_FIELDS = (
    "value", "value_type", "value_net", "score", "risk_level",
    "asn", "country", "source", "first_seen", "last_seen",
)


class SessionExpiredError(Exception):
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

        inactive = [r["name"] for r in rows if r["status"] != "active"]
        if inactive:
            raise ValueError(f"Списки не активны и не могут применяться как исключения: {', '.join(inactive)}")

        return [{"id": r["id"], "version": r["version"], "name": r["name"]} for r in rows]

    async def sync_mirror(self, row) -> bool:
        list_id, version = row["id"], row["version"]
        cursor = row["mirror_cursor"]
        updated_at = row["mirror_updated_at"]

        try:
            if cursor is None:
                await self.mirror.clear_version(list_id, version)
                updated_at = datetime.now(timezone.utc).replace(tzinfo=None)
                await self.repo.start_mirror_sync(list_id, updated_at)
                cursor = ""

            host = self.mirror.pick_write_host()

            async for chunk in self.repo.iter_items(list_id, version, after_value=cursor):
                token = f"{list_id}:{version}:{cursor}"
                mirror_rows = await asyncio.to_thread(
                    _items_to_mirror_rows, chunk, list_id, version, updated_at
                )
                await self.mirror.insert_rows(host, mirror_rows, token)
                cursor = chunk[-1]["value"]
                await self.repo.save_mirror_cursor(list_id, cursor)

            ch_count = await self.mirror.count(list_id, version)
            if ch_count != row["item_count"]:
                await asyncio.sleep(MIRROR_COUNT_RETRY_DELAY)
                ch_count = await self.mirror.count(list_id, version)

            if ch_count != row["item_count"]:
                raise ValueError(
                    f"Зеркало собрано не полностью: в списке {row['item_count']}, "
                    f"в ClickHouse {ch_count}"
                )

            await self.repo.activate_list(list_id)
            logger.info(
                "action=feed_list_mirror_synced id=%d version=%d rows=%d host=%s",
                list_id, version, ch_count, host,
            )
            return True

        except Exception as e:
            attempts = row["sync_attempts"] + 1
            if attempts >= MIRROR_MAX_ATTEMPTS:
                await self.repo.mark_sync_failed(list_id, str(e), attempts)
                logger.error(
                    "action=feed_list_mirror_gave_up id=%d attempts=%d error=%s",
                    list_id, attempts, str(e),
                )
            else:
                delay = _mirror_backoff_minutes(attempts)
                await self.repo.schedule_mirror_retry(list_id, str(e), attempts, delay)
                logger.warning(
                    "action=feed_list_mirror_attempt_failed id=%d attempts=%d "
                    "retry_in_min=%d error=%s",
                    list_id, attempts, delay, str(e),
                )
            return False

    async def process_deletion(self, row) -> None:
        list_id, version = row["id"], row["version"]
        try:
            await self.mirror.clear_version(list_id, version)

            for _ in range(DELETION_CHUNKS_PER_TICK):
                deleted = await self.repo.delete_items_chunk(list_id, DELETION_CHUNK)
                if deleted < DELETION_CHUNK:
                    await self.repo.purge_list(list_id)
                    logger.info("action=feed_list_purged id=%d version=%d", list_id, version)
                    return

            await self.repo.continue_deletion(list_id)
            logger.info("action=feed_list_purge_continue id=%d", list_id)

        except Exception as e:
            attempts = row["sync_attempts"] + 1
            delay = _mirror_backoff_minutes(attempts)
            await self.repo.schedule_mirror_retry(list_id, str(e), attempts, delay)
            logger.warning(
                "action=feed_list_purge_failed id=%d attempts=%d retry_in_min=%d error=%s",
                list_id, attempts, delay, str(e),
            )

    async def delete_list(self, list_id: int) -> None:
        await self.repo.mark_for_deletion(list_id, DELETION_GRACE_MINUTES)

    async def create_manual(self, name: str, description: str, created_by: str, values: list[str]) -> dict:
        items = build_items_from_values(values)
        if not items:
            raise ValueError("Выборка пуста, список не создан")

        row = await self.repo.create_list(
            name=name.strip(),
            description=description.strip(),
            created_by=created_by,
            source_type="manual",
            source_filters=json.dumps({"values_count": len(items)}, ensure_ascii=False),
            status="creating",
        )
        try:
            await self.repo.insert_items(row["id"], row["version"], items)
            finalized = await self.repo.finalize_list(row["id"])

        except Exception as e:
            await self.repo.fail_list(row["id"], str(e))
            try:
                await self.repo.delete_items(row["id"], row["version"])
            except Exception as cleanup_err:
                logger.error(
                    "action=feed_list_items_cleanup_failed id=%d error=%s", row["id"], str(cleanup_err)
                )
            raise

        logger.info(
            "action=feed_list_created id=%d name=%s source_type=manual items=%d created_by=%s",
            row["id"], row["name"], len(items), created_by,
        )
        return self.serialize_list(finalized)

    async def create_background(
            self,
            name: str,
            description: str,
            created_by: str,
            source_type: str,
            source_filters: dict,
            builder,
    ) -> dict:
        row = await self.repo.create_list(
            name=name.strip(),
            description=description.strip(),
            created_by=created_by,
            source_type=source_type,
            source_filters=json.dumps(source_filters, ensure_ascii=False, default=str),
            status="creating",
        )
        list_id, version = row["id"], row["version"]

        async def runner():
            try:
                await builder(list_id, version)
                finalized = await self.repo.finalize_list(list_id)
                logger.info(
                    "action=feed_list_build_done id=%d items=%d",
                    list_id, finalized["item_count"] if finalized else -1,
                )

            except Exception as e:
                logger.error("action=feed_list_build_failed id=%d error=%s", list_id, str(e))
                try:
                    await self.repo.fail_list(list_id, str(e))
                except Exception as db_err:
                    logger.error("action=feed_list_fail_mark_error id=%d error=%s", list_id, str(db_err))
                try:
                    deleted = await self.repo.delete_items(list_id, version)
                    if deleted:
                        logger.info("action=feed_list_items_cleanup id=%d deleted=%d", list_id, deleted)
                except Exception as cleanup_err:
                    logger.error(
                        "action=feed_list_items_cleanup_failed id=%d error=%s", list_id, str(cleanup_err)
                    )

        asyncio.create_task(runner())
        logger.info(
            "action=feed_list_build_started id=%d name=%s source_type=%s created_by=%s",
            list_id, row["name"], source_type, created_by,
        )
        return self.serialize_list(row)

    async def build_from_ch(self,
                            list_id: int,
                            version: int,
                            ch_service,
                            filters,
                            exclude_lists: Optional[list[dict]] = None) -> None:
        after_ip = ""

        while True:
            rows = await ch_service.fetch_unique_ip_chunk(
                filters, CHUNK_SIZE, after_ip, exclude_lists,
            )
            if not rows:
                break

            fetched = len(rows)
            after_ip = rows[-1]["ip_address"]

            tuples = await asyncio.to_thread(_rows_to_tuples, rows, _blocked_row_to_item)
            await self.repo.insert_items(list_id, version, tuples)

            if fetched < CHUNK_SIZE:
                break

    async def build_from_reputation_rows(self,
                                         list_id: int,
                                         version: int,
                                         rows: list[dict]) -> None:
        if not rows:
            return
        tuples = await asyncio.to_thread(_rows_to_tuples, rows, _reputation_row_to_item)
        await self.repo.insert_items(list_id, version, tuples)

    async def build_from_reputation_snapshot(self,
                                             list_id: int,
                                             version: int,
                                             reputation_service,
                                             filters,
                                             exclude_lists: Optional[list[dict]] = None) -> None:
        offset = 0
        while True:
            rows, fetched = await reputation_service.fetch_snapshot_chunk(
                filters, CHUNK_SIZE, offset, exclude_lists=exclude_lists,
            )
            if fetched == 0:
                break
            await self.build_from_reputation_rows(list_id, version, rows)
            if fetched < CHUNK_SIZE:
                break
            offset += CHUNK_SIZE

    async def build_search(self,
                           owner: str,
                           kind: str,
                           filters,
                           fetch_chunk) -> dict:
        search_id = uuid.uuid4().hex
        await self.repo.evict_owner_sessions(owner, SEARCH_SESSIONS_PER_USER - 1)
        await self.repo.create_search_session(
            search_id, owner, kind,
            json.dumps(filters.model_dump(exclude_none=True), ensure_ascii=False, default=str),
            SEARCH_TTL_MINUTES,
        )

        seq = 0
        offset = 0
        while True:
            rows, fetched = await fetch_chunk(CHUNK_SIZE, offset)
            if fetched == 0:
                break

            seq += len(rows)
            if seq > MAX_SOURCE_ROWS:
                raise ValueError(f"Выборка превысила лимит {MAX_SOURCE_ROWS} записей, уточните фильтры")

            payload = await asyncio.to_thread(_rows_to_json, rows)
            await self.repo.add_search_rows(search_id, seq - len(rows), payload)

            if fetched < CHUNK_SIZE:
                break

            offset += CHUNK_SIZE

        await self.repo.finish_search_session(search_id, seq)
        logger.info("action=search_built search_id=%s kind=%s total=%d owner=%s", search_id, kind, seq, owner)
        return {"search_id": search_id, "total": seq}

    async def build_ch_search(self,
                              owner: str,
                              ch_service,
                              filters,
                              exclude_lists: Optional[list[dict]],
                              kind: str) -> dict:
        async def fetch_chunk(limit, offset):
            if kind == "export":
                rows = await ch_service.fetch_export_chunk(filters, limit, offset, exclude_lists)
            else:
                rows = await ch_service.fetch_read_chunk(filters, limit, offset, exclude_lists)
            return rows, len(rows)

        return await self.build_search(owner, kind, filters, fetch_chunk)

    async def build_reputation_search(self,
                                      owner: str,
                                      reputation_service,
                                      filters,
                                      exclude_lists: Optional[list[dict]]) -> dict:
        async def fetch_chunk(limit, offset):
            return await reputation_service.fetch_snapshot_chunk(
                filters, limit, offset, exclude_lists=exclude_lists,
            )

        return await self.build_search(owner, "reputation", filters, fetch_chunk)

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

            rows = await service.repo.get_lists_for_mirror_sync(MIRROR_SYNC_BATCH)
            for row in rows:
                await service.sync_mirror(row)

            deletions = await service.repo.get_lists_for_deletion(MIRROR_SYNC_BATCH)
            for row in deletions:
                await service.process_deletion(row)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=mirror_sync_loop_error error=%s", str(e))
