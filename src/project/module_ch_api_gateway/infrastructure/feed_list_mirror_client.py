import asyncio
import logging
import random

from clickhouse_driver import Client as CHClient

logger = logging.getLogger("ch-api-gateway.feed_list_mirror")

_INSERT_SQL = """
              INSERT INTO feedgen.feed_list_mirror_data
              (list_id, version, value, value_type, range_start, range_end, updated_at)
              VALUES \
              """

_COUNT_SQL = (
    "SELECT count() FROM feedgen.feed_list_mirror "
    "WHERE list_id = %(list_id)s AND version = %(version)s"
)

_DROP_PARTITION_SQL = (
    "ALTER TABLE feedgen.feed_list_mirror_data "
    "DROP PARTITION (%(list_id)s, %(version)s)"
)

_DELETE_LIST_SQL = (
    "ALTER TABLE feedgen.feed_list_mirror_data DELETE WHERE list_id = %(list_id)s"
)


class FeedListMirrorClient:
    def __init__(self, cfg: dict):
        self._cfg = cfg

    def pick_write_host(self) -> str:
        return random.choice(self._cfg["write_hosts"])

    def _client(self, host: str) -> CHClient:
        return CHClient(
            host=host,
            port=self._cfg["port"],
            database=self._cfg.get("database", "feedgen"),
            user=self._cfg["user"],
            password=self._cfg["password"],
        )

    def _execute_sync(self, host: str, sql: str, params=None):
        client = self._client(host)
        try:
            return client.execute(sql, params)
        finally:
            client.disconnect()

    def _insert_sync(self, host: str, rows: list[tuple], token: str) -> None:
        client = self._client(host)
        try:
            client.execute(
                _INSERT_SQL, rows,
                settings={"insert_deduplication_token": token},
            )
        finally:
            client.disconnect()

    async def insert_rows(self, host: str, rows: list[tuple], token: str) -> None:
        if not rows:
            return
        await asyncio.to_thread(self._insert_sync, host, rows, token)

    async def count(self, list_id: int, version: int) -> int:
        result = await asyncio.to_thread(
            self._execute_sync,
            self._cfg["host"],
            _COUNT_SQL,
            {"list_id": list_id, "version": version},
        )
        return int(result[0][0]) if result else 0

    async def clear_version(self, list_id: int, version: int) -> None:
        for host in self._cfg["write_hosts"]:
            await asyncio.to_thread(
                self._execute_sync,
                host,
                _DROP_PARTITION_SQL,
                {"list_id": list_id, "version": version},
            )

    async def delete_list(self, list_id: int) -> None:
        for host in self._cfg["write_hosts"]:
            await asyncio.to_thread(
                self._execute_sync, host, _DELETE_LIST_SQL, {"list_id": list_id}
            )



