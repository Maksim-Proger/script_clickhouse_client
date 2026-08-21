import asyncio
import ipaddress
import logging
from itertools import islice
from typing import AsyncIterator

from clickhouse_driver import Client as CHClient

logger = logging.getLogger("ch-api-gateway.ch_stream")

_IP_TYPES = (ipaddress.IPv4Address, ipaddress.IPv6Address)


def _normalize(value):
    if isinstance(value, _IP_TYPES):
        return str(value)
    return value


def _take_header(gen) -> list[str]:
    header = list(islice(gen, 1))
    return [col[0] for col in header[0]] if header else []


def _take_chunk(gen, size: int, columns: list[str]) -> list[dict]:
    return [
        {col: _normalize(val) for col, val in zip(columns, row)}
        for row in islice(gen, size)
    ]


class ClickHouseStreamClient:
    def __init__(self, host: str, port: int, database: str, user: str, password: str, timeout_sec: int):
        self._cfg = {
            "host": host,
            "port": port,
            "database": database,
            "user": user,
            "password": password,
        }
        self._timeout_sec = timeout_sec

    def _connect(self) -> CHClient:
        return CHClient(**self._cfg, send_receive_timeout=self._timeout_sec)

    async def iter_rows(self, query: str, chunk_size: int) -> AsyncIterator[list[dict]]:
        client = await asyncio.to_thread(self._connect)
        rows_total = 0
        try:
            gen = await asyncio.to_thread(
                lambda: client.execute_iter(query, with_column_types=True)
            )
            columns = await asyncio.to_thread(_take_header, gen)
            if not columns:
                logger.info("action=ch_stream_done rows=0 query=\"%s\"", query[:100])
                return

            while True:
                chunk = await asyncio.to_thread(_take_chunk, gen, chunk_size, columns)
                if not chunk:
                    break
                rows_total += len(chunk)
                yield chunk

            logger.info("action=ch_stream_done rows=%d query=\"%s\"", rows_total, query[:100])
        except Exception as e:
            logger.error(
                "action=ch_stream_failed rows_read=%d error=%s query=\"%s\"",
                rows_total, str(e), query[:100],
            )
            raise
        finally:
            client.disconnect()


