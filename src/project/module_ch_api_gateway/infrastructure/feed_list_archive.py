import asyncio
import csv
import gzip
import io

_ARCHIVE_COLUMNS = (
    "value", "value_type", "score", "risk_level",
    "asn", "country", "source", "first_seen", "last_seen",
)
_DUMP_CHUNK = 50_000


def _write_rows(gz, rows) -> None:
    text = io.StringIO()
    csv.writer(text, lineterminator="\n").writerows(rows)
    gz.write(text.getvalue().encode("utf-8"))


async def dump_version(conn, list_id: int, version: int) -> bytes:
    raw = io.BytesIO()
    last_value = ""
    with gzip.GzipFile(fileobj=raw, mode="wb") as gz:
        while True:
            rows = await conn.fetch(
                f"SELECT {', '.join(_ARCHIVE_COLUMNS)} FROM feed_list_items "
                f"WHERE list_id = $1 AND version = $2 AND value > $3 "
                f"ORDER BY value LIMIT $4",
                list_id, version, last_value, _DUMP_CHUNK,
            )
            if not rows:
                break
            await asyncio.to_thread(_write_rows, gz, rows)
            last_value = rows[-1]["value"]
    return raw.getvalue()


async def load_version(conn, list_id: int, version: int, blob: bytes) -> None:
    async with conn.transaction():
        await conn.execute(
            """
            CREATE TEMP TABLE staging_archive (
                value      VARCHAR(64),
                value_type VARCHAR(10),
                score      REAL,
                risk_level VARCHAR(20),
                asn        BIGINT,
                country    VARCHAR(8),
                source     VARCHAR(150),
                first_seen TIMESTAMPTZ,
                last_seen  TIMESTAMPTZ
            ) ON COMMIT DROP
            """
        )
        await conn.copy_to_table(
            "staging_archive",
            source=io.BytesIO(gzip.decompress(blob)),
            columns=list(_ARCHIVE_COLUMNS),
            format="csv",
        )
        await conn.execute(
            """
            INSERT INTO feed_list_items
                (list_id, version, value, value_type, value_net, score,
                 risk_level, asn, country, source, first_seen, last_seen)
            SELECT $1, $2, value, value_type, value::inet, score,
                   risk_level, asn, country, source, first_seen, last_seen
            FROM staging_archive
            """,
            list_id, version,
        )

