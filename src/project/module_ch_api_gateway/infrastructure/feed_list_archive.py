import gzip
import io

_ARCHIVE_COLUMNS = (
    "value", "value_type", "score", "risk_level",
    "asn", "country", "source", "first_seen", "last_seen",
)


async def dump_version(conn, list_id: int, version: int) -> bytes:
    buf = io.BytesIO()
    await conn.copy_from_query(
        f"SELECT {', '.join(_ARCHIVE_COLUMNS)} FROM feed_list_items "
        f"WHERE list_id = $1 AND version = $2 ORDER BY value",
        list_id, version, output=buf, format="csv",
    )
    return gzip.compress(buf.getvalue())


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

