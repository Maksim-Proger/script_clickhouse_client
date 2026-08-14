import asyncio
import logging
import uuid
from datetime import datetime, timezone, timedelta
from typing import Optional

import asyncpg

logger = logging.getLogger("ch-api-gateway")

CREATE_USERS_TABLE = """
CREATE TABLE IF NOT EXISTS users (
    id          SERIAL PRIMARY KEY,
    username    VARCHAR(150) NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
"""

CREATE_SESSIONS_TABLE = """
CREATE TABLE IF NOT EXISTS sessions (
    id          SERIAL PRIMARY KEY,
    jti         UUID NOT NULL UNIQUE,
    username    VARCHAR(150) NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at  TIMESTAMPTZ NOT NULL,
    revoked_at  TIMESTAMPTZ
);
"""

CREATE_SESSIONS_JTI_INDEX = """
CREATE INDEX IF NOT EXISTS idx_sessions_jti ON sessions (jti);
"""

CREATE_SESSIONS_USERNAME_INDEX = """
CREATE INDEX IF NOT EXISTS idx_sessions_username ON sessions (username);
"""

CREATE_PROFILE_STATES_TABLE = """
CREATE TABLE IF NOT EXISTS profile_states (
    profile         VARCHAR(150) PRIMARY KEY,
    status          VARCHAR(20)  NOT NULL DEFAULT 'success',
    last_success_at TIMESTAMPTZ  NOT NULL DEFAULT now(),
    claim_until     TIMESTAMPTZ,
    claim_owner     VARCHAR(64),
    last_error      TEXT
);
"""

CREATE_PROFILE_STATES_INDEX = """
CREATE INDEX IF NOT EXISTS idx_profile_states_profile ON profile_states (profile);
"""

CREATE_FEED_LISTS_TABLE = """
CREATE TABLE IF NOT EXISTS feed_lists (
    id             SERIAL PRIMARY KEY,
    name           VARCHAR(200) NOT NULL UNIQUE,
    description    TEXT NOT NULL DEFAULT '',
    created_by     VARCHAR(150) NOT NULL,
    created_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    status         VARCHAR(20) NOT NULL DEFAULT 'creating',
    source_type    VARCHAR(30) NOT NULL,
    source_filters JSONB NOT NULL DEFAULT '{}'::jsonb,
    version        INT NOT NULL DEFAULT 1,
    item_count     BIGINT NOT NULL DEFAULT 0,
    last_error     TEXT
);
"""

CREATE_FEED_LIST_ITEMS_TABLE = """
CREATE TABLE IF NOT EXISTS feed_list_items (
    list_id     INT NOT NULL REFERENCES feed_lists(id) ON DELETE CASCADE,
    version     INT NOT NULL,
    value       VARCHAR(64) NOT NULL,
    value_type  VARCHAR(10) NOT NULL DEFAULT 'ip',
    value_net   INET NOT NULL,
    score       REAL,
    risk_level  VARCHAR(20),
    asn         BIGINT,
    country     VARCHAR(8),
    source      VARCHAR(150),
    first_seen  TIMESTAMPTZ,
    last_seen   TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (list_id, version, value)
);
"""

CREATE_FEED_LIST_ITEMS_INDEX = """
CREATE INDEX IF NOT EXISTS idx_feed_list_items_list_version ON feed_list_items (list_id, version);
"""

CREATE_FEED_LISTS_SYNC_INDEX = """
CREATE INDEX IF NOT EXISTS idx_feed_lists_pending_sync
    ON feed_lists (next_attempt_at)
    WHERE status = 'pending_sync';
"""

CREATE_FEED_LIST_ITEMS_NET_INDEX = """
CREATE INDEX IF NOT EXISTS idx_feed_list_items_net ON feed_list_items USING gist (value_net inet_ops);
"""

CREATE_SEARCH_SESSIONS_TABLE = """
CREATE UNLOGGED TABLE IF NOT EXISTS search_sessions (
    search_id  VARCHAR(64) PRIMARY KEY,
    owner      VARCHAR(150) NOT NULL,
    kind       VARCHAR(20) NOT NULL,
    filters    JSONB NOT NULL DEFAULT '{}'::jsonb,
    total      BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at TIMESTAMPTZ NOT NULL
);
"""

CREATE_SEARCH_SESSION_ROWS_TABLE = """
CREATE UNLOGGED TABLE IF NOT EXISTS search_session_rows (
    search_id VARCHAR(64) NOT NULL REFERENCES search_sessions(search_id) ON DELETE CASCADE,
    seq       BIGINT NOT NULL,
    row       TEXT NOT NULL,
    PRIMARY KEY (search_id, seq)
);
"""

UPGRADE_SEARCH_TABLES = [
    "ALTER TABLE search_sessions SET UNLOGGED",
    "ALTER TABLE search_session_rows SET UNLOGGED",
]

UPGRADE_FEED_TABLES = [
    """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'feed_lists'
                     AND column_name = 'item_count' AND data_type = 'integer') THEN
            ALTER TABLE feed_lists ALTER COLUMN item_count TYPE BIGINT;
        END IF;
    END $$;
    """,
    "ALTER TABLE feed_lists ADD COLUMN IF NOT EXISTS last_error TEXT",
    "ALTER TABLE feed_lists ALTER COLUMN status SET DEFAULT 'creating'",
    "ALTER TABLE feed_list_items ADD COLUMN IF NOT EXISTS value_net INET",
    "UPDATE feed_list_items SET value_net = value::inet WHERE value_net IS NULL",
    "ALTER TABLE feed_lists ADD COLUMN IF NOT EXISTS mirror_cursor VARCHAR(64)",
    "ALTER TABLE feed_lists ADD COLUMN IF NOT EXISTS mirror_updated_at TIMESTAMP",
    "ALTER TABLE feed_lists ADD COLUMN IF NOT EXISTS sync_attempts INT NOT NULL DEFAULT 0",
    "ALTER TABLE feed_lists ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ",
    """
    DO $$
    BEGIN
        IF EXISTS (SELECT 1 FROM information_schema.columns
                   WHERE table_name = 'feed_list_items'
                     AND column_name = 'value_net' AND is_nullable = 'YES') THEN
            ALTER TABLE feed_list_items ALTER COLUMN value_net SET NOT NULL;
        END IF;
    END $$;
    """,
]


class DatabaseManager:

    def __init__(self, dsn: str, min_size: int = 2, max_size: int = 10):
        self.dsn = dsn
        self.min_size = min_size
        self.max_size = max_size
        self.pool: Optional[asyncpg.Pool] = None
        self._reconnect_task: Optional[asyncio.Task] = None
        self._on_connect_callback = None

    @property
    def is_connected(self) -> bool:
        return self.pool is not None

    async def connect(self):
        self.pool = await asyncpg.create_pool(
            dsn=self.dsn,
            min_size=self.min_size,
            max_size=self.max_size,
        )
        await self._ensure_tables()
        logger.info("action=db_connect status=success")

    async def connect_safe(self):
        try:
            await self.connect()
            return True
        except Exception as e:
            logger.warning("action=db_connect status=failed error=%s", str(e))
            self.pool = None
            return False

    def start_reconnect_loop(self, on_connect=None):
        self._on_connect_callback = on_connect
        self._reconnect_task = asyncio.create_task(self._reconnect_loop())

    def stop_reconnect_loop(self):
        if self._reconnect_task:
            self._reconnect_task.cancel()

    async def _reconnect_loop(self):
        while True:
            try:
                await asyncio.sleep(5)
                if self.pool is None:
                    connected = await self.connect_safe()
                    if connected:
                        logger.info("action=db_reconnect status=success")
                        if self._on_connect_callback:
                            await self._on_connect_callback()
                        return
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("action=db_reconnect status=failed error=%s", str(e))

    async def close(self):
        self.stop_reconnect_loop()
        if self.pool:
            await self.pool.close()
            self.pool = None
            logger.info("action=db_disconnect status=success")

    async def _ensure_tables(self):
        async with self.pool.acquire() as conn:
            await conn.execute(CREATE_USERS_TABLE)
            await conn.execute(CREATE_SESSIONS_TABLE)
            await conn.execute(CREATE_SESSIONS_JTI_INDEX)
            await conn.execute(CREATE_SESSIONS_USERNAME_INDEX)
            await conn.execute(CREATE_PROFILE_STATES_TABLE)
            await conn.execute(CREATE_PROFILE_STATES_INDEX)
            await conn.execute(CREATE_FEED_LISTS_TABLE)
            await conn.execute(CREATE_FEED_LIST_ITEMS_TABLE)
            for statement in UPGRADE_FEED_TABLES:
                await conn.execute(statement)
            await conn.execute(CREATE_FEED_LIST_ITEMS_INDEX)
            await conn.execute(CREATE_FEED_LIST_ITEMS_NET_INDEX)
            await conn.execute(CREATE_SEARCH_SESSIONS_TABLE)
            await conn.execute(CREATE_SEARCH_SESSION_ROWS_TABLE)
            await conn.execute(CREATE_FEED_LISTS_SYNC_INDEX)
            for statement in UPGRADE_SEARCH_TABLES:
                await conn.execute(statement)

    async def get_user_by_username(self, username: str) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "SELECT id, username, password_hash, is_active, created_at "
                "FROM users WHERE username = $1",
                username,
            )

    async def get_all_users(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT id, username, is_active, created_at "
                "FROM users ORDER BY id"
            )

    async def count_users(self) -> int:
        async with self.pool.acquire() as conn:
            return await conn.fetchval("SELECT count(*) FROM users")

    async def insert_user(self, username: str, password_hash: str) -> asyncpg.Record:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                "INSERT INTO users (username, password_hash) "
                "VALUES ($1, $2) RETURNING id, username, is_active, created_at",
                username, password_hash,
            )

    async def insert_user_ignore(self, username: str, password_hash: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO users (username, password_hash) "
                "VALUES ($1, $2) ON CONFLICT (username) DO NOTHING",
                username, password_hash,
            )

    async def update_password(self, username: str, password_hash: str):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET password_hash = $1 WHERE username = $2",
                password_hash, username,
            )

    async def deactivate_user(self, username: str) -> list:
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute(
                    "UPDATE users SET is_active = FALSE WHERE username = $1",
                    username,
                )
                rows = await conn.fetch(
                    "UPDATE sessions SET revoked_at = $1 "
                    "WHERE username = $2 AND revoked_at IS NULL "
                    "RETURNING jti",
                    now, username,
                )
                return [r["jti"] for r in rows]

    async def insert_session(self, username: str, expires_at: datetime) -> str:
        jti = str(uuid.uuid4())
        async with self.pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO sessions (jti, username, expires_at) "
                "VALUES ($1, $2, $3)",
                uuid.UUID(jti), username, expires_at,
            )
        return jti

    async def revoke_session(self, jti: str):
        now = datetime.now(timezone.utc)
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE sessions SET revoked_at = $1 WHERE jti = $2",
                now, uuid.UUID(jti),
            )

    async def get_active_revoked_sessions(self) -> list[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetch(
                "SELECT jti FROM sessions "
                "WHERE revoked_at IS NOT NULL AND expires_at > now()"
            )

    async def delete_expired_sessions(self) -> int:
        async with self.pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM sessions WHERE expires_at < now()"
            )
            count = int(result.split()[-1])
            if count > 0:
                logger.info("action=cleanup_sessions deleted=%d", count)
            return count

    async def try_claim_dg_fetch(self, profile: str, owner_id: str) -> bool:
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow(
                    """
                    SELECT status, last_success_at, claim_until
                    FROM profile_states
                    WHERE profile = $1
                    FOR UPDATE
                    """,
                    profile,
                )

                if row is None:
                    result = await conn.fetchrow(
                        """
                        INSERT INTO profile_states
                            (profile, status, last_success_at, claim_until, claim_owner)
                        VALUES ($1, 'in_progress', now(),
                                now() + interval '340 seconds', $2)
                        ON CONFLICT (profile) DO NOTHING
                        RETURNING profile
                        """,
                        profile, owner_id,
                    )
                    if result is not None:
                        return True
                    return False

                now = datetime.now(timezone.utc)
                if (
                        row["status"] == "in_progress"
                        and row["claim_until"] is not None
                        and row["claim_until"] > now
                ):
                    return False

                if (
                        row["status"] == "success"
                        and row["last_success_at"] is not None
                        and row["last_success_at"] > now - timedelta(minutes=5)
                ):
                    return False

                await conn.execute(
                    """
                    UPDATE profile_states
                    SET status      = 'in_progress',
                        claim_until = now() + interval '340 seconds',
                        claim_owner = $2
                    WHERE profile = $1
                    """,
                    profile, owner_id,
                )
                return True

    async def get_profile_status(self, profile: str) -> Optional[asyncpg.Record]:
        async with self.pool.acquire() as conn:
            return await conn.fetchrow(
                """
                SELECT status, last_success_at, claim_until
                FROM profile_states
                WHERE profile = $1
                """,
                profile,
            )

    async def release_dg_claim(
            self,
            profile: str,
            owner_id: str,
            success: bool,
            error: Optional[str] = None,
    ) -> None:
        async with self.pool.acquire() as conn:
            if success:
                await conn.execute(
                    """
                    UPDATE profile_states
                    SET status          = 'success',
                        last_success_at = now(),
                        claim_until     = NULL,
                        claim_owner     = NULL,
                        last_error      = NULL
                    WHERE profile = $1 AND claim_owner = $2
                    """,
                    profile, owner_id,
                )
            else:
                await conn.execute(
                    """
                    UPDATE profile_states
                    SET status      = 'error',
                        claim_until = NULL,
                        claim_owner = NULL,
                        last_error  = $3
                    WHERE profile = $1 AND claim_owner = $2
                    """,
                    profile, owner_id, error,
                )
