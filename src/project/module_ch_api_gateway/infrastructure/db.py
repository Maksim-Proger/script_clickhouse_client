import logging
import asyncio
import uuid
from datetime import datetime, timezone
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
