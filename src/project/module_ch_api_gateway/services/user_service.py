import logging
import asyncio
from datetime import datetime, timezone
from typing import Optional

from passlib.context import CryptContext

from project.module_ch_api_gateway.infrastructure.db import DatabaseManager

logger = logging.getLogger("ch-api-gateway")

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")


class UserService:

    def __init__(self, db: DatabaseManager):
        self.db = db
        self._revoked_jtis: set[str] = set()
        self._cleanup_task: Optional[asyncio.Task] = None

    async def load_revoked_jtis(self):
        rows = await self.db.get_active_revoked_sessions()
        self._revoked_jtis = {str(r["jti"]) for r in rows}
        logger.info(
            "action=load_revoked_jtis count=%d", len(self._revoked_jtis)
        )

    def start_cleanup_loop(self):
        self._cleanup_task = asyncio.create_task(self._cleanup_expired_jtis())

    def stop_cleanup_loop(self):
        if self._cleanup_task:
            self._cleanup_task.cancel()

    async def _cleanup_expired_jtis(self):
        while True:
            try:
                await asyncio.sleep(3600)
                removed = await self.db.delete_expired_sessions()
                if removed:
                    await self.load_revoked_jtis()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error("action=cleanup_jtis error=%s", str(e))

    def is_session_revoked(self, jti: str) -> bool:
        return jti in self._revoked_jtis

    async def authenticate(self, login: str, password: str) -> Optional[dict]:
        user = await self.db.get_user_by_username(login)
        if not user:
            return None
        if not user["is_active"]:
            return None
        if not pwd_context.verify(password, user["password_hash"]):
            return None
        return dict(user)

    async def create_user(self, username: str, password: str) -> dict:
        hashed = pwd_context.hash(password)
        row = await self.db.insert_user(username, hashed)
        return dict(row)

    async def change_password(self, username: str, new_password: str) -> bool:
        user = await self.db.get_user_by_username(username)
        if not user:
            return False
        hashed = pwd_context.hash(new_password)
        await self.db.update_password(username, hashed)
        return True

    async def deactivate_user(self, username: str) -> bool:
        user = await self.db.get_user_by_username(username)
        if not user or not user["is_active"]:
            return False
        revoked_jtis = await self.db.deactivate_user(username)
        for jti in revoked_jtis:
            self._revoked_jtis.add(str(jti))
        return True

    async def get_all_users(self) -> list[dict]:
        rows = await self.db.get_all_users()
        return [dict(r) for r in rows]

    async def create_session(self, username: str, expires_at: datetime) -> str:
        jti = await self.db.insert_session(username, expires_at)
        return jti

    async def revoke_session(self, jti: str):
        await self.db.revoke_session(jti)
        self._revoked_jtis.add(jti)

    async def seed_admin(self):
        count = await self.db.count_users()
        if count == 0:
            hashed = pwd_context.hash("admin")
            await self.db.insert_user_ignore("admin", hashed)
            logger.info("action=seed_admin status=created")
