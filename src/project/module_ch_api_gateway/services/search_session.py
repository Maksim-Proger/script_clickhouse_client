import asyncio
import logging
import sys
import time
import uuid

logger = logging.getLogger("ch-api-gateway.search_session")

_CLEANUP_INTERVAL = 60


def _estimate_bytes(data: list) -> int:
    if not data:
        return 0
    sample = data[:50]
    size = 0
    for row in sample:
        size += sys.getsizeof(row)
        for key, value in row.items():
            size += sys.getsizeof(key) + sys.getsizeof(value)
    avg_row = size // len(sample)
    return avg_row * len(data)


class SessionExpiredError(Exception):
    pass


class SearchSessionStore:
    def __init__(self, ttl_seconds: int = 900, max_lifetime_seconds: int = 3600,
                 per_user_limit: int = 10, max_total_bytes: int = 512 * 1024 * 1024):
        self._ttl = ttl_seconds
        self._max_lifetime = max_lifetime_seconds
        self._per_user_limit = per_user_limit
        self._max_total_bytes = max_total_bytes
        self._sessions: dict[str, dict] = {}

    def create(self, user: str, data: list) -> str:
        incoming_bytes = _estimate_bytes(data)
        self._evict_for_user(user)
        self._evict_for_memory(incoming_bytes)
        sid = uuid.uuid4().hex
        now = time.monotonic()
        self._sessions[sid] = {
            "user": user, "data": data, "rows": len(data), "bytes": incoming_bytes,
            "created": now, "expires": now + self._ttl,
        }
        return sid

    def get(self, user: str, sid: str):
        session = self._sessions.get(sid)
        if session is None or session["user"] != user:
            return None
        now = time.monotonic()
        if now > session["expires"]:
            del self._sessions[sid]
            return None
        session["expires"] = min(now + self._ttl, session["created"] + self._max_lifetime)
        return session["data"]

    def sweep(self) -> None:
        now = time.monotonic()
        for sid in [sid for sid, s in self._sessions.items() if now > s["expires"]]:
            del self._sessions[sid]

    def _evict_for_user(self, user: str) -> None:
        user_sids = [sid for sid, s in self._sessions.items() if s["user"] == user]
        while len(user_sids) >= self._per_user_limit:
            oldest = min(user_sids, key=lambda sid: self._sessions[sid]["created"])
            del self._sessions[oldest]
            user_sids.remove(oldest)

    def _evict_for_memory(self, incoming_bytes: int) -> None:
        total = sum(s["bytes"] for s in self._sessions.values())
        while self._sessions and total + incoming_bytes > self._max_total_bytes:
            oldest = min(self._sessions, key=lambda sid: self._sessions[sid]["created"])
            logger.warning("action=search_session_evicted search_id=%s rows=%d", oldest, self._sessions[oldest]["rows"])
            total -= self._sessions[oldest]["bytes"]
            del self._sessions[oldest]


SESSIONS = SearchSessionStore()


async def session_cleanup_loop(interval: int = _CLEANUP_INTERVAL) -> None:
    while True:
        try:
            await asyncio.sleep(interval)
            SESSIONS.sweep()
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=session_cleanup error=%s", str(e))
