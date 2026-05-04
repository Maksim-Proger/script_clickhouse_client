from datetime import datetime, timedelta, timezone

from project.module_ch_api_gateway.infrastructure.db import DatabaseManager

FETCH_TTL = timedelta(minutes=5)


class StateService:
    def __init__(self, db: DatabaseManager):
        self.db = db

    async def should_fetch_from_source(self, profile: str) -> bool:
        row = await self.db.get_profile_state(profile)
        if row is None:
            return True
        return datetime.now(timezone.utc) - row["updated_at"] > FETCH_TTL

    async def update_timestamp(self, profile: str) -> None:
        await self.db.upsert_profile_state(profile)
