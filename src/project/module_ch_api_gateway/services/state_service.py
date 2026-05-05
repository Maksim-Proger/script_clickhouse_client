from typing import Optional

from project.module_ch_api_gateway.infrastructure.db import DatabaseManager


class StateService:
    def __init__(self, db: DatabaseManager):
        self.db = db

    async def try_claim_dg_fetch(self, profile: str, owner_id: str) -> bool:
        return await self.db.try_claim_dg_fetch(profile, owner_id)

    async def get_profile_status(self, profile: str) -> Optional[dict]:
        row = await self.db.get_profile_status(profile)
        if row is None:
            return None
        return dict(row)

    async def release_dg_claim(
            self,
            profile: str,
            owner_id: str,
            success: bool,
            error: Optional[str] = None,
    ) -> None:
        await self.db.release_dg_claim(profile, owner_id, success, error)
