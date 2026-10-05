from typing import Literal, Optional

from pydantic import BaseModel, Field


class ReputationCalcRequest(BaseModel):
    source: Literal["dosgate", "ipban"]
    profile: Optional[str] = Field(None, max_length=150)
