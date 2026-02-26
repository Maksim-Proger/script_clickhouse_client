from typing import Optional, Dict

from pydantic import BaseModel, Field


class PeriodFilter(BaseModel):
    from_date: Optional[str] = Field(None, alias="from")
    to_date: Optional[str] = Field(None, alias="to")


class CHReadFilters(BaseModel):
    blocked_at: Optional[str] = None
    period: Optional[dict] = None
    ip: Optional[str] = None
    source: Optional[str] = None
    profile: Optional[str] = None


class CHSimpleFilters(BaseModel):
    profile: str
    period: Dict[str, str]
