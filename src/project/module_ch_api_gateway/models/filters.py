from typing import Optional

from pydantic import BaseModel, Field


class PeriodFilter(BaseModel):
    from_date: Optional[str] = Field(None, alias="from")
    to_date: Optional[str] = Field(None, alias="to")


class CHReadFilters(BaseModel):
    blocked_at: Optional[str] = None
    period: Optional[PeriodFilter] = None
    ip: Optional[str] = None
    source: Optional[str] = None
    profile: Optional[str] = None
    page: int = 1
    page_size: int = 100
    unique_ips: bool = False


class CHSimpleFilters(BaseModel):
    profile: str
    period: PeriodFilter
    ip: Optional[str] = None


class ReputationFilters(BaseModel):
    score_from: Optional[float] = None
    score_to: Optional[float] = None
    ip: Optional[str] = None
    asn: list[int] = []
    asn_exclude: bool = False
    country: list[str] = []
    country_exclude: bool = False
    page: int = Field(1, ge=1)
    page_size: int = Field(100, ge=1, le=1000)
    only_ip: bool = False
    search_id: Optional[str] = None
