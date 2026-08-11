from typing import Literal, Optional

from pydantic import BaseModel, Field

from project.module_ch_api_gateway.models.filters import CHReadFilters, ReputationFilters


class FeedListCreateRequest(BaseModel):
    name: str = Field(..., min_length=1, max_length=200)
    description: str = Field("", max_length=2000)
    source_type: Literal["reputation", "blocked_ips", "manual"]
    reputation_filters: Optional[ReputationFilters] = None
    blocked_ips_filters: Optional[CHReadFilters] = None
    values: Optional[list[str]] = None


class FeedListStatusRequest(BaseModel):
    status: Literal["active", "archived"]
