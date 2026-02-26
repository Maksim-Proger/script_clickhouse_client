from fastapi import APIRouter, Depends

from project.module_ch_api_gateway.api.dependencies.dependencies import get_ch_service, get_current_user
from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters

router = APIRouter(prefix="/ch", tags=["ClickHouse"])


@router.post("/read")
async def read_ch(filters: CHReadFilters = None, service=Depends(get_ch_service), user=Depends(get_current_user)):
    return await service.get_blocked_ips(filters or CHReadFilters())


@router.post("/read/simple")
async def read_simple(filters: CHSimpleFilters, service=Depends(get_ch_service), user=Depends(get_current_user)):
    return await service.get_simple_ips(filters)
