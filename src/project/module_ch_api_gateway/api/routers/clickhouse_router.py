from fastapi import APIRouter, Depends
from models.filters import CHReadFilters, CHSimpleFilters
from api.dependencies import get_ch_service, get_current_user

router = APIRouter(prefix="/ch", tags=["ClickHouse"])

@router.post("/read")
async def read_ch(filters: CHReadFilters = None, service = Depends(get_ch_service), user = Depends(get_current_user)):
    return await service.get_blocked_ips(filters or CHReadFilters()) # [cite: 19]

@router.post("/read/simple")
async def read_simple(filters: CHSimpleFilters, service = Depends(get_ch_service), user = Depends(get_current_user)):
    return await service.get_simple_ips(filters)
