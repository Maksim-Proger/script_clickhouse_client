from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import get_ch_service, get_current_user
from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters

router = APIRouter(prefix="/ch", tags=["ClickHouse"])


@router.post("/read")
async def read_ch(
        filters: CHReadFilters = None,
        service=Depends(get_ch_service),
        user=Depends(get_current_user)
):
    return await service.get_blocked_ips(filters or CHReadFilters())


@router.post("/export")
async def export_ch(
        filters: CHReadFilters = None,
        service=Depends(get_ch_service),
        user=Depends(get_current_user)
):
    f = filters or CHReadFilters()

    if f.period and f.period.from_date and f.period.to_date:
        try:
            fmt = "%Y-%m-%d %H:%M:%S"
            dt_from = datetime.strptime(f.period.from_date, fmt)
            dt_to = datetime.strptime(f.period.to_date, fmt)
            if (dt_to - dt_from).days > 7:
                raise HTTPException(
                    status_code=400,
                    detail="Период запроса не может превышать 7 суток"
                )
        except ValueError:
            pass
    data = await service.get_export_ips(f)
    return {"data": data, "total": len(data)}
