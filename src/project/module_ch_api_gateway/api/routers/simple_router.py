from fastapi import APIRouter, Depends, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_ch_service,
    get_nats_service,
    get_state_service,
    get_current_user,
    check_rate_limit,
)
from project.module_ch_api_gateway.models.filters import CHSimpleFilters

router = APIRouter(tags=["Simple"])


@router.post("/ch/read/simple")
async def read_simple(
        filters: CHSimpleFilters,
        ch_service=Depends(get_ch_service),
        nats_service=Depends(get_nats_service),
        state_service=Depends(get_state_service),
        user=Depends(get_current_user),
        _=Depends(check_rate_limit),
):
    if not state_service.db.is_connected:
        raise HTTPException(status_code=503, detail="БД временно недоступна")

    if await state_service.should_fetch_from_source(filters.profile):
        payload = {
            "name": filters.profile,
            "period": {
                "from": filters.period.from_date,
                "to": filters.period.to_date,
            } if filters.period else None,
            "ip": filters.ip,
        }
        try:
            result = await nats_service.request_pa_data_load(payload)
        except TimeoutError:
            raise HTTPException(status_code=504, detail="Источник данных не ответил вовремя")
        finally:
            await state_service.update_timestamp(filters.profile)

        if result.get("status") == "error":
            raise HTTPException(status_code=502, detail=result.get("message", "Ошибка получения данных"))

        return result.get("data", [])

    return await ch_service.get_simple_ips(filters)
