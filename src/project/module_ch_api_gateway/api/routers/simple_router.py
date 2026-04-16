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
    if await state_service.should_fetch_from_source(filters.profile):
        try:
            payload = {
                "name": filters.profile,
                "period": {
                    "from": filters.period.from_date,
                    "to": filters.period.to_date,
                },
            }
            await nats_service.request_pa_data_load(payload)
        except TimeoutError:
            raise HTTPException(status_code=504, detail="Источник данных не ответил вовремя")

        await state_service.update_timestamp(filters.profile)

    return await ch_service.get_simple_ips(filters)
