import logging
import uuid
import asyncio

from fastapi import APIRouter, Depends, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_ch_service,
    get_nats_service,
    get_state_service,
    get_current_user,
    check_rate_limit,
)
from project.module_ch_api_gateway.models.filters import CHSimpleFilters

logger = logging.getLogger("ch-api-gateway")

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

    owner_id = str(uuid.uuid4())

    payload = {
        "name": filters.profile,
        "period": {
            "from": filters.period.from_date,
            "to": filters.period.to_date,
        } if filters.period else None,
        "ip": filters.ip,
    }

    claimed = await state_service.try_claim_dg_fetch(filters.profile, owner_id)

    if claimed:
        try:
            result = await nats_service.request_pa_data_load(payload)
        except TimeoutError:
            try:
                await state_service.release_dg_claim(
                    filters.profile, owner_id, success=False, error="timeout",
                )
            except Exception as release_err:
                logger.error("action=release_claim_failed error=%s", str(release_err))
            raise HTTPException(status_code=504, detail="Источник данных не ответил вовремя")
        except Exception as e:
            try:
                await state_service.release_dg_claim(
                    filters.profile, owner_id, success=False, error=str(e),
                )
            except Exception as release_err:
                logger.error("action=release_claim_failed error=%s", str(release_err))
            raise HTTPException(status_code=502, detail="Ошибка получения данных")

        if result.get("status") == "error":
            try:
                await state_service.release_dg_claim(
                    filters.profile, owner_id, success=False, error=result.get("message"),
                )
            except Exception as release_err:
                logger.error("action=release_claim_failed error=%s", str(release_err))
            raise HTTPException(status_code=502, detail=result.get("message", "Ошибка получения данных"))

        try:
            await state_service.release_dg_claim(
                filters.profile, owner_id, success=True,
            )
        except Exception as release_err:
            logger.error("action=release_claim_failed error=%s", str(release_err))

        return result.get("data", [])

    profile_status = await state_service.get_profile_status(filters.profile)

    if profile_status and profile_status["status"] == "in_progress":
        data = await ch_service.get_simple_ips(filters)
        if data:
            return data
        raise HTTPException(
            status_code=202,
            detail="Данные обновляются, повторите запрос через несколько секунд",
        )

    data = await ch_service.get_simple_ips(filters)
    if data:
        return data

    if profile_status and profile_status["status"] == "success":
        for _ in range(3):
            await asyncio.sleep(3)
            data = await ch_service.get_simple_ips(filters)
            if data:
                return data

    return []
