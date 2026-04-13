from fastapi import APIRouter, Depends, Request, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import get_nats_service, get_current_user, check_rate_limit

router = APIRouter(tags=["Data"])


@router.post("/dg/request")
async def dg_request(
        request: Request,
        service=Depends(get_nats_service),
        user=Depends(get_current_user)
):
    data = await request.json()
    await service.request_data_load(data)
    return {"status": "accepted"}


@router.post("/dg/pa-request")
async def pa_dg_request(
        request: Request,
        service=Depends(get_nats_service),
        user=Depends(get_current_user),
        _=Depends(check_rate_limit)
):
    data = await request.json()
    try:
        result = await service.request_pa_data_load(data)
    except TimeoutError:
        raise HTTPException(status_code=504, detail="Источник данных не ответил вовремя")

    if result.get("status") == "error":
        raise HTTPException(status_code=502, detail=result.get("message", "Ошибка получения данных"))

    return result


@router.post("/data/receive")
async def receive_data(
        request: Request,
        service=Depends(get_nats_service),
        user=Depends(get_current_user)
):
    data = await request.json()
    await service.publish_external_data(data)
    return {"status": "success"}
