from fastapi import APIRouter, Depends, Request
from api.dependencies import get_nats_service, get_current_user

router = APIRouter(tags=["Data"])

@router.post("/dg/request")
async def dg_request(
    request: Request,
    service = Depends(get_nats_service),
    user = Depends(get_current_user)
):
    data = await request.json()
    await service.request_data_load(data)
    return {"status": "accepted"}

@router.post("/data/receive")
async def receive_data(
    request: Request,
    service = Depends(get_nats_service),
    user = Depends(get_current_user)
):
    data = await request.json()
    await service.publish_external_data(data)
    return {"status": "success"}
