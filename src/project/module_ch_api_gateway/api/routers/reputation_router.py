from fastapi import APIRouter, Depends, Request

from project.module_ch_api_gateway.api.dependencies.dependencies import get_current_user
from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.reputation_service import ReputationService

router = APIRouter(prefix="/ch", tags=["Reputation"])


def get_reputation_service(request: Request) -> ReputationService:
    return ReputationService(
        ch_client=request.app.state.ch_client,
        geoip_client=request.app.state.geoip_client,
    )


@router.post("/reputation")
async def get_reputation(
        filters: ReputationFilters = None,
        service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    f = filters or ReputationFilters()
    return await service.get_reputation(page=f.page, page_size=f.page_size)

