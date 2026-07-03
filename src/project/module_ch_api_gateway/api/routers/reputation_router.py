from fastapi import APIRouter, Depends, HTTPException, Request

from project.module_ch_api_gateway.api.dependencies.dependencies import get_current_user
from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.reputation_service import ReputationService
from project.module_ch_api_gateway.services.search_session import SESSIONS, SessionExpiredError

router = APIRouter(prefix="/ch", tags=["Reputation"])


def get_reputation_service(request: Request) -> ReputationService:
    return ReputationService(
        ch_client=request.app.state.ch_client,
        geoip_client=request.app.state.geoip_client,
        sessions=SESSIONS,
    )


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


@router.post("/reputation")
async def get_reputation(
        filters: ReputationFilters = None,
        service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    f = filters or ReputationFilters()
    try:
        return await service.get_reputation(f, _user_key(user))
    except SessionExpiredError:
        raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/reputation/export")
async def export_reputation(
        filters: ReputationFilters = None,
        service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    f = filters or ReputationFilters()
    try:
        return await service.export_reputation(f, _user_key(user))
    except SessionExpiredError:
        raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
