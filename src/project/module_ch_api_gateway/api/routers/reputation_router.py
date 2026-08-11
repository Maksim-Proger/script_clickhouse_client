from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from project.module_ch_api_gateway.api.dependencies.dependencies import get_current_user, resolve_exclusions
from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.services.feed_list_service import SessionExpiredError, SourceUnavailableError
from project.module_ch_api_gateway.services.reputation_service import ReputationService, needs_session

router = APIRouter(prefix="/ch", tags=["Reputation"])


def get_reputation_service(request: Request) -> ReputationService:
    return ReputationService(
        ch_client=request.app.state.ch_client,
        geoip_client=request.app.state.geoip_client,
    )


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


def _require_db(request: Request, filters: ReputationFilters) -> None:
    if not (filters.search_id or needs_session(filters)):
        return
    if not request.app.state.feed_list_service.is_available:
        raise HTTPException(status_code=503, detail="БД временно недоступна")


@router.post("/reputation")
async def get_reputation(
        request: Request,
        filters: ReputationFilters = None,
        service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    f = filters or ReputationFilters()
    await resolve_exclusions(request, f.exclude_list_ids)
    _require_db(request, f)
    try:
        return await service.get_reputation(f, _user_key(user), request.app.state.feed_list_service)
    except SessionExpiredError:
        raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")
    except SourceUnavailableError:
        raise HTTPException(status_code=503, detail="Источник данных временно недоступен")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/reputation/export")
async def export_reputation(
        request: Request,
        filters: ReputationFilters = None,
        service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    f = filters or ReputationFilters()
    await resolve_exclusions(request, f.exclude_list_ids)
    _require_db(request, f)

    feed_service = request.app.state.feed_list_service
    owner = _user_key(user)

    try:
        search_id, total = await service.start_export(f, owner, feed_service)
    except SessionExpiredError:
        raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")
    except SourceUnavailableError:
        raise HTTPException(status_code=503, detail="Источник данных временно недоступен")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    async def stream():
        yield '{"data": ['
        first = True
        async for chunk in service.iter_export_chunks(f, owner, feed_service, search_id):
            if not chunk:
                continue
            text = ",".join(chunk)
            yield text if first else "," + text
            first = False
        yield '], "total": ' + str(total) + '}'

    return StreamingResponse(stream(), media_type="application/json")
