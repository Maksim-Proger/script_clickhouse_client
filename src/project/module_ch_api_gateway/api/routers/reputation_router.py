from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_current_user,
    get_interactive_user,
    resolve_exclusions,
)
from project.module_ch_api_gateway.models.filters import ReputationFilters
from project.module_ch_api_gateway.models.reputation_calc_schemas import ReputationCalcRequest
from project.module_ch_api_gateway.services.feed_list_service import SessionExpiredError, SourceUnavailableError
from project.module_ch_api_gateway.services.reputation_calc_service import CalcBusyError, ReputationCalcService
from project.module_ch_api_gateway.services.reputation_service import ReputationService, needs_session

router = APIRouter(prefix="/ch", tags=["Reputation"])


def get_reputation_service(request: Request) -> ReputationService:
    return ReputationService(
        ch_client=request.app.state.ch_client,
        geoip_client=request.app.state.geoip_client,
        stream_client=request.app.state.ch_stream_client,
    )


def get_reputation_calc_service(request: Request) -> ReputationCalcService:
    return request.app.state.reputation_calc_service


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


def _require_calc_db(service: ReputationCalcService) -> None:
    if not service.is_available:
        raise HTTPException(status_code=503, detail="БД временно недоступна")


async def _get_calc_or_404(service: ReputationCalcService, calc_id: int) -> dict:
    row = await service.repo.get_calc(calc_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Расчёт не найден")
    return dict(row)


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
    exclude_lists = await resolve_exclusions(request, f.exclude_list_ids)
    _require_db(request, f)
    try:
        return await service.get_reputation(
            f, _user_key(user), request.app.state.feed_list_service, exclude_lists,
        )
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
    exclude_lists = await resolve_exclusions(request, f.exclude_list_ids)
    _require_db(request, f)

    feed_service = request.app.state.feed_list_service
    owner = _user_key(user)

    try:
        search_id, total = await service.start_export(f, owner, feed_service, exclude_lists)
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


@router.post("/reputation/calcs")
async def start_reputation_calc(
        body: ReputationCalcRequest,
        service: ReputationCalcService = Depends(get_reputation_calc_service),
        user=Depends(get_interactive_user),
):
    _require_calc_db(service)
    profile = (body.profile or "").strip() or None
    try:
        return await service.start_calc(body.source, profile, _user_key(user))
    except CalcBusyError as e:
        raise HTTPException(status_code=409, detail=str(e))


@router.get("/reputation/calcs")
async def list_reputation_calcs(
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=500),
        service: ReputationCalcService = Depends(get_reputation_calc_service),
        user=Depends(get_current_user),
):
    _require_calc_db(service)
    rows, total = await service.repo.list_calcs(page, page_size)
    return {
        "data": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
    }


@router.get("/reputation/calcs/{calc_id}")
async def get_reputation_calc(
        calc_id: int,
        service: ReputationCalcService = Depends(get_reputation_calc_service),
        user=Depends(get_current_user),
):
    _require_calc_db(service)
    return await _get_calc_or_404(service, calc_id)


@router.get("/reputation/calcs/{calc_id}/rows")
async def get_reputation_calc_rows(
        calc_id: int,
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1, le=1000),
        service: ReputationCalcService = Depends(get_reputation_calc_service),
        reputation_service: ReputationService = Depends(get_reputation_service),
        user=Depends(get_current_user),
):
    _require_calc_db(service)
    calc = await _get_calc_or_404(service, calc_id)
    if calc["status"] == "building":
        raise HTTPException(status_code=409, detail="Расчёт ещё выполняется")
    if calc["status"] != "ready":
        raise HTTPException(status_code=409, detail="Расчёт завершился ошибкой")

    try:
        return await reputation_service.get_calc_page(calc_id, calc["row_count"], page, page_size)
    except SourceUnavailableError:
        raise HTTPException(status_code=503, detail="Источник данных временно недоступен")
