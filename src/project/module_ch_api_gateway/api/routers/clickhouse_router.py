from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_ch_service,
    get_current_user,
    get_feed_list_service,
    resolve_exclusions,
)
from project.module_ch_api_gateway.models.filters import CHReadFilters, CHSimpleFilters
from project.module_ch_api_gateway.services.feed_list_service import check_source_size

router = APIRouter(prefix="/ch", tags=["ClickHouse"])


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


def _envelope(data: list, total: int, page: int, page_size: int, search_id: str = None) -> dict:
    env = {
        "data": data,
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
    }
    if search_id is not None:
        env["search_id"] = search_id
    return env


def _check_export_period(f: CHReadFilters) -> None:
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


@router.post("/read")
async def read_ch(
        request: Request,
        filters: CHReadFilters = None,
        service=Depends(get_ch_service),
        feed_service=Depends(get_feed_list_service),
        user=Depends(get_current_user)
):
    f = filters or CHReadFilters()

    if f.search_id:
        if not feed_service.is_available:
            raise HTTPException(status_code=503, detail="БД временно недоступна")
        page = await feed_service.get_search_page(_user_key(user), f.search_id, f.page, f.page_size)
        if page is None:
            raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")
        return _envelope(page["data"], page["total"], f.page, f.page_size, search_id=f.search_id)

    exclude_lists = await resolve_exclusions(request, f.exclude_list_ids)
    if exclude_lists:
        try:
            check_source_size(await service.count_rows(f))
            built = await feed_service.build_ch_search(
                _user_key(user), service, f, exclude_lists, kind="read",
            )
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        page = await feed_service.get_search_page(_user_key(user), built["search_id"], f.page, f.page_size)
        data = page["data"] if page else []
        return _envelope(data, built["total"], f.page, f.page_size, search_id=built["search_id"])

    return await service.get_blocked_ips(f)


@router.post("/export")
async def export_ch(
        request: Request,
        filters: CHReadFilters = None,
        service=Depends(get_ch_service),
        feed_service=Depends(get_feed_list_service),
        user=Depends(get_current_user)
):
    f = filters or CHReadFilters()
    _check_export_period(f)

    exclude_lists = await resolve_exclusions(request, f.exclude_list_ids)
    if not f.search_id and not exclude_lists:
        data = await service.get_export_ips(f)
        return {"data": data, "total": len(data)}

    if not feed_service.is_available:
        raise HTTPException(status_code=503, detail="БД временно недоступна")

    owner = _user_key(user)
    if f.search_id:
        search_id = f.search_id
    else:
        try:
            check_source_size(await service.count_export_rows(f))
            built = await feed_service.build_ch_search(owner, service, f, exclude_lists, kind="export")
        except ValueError as e:
            raise HTTPException(status_code=400, detail=str(e))
        search_id = built["search_id"]

    total = await feed_service.get_search_total(owner, search_id)
    if total is None:
        raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")

    async def stream():
        yield '{"data": ['
        first = True
        async for chunk in feed_service.iter_search_rows(owner, search_id):
            text = ",".join(chunk)
            yield text if first else "," + text
            first = False
        yield '], "total": ' + str(total) + '}'

    return StreamingResponse(stream(), media_type="application/json")
