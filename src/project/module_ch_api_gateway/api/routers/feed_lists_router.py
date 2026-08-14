import json
import logging
from datetime import datetime
from typing import Optional

import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.responses import StreamingResponse

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_ch_service,
    get_current_user,
    get_feed_list_service,
    get_interactive_user,
    resolve_exclusions,
)
from project.module_ch_api_gateway.api.routers.reputation_router import get_reputation_service
from project.module_ch_api_gateway.models.feed_list_schemas import FeedListCreateRequest, FeedListStatusRequest
from project.module_ch_api_gateway.models.filters import CHReadFilters, ReputationFilters
from project.module_ch_api_gateway.services.clickhouse_service import apply_default_period
from project.module_ch_api_gateway.services.feed_list_service import (
    DEFAULT_PERIOD_DAYS,
    FeedListService,
    SourceUnavailableError,
    check_source_size,
)

logger = logging.getLogger("ch-api-gateway.feed_lists")

router = APIRouter(prefix="/api/feed-lists", tags=["FeedLists"])


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


def _require_db(service: FeedListService) -> None:
    if not service.is_available:
        raise HTTPException(status_code=503, detail="БД временно недоступна")


async def _get_list_or_404(service: FeedListService, list_id: int) -> dict:
    row = await service.repo.get_list(list_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Список не найден")
    return service.serialize_list(row)


def _check_period_limit(filters: CHReadFilters) -> None:
    if not (filters.period and filters.period.from_date and filters.period.to_date):
        return
    fmt = "%Y-%m-%d %H:%M:%S"
    try:
        dt_from = datetime.strptime(filters.period.from_date, fmt)
        dt_to = datetime.strptime(filters.period.to_date, fmt)
    except ValueError:
        return
    if (dt_to - dt_from).days > DEFAULT_PERIOD_DAYS:
        raise HTTPException(
            status_code=400,
            detail=f"Период выборки не может превышать {DEFAULT_PERIOD_DAYS} суток",
        )


def _source_filters(filters, exclude_lists: Optional[list[dict]]) -> dict:
    clean = filters.model_dump(exclude_none=True)
    clean.pop("search_id", None)
    clean.pop("page", None)
    clean.pop("page_size", None)
    result = {"filters": clean}
    if exclude_lists:
        result["exclude_lists"] = [{"id": l["id"], "version": l["version"]} for l in exclude_lists]
    return result


def _json_default(value):
    if isinstance(value, datetime):
        return value.isoformat(sep=" ")
    return str(value)


@router.get("/")
async def list_feed_lists(
        search: Optional[str] = Query(None, max_length=200),
        status: Optional[str] = Query(None, pattern="^(creating|pending_sync|active|archived|failed|sync_failed)$"),
        page: int = Query(1, ge=1),
        page_size: int = Query(50, ge=1, le=500),
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_current_user),
):
    _require_db(service)
    rows, total = await service.repo.list_catalog(search, status, page, page_size)
    return {
        "data": [service.serialize_list(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
    }


@router.post("/create")
async def create_feed_list(
        request: Request,
        body: FeedListCreateRequest,
        service: FeedListService = Depends(get_feed_list_service),
        ch_service=Depends(get_ch_service),
        reputation_service=Depends(get_reputation_service),
        user=Depends(get_interactive_user),
):
    _require_db(service)
    created_by = _user_key(user)

    try:
        if body.source_type == "manual":
            if not body.values:
                raise ValueError("Для ручного списка нужно передать значения (values)")
            return await service.create_manual(body.name, body.description, created_by, body.values)

        if body.source_type == "reputation":
            filters = body.reputation_filters or ReputationFilters()
            filters.only_ip = False
            exclude_lists = await resolve_exclusions(request, filters.exclude_list_ids)
            exclude_ids = filters.exclude_list_ids

            if filters.search_id:
                search_id = filters.search_id
                if await service.get_search_total(created_by, search_id) is None:
                    raise HTTPException(status_code=410, detail="Результат поиска устарел, повторите запрос")

                async def build(list_id, version):
                    await service.build_from_reputation_session(list_id, version, created_by, search_id)
            else:
                check_source_size(await reputation_service.count_snapshot(filters))

                async def build(list_id, version):
                    await service.build_from_reputation_snapshot(
                        list_id, version, reputation_service, filters, exclude_ids,
                    )

            return await service.create_background(
                body.name, body.description, created_by, "reputation",
                _source_filters(filters, exclude_lists), build,
            )

        filters = body.blocked_ips_filters or CHReadFilters()
        apply_default_period(filters, DEFAULT_PERIOD_DAYS)
        _check_period_limit(filters)
        exclude_lists = await resolve_exclusions(request, filters.exclude_list_ids)
        exclude_ids = filters.exclude_list_ids
        check_source_size(await ch_service.count_unique_ips(filters))

        async def build(list_id, version):
            await service.build_from_ch(list_id, version, ch_service, filters, exclude_ids)

        return await service.create_background(
            body.name, body.description, created_by, "blocked_ips",
            _source_filters(filters, exclude_lists), build,
        )

    except asyncpg.UniqueViolationError:
        raise HTTPException(status_code=409, detail="Список с таким именем уже существует")
    except SourceUnavailableError:
        raise HTTPException(status_code=503, detail="Источник данных временно недоступен")
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/{list_id}")
async def get_feed_list(
        list_id: int,
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_current_user),
):
    _require_db(service)
    return await _get_list_or_404(service, list_id)


@router.get("/{list_id}/items")
async def get_feed_list_items(
        list_id: int,
        page: int = Query(1, ge=1),
        page_size: int = Query(100, ge=1, le=1000),
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_current_user),
):
    _require_db(service)
    meta = await _get_list_or_404(service, list_id)
    rows = await service.repo.get_items_page(list_id, meta["version"], page, page_size)
    total = meta["item_count"]
    return {
        "data": [dict(r) for r in rows],
        "total": total,
        "page": page,
        "page_size": page_size,
        "total_pages": (total + page_size - 1) // page_size if total > 0 else 1,
        "version": meta["version"],
        "status": meta["status"],
        "updated_at": meta["updated_at"],
    }


@router.get("/{list_id}/export")
async def export_feed_list(
        list_id: int,
        format: str = Query("txt", pattern="^(txt|json)$"),
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_current_user),
):
    _require_db(service)
    meta = await _get_list_or_404(service, list_id)
    if meta["status"] != "active":
        raise HTTPException(status_code=409, detail="Список не активен, выгрузка недоступна")

    logger.info(
        "action=feed_list_export id=%d format=%s items=%d user=%s",
        list_id, format, meta["item_count"], _user_key(user),
    )

    if format == "txt":
        async def stream_txt():
            async for chunk in service.repo.iter_items(list_id, meta["version"]):
                yield "\n".join(r["value"] for r in chunk) + "\n"

        return StreamingResponse(
            stream_txt(),
            media_type="text/plain; charset=utf-8",
            headers={
                "Content-Disposition":
                    f'attachment; filename="feed_list_{list_id}_v{meta["version"]}.txt"'
            },
        )

    async def stream_json():
        yield '{"list": ' + json.dumps(meta, ensure_ascii=False, default=_json_default) + ', "items": ['
        first = True
        async for chunk in service.repo.iter_items(list_id, meta["version"]):
            text = ",".join(json.dumps(dict(r), ensure_ascii=False, default=_json_default) for r in chunk)
            yield text if first else "," + text
            first = False
        yield "]}"

    return StreamingResponse(stream_json(), media_type="application/json")


@router.post("/{list_id}/status")
async def set_feed_list_status(
        list_id: int,
        body: FeedListStatusRequest,
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_interactive_user),
):
    _require_db(service)
    meta = await _get_list_or_404(service, list_id)
    if meta["status"] not in ("active", "archived"):
        raise HTTPException(status_code=409, detail="Список ещё не готов, смена статуса недоступна")
    row = await service.repo.set_status(list_id, body.status)
    logger.info(
        "action=feed_list_status_changed id=%d status=%s user=%s",
        list_id, body.status, _user_key(user),
    )
    return service.serialize_list(row)


@router.delete("/{list_id}")
async def delete_feed_list(
        list_id: int,
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_interactive_user),
):
    _require_db(service)
    meta = await _get_list_or_404(service, list_id)
    if meta["status"] in ("creating", "pending_sync"):
        raise HTTPException(
            status_code=409,
            detail="Список ещё обрабатывается, дождитесь завершения",
        )
    await service.delete_list(list_id)
    logger.info("action=feed_list_deleted id=%d user=%s", list_id, _user_key(user))
    return {"ok": True}
