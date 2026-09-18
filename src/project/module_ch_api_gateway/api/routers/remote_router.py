import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_current_user,
    get_feed_list_service,
)
from project.module_ch_api_gateway.services.feed_list_service import FeedListService

logger = logging.getLogger("ch-api-gateway.remote")

router = APIRouter(tags=["Remote"])

DOSGATE_VALUE = "1"


def _user_key(user: dict) -> str:
    return user.get("sub") or user.get("user") or "anon"


def _client_ip(request: Request) -> str:
    return request.client.host if request.client else "-"


def _forwarded_for(request: Request) -> str:
    forwarded = request.headers.get("x-forwarded-for")
    return forwarded.split(",")[0].strip() if forwarded else "-"


def _line(row) -> str:
    value = row["value"]
    if row["value_type"] == "ip":
        value = f"{value}/32"
    return f"{value} {DOSGATE_VALUE}\n"


@router.get("/remote/{name}")
async def remote_feed_list(
        name: str,
        request: Request,
        service: FeedListService = Depends(get_feed_list_service),
        user=Depends(get_current_user),
):
    client = _client_ip(request)
    forwarded = _forwarded_for(request)
    who = _user_key(user)

    if not service.is_available:
        logger.warning("action=remote_feed_list status=db_unavailable name=%s client=%s user=%s", name, client, who)
        raise HTTPException(status_code=503, detail="БД временно недоступна")

    meta = await service.repo.get_catalog_row_by_name(name)
    if meta is None or meta["status"] != "active":
        logger.warning("action=remote_feed_list status=not_found name=%s client=%s user=%s", name, client, who)
        raise HTTPException(status_code=404, detail="Список не найден")

    if meta["current_version"] is None:
        if meta["pending_version"] is not None:
            logger.info("action=remote_feed_list status=building name=%s id=%d client=%s user=%s",
                        name, meta["id"], client, who)
            raise HTTPException(status_code=503, detail="Список собирается, повторите запрос позже")
        logger.warning("action=remote_feed_list status=no_version name=%s id=%d client=%s user=%s",
                       name, meta["id"], client, who)
        raise HTTPException(status_code=404, detail="У списка нет готовой версии")

    list_id = meta["id"]
    version = meta["current_version"]
    item_count = meta["item_count"]

    if item_count == 0:
        logger.warning("action=remote_feed_list status=empty name=%s id=%d version=%d client=%s user=%s",
                       name, list_id, version, client, who)
        raise HTTPException(status_code=404, detail="В списке нет адресов")

    logger.info(
        "action=remote_feed_list status=start name=%s id=%d version=%d items=%d client=%s forwarded=%s user=%s",
        name, list_id, version, item_count, client, forwarded, who,
    )

    async def stream():
        sent = 0
        async for chunk in service.repo.iter_values(list_id, version):
            sent += len(chunk)
            yield "".join(_line(row) for row in chunk)

        if sent != item_count:
            logger.error(
                "action=remote_feed_list status=truncated name=%s id=%d version=%d sent=%d items=%d client=%s user=%s",
                name, list_id, version, sent, item_count, client, who,
            )
            raise RuntimeError("Список отдан не полностью")

        logger.info(
            "action=remote_feed_list status=success name=%s id=%d version=%d rows=%d client=%s user=%s",
            name, list_id, version, sent, client, who,
        )

    return StreamingResponse(stream(), media_type="text/plain; charset=utf-8")
