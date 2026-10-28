import logging

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_current_user,
    get_interactive_user,
)
from project.module_ch_api_gateway.services.auto_list_service import MIN_SCORE_SETTING

logger = logging.getLogger("ch-api-gateway.settings")

router = APIRouter(prefix="/api/settings", tags=["Settings"])


class SettingsSaveRequest(BaseModel):
    values: dict[str, str]


async def _current(request: Request) -> dict[str, str]:
    db = request.app.state.db
    if not db.is_connected:
        raise HTTPException(status_code=503, detail="БД временно недоступна")
    default = _format_score(request.app.state.auto_list_service.min_score)
    return {MIN_SCORE_SETTING: default, **await db.get_settings()}


def _format_score(score: float) -> str:
    return str(int(score)) if score.is_integer() else str(score)


def _check(key: str, value: str) -> str:
    if key != MIN_SCORE_SETTING:
        raise HTTPException(status_code=400, detail={"field": key, "message": "Неизвестная настройка"})
    try:
        score = float(value)
    except ValueError:
        score = None
    if score is None or not 20 <= score <= 100:
        raise HTTPException(status_code=400, detail={"field": key, "message": "Нужно число от 20 до 100"})
    return _format_score(score)


@router.get("")
async def get_settings(
        request: Request,
        user=Depends(get_current_user),
):
    return await _current(request)


@router.post("")
async def save_settings(
        body: SettingsSaveRequest,
        request: Request,
        user=Depends(get_interactive_user),
):
    old = await _current(request)
    values = {key: _check(key, value.strip()) for key, value in body.values.items()}

    changed = {key: value for key, value in values.items() if old[key] != value}
    if changed:
        await request.app.state.db.save_settings(changed, user["sub"])
    for key, value in changed.items():
        logger.info("action=setting_changed key=%s old=%s new=%s user=%s", key, old[key], value, user["sub"])
    return {"ok": True}
