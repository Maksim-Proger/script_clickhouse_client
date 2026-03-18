import logging
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_config,
    get_user_service,
    get_current_user,
)
from project.module_ch_api_gateway.core.auth import create_access_token
from project.module_ch_api_gateway.models.user_schemas import LoginRequest

logger = logging.getLogger("ch-api-gateway")

router = APIRouter(tags=["Auth"])


@router.post("/login")
async def login(
    data: LoginRequest,
    config=Depends(get_config),
    user_service=Depends(get_user_service),
):
    if not user_service.db.is_connected:
        raise HTTPException(status_code=503, detail="БД временно недоступна")

    user = await user_service.authenticate(data.login, data.password)
    if not user:
        raise HTTPException(status_code=401, detail="Неверный логин или пароль")

    expires_at = datetime.now(timezone.utc) + timedelta(hours=24)
    jti = await user_service.create_session(user["username"], expires_at)

    token = create_access_token(
        data={"sub": user["username"], "jti": jti},
        secret_key=config["auth"]["secret_key"],
        algorithm="HS256",
    )

    logger.info("action=login_success username=%s jti=%s", user["username"], jti)
    return {"access_token": token, "token_type": "bearer"}


@router.post("/logout")
async def logout(
    user=Depends(get_current_user),
    user_service=Depends(get_user_service),
):
    jti = user.get("jti")
    if jti:
        await user_service.revoke_session(jti)
        logger.info("action=logout username=%s jti=%s", user.get("sub"), jti)
    return {"ok": True}
