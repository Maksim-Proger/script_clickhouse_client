import asyncio
import logging
import time

from fastapi import Request, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError, ExpiredSignatureError

from project.module_ch_api_gateway.services.clickhouse_service import ClickHouseService
from project.module_ch_api_gateway.services.nats_service import NatsService
from project.module_ch_api_gateway.services.state_service import StateService

logger = logging.getLogger("ch-api-gateway.dependencies")

RATE_LIMIT_MAX = 5
RATE_LIMIT_RATE = 0.5
RATE_LIMIT_CLEANUP_INTERVAL = 60
RATE_LIMIT_STALE_AFTER = 300

security = HTTPBearer()


def get_config(request: Request):
    return request.app.state.config


def get_user_service(request: Request):
    return request.app.state.user_service


async def get_current_user(
        auth_creds: HTTPAuthorizationCredentials = Depends(security),
        config=Depends(get_config),
        user_service=Depends(get_user_service),
):
    token = auth_creds.credentials
    secret_key = config["auth"]["secret_key"]
    static_token = config["auth"]["static_token"]

    if token == static_token:
        return {"user": "api_client", "type": "static"}

    try:
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
    except ExpiredSignatureError:
        logger.warning("action=auth_failed error=token_expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Сессия истекла. Войдите заново."
        )
    except JWTError as e:
        logger.error("action=auth_failed error=invalid_token details=%s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен"
        )

    jti = payload.get("jti")
    if jti and user_service.is_session_revoked(jti):
        logger.warning("action=auth_failed error=session_revoked jti=%s", jti)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Сессия отозвана. Войдите заново."
        )

    return payload


async def check_rate_limit(request: Request):
    data = await request.json()
    profile = data.get("profile")
    if not profile:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="profile обязателен")

    counters = request.app.state.rate_limit_counters

    now = time.monotonic()

    if profile not in counters:
        counters[profile] = {"tokens": float(RATE_LIMIT_MAX - 1), "last": now}
        return

    entry = counters[profile]
    elapsed = now - entry["last"]
    entry["tokens"] = min(RATE_LIMIT_MAX, entry["tokens"] + elapsed * RATE_LIMIT_RATE)
    entry["last"] = now

    if entry["tokens"] < 1:
        logger.warning("action=rate_limit_exceeded profile=%s", profile)
        raise HTTPException(status_code=status.HTTP_429_TOO_MANY_REQUESTS, detail="Слишком много запросов")

    entry["tokens"] -= 1


async def rate_limit_cleanup_loop(app_state):
    while True:
        try:
            await asyncio.sleep(RATE_LIMIT_CLEANUP_INTERVAL)
            now = time.monotonic()
            stale = [
                k for k, v in app_state.rate_limit_counters.items()
                if now - v["last"] > RATE_LIMIT_STALE_AFTER
            ]
            for k in stale:
                del app_state.rate_limit_counters[k]
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error("action=rate_limit_cleanup error=%s", str(e))


def get_ch_service(request: Request) -> ClickHouseService:
    return ClickHouseService(request.app.state.ch_client)


def get_state_service(request: Request) -> StateService:
    return request.app.state.state_service


def get_nats_service(request: Request, config=Depends(get_config)) -> NatsService:
    nats_cfg = config["nats"]
    return NatsService(
        request.app.state.nats_infra,
        config["nats"]["dg_subject"],
        pa_subject=nats_cfg["pa_subject"],
        pa_timeout=float(nats_cfg["pa_timeout_sec"]),
    )
