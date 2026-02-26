from fastapi import Request, Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError, ExpiredSignatureError
import logging

from project.module_ch_api_gateway.services.clickhouse_service import ClickHouseService
from project.module_ch_api_gateway.services.nats_service import NatsService


logger = logging.getLogger("ch-api-gateway")

security = HTTPBearer()

def get_config(request: Request):
    return request.app.state.config

async def get_current_user(
    auth_creds: HTTPAuthorizationCredentials = Depends(security),
    config = Depends(get_config)
):
    token = auth_creds.credentials
    secret_key = config["auth"]["secret_key"]
    static_token = config["auth"]["static_token"]

    if token == static_token:
        return {"user": "api_client", "type": "static"}

    try:
        payload = jwt.decode(token, secret_key, algorithms=["HS256"])
        return payload
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

def get_ch_service(request: Request) -> ClickHouseService:
    return ClickHouseService(request.app.state.ch_client)

def get_nats_service(request: Request, config = Depends(get_config)) -> NatsService:
    return NatsService(request.app.state.nats_infra, config["nats"]["dg_subject"])
