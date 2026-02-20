import jwt
import logging
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


logger = logging.getLogger("ch-client")

# Значения по умолчанию
SECRET_KEY = "not_set_yet"
ALGORITHM = "HS256"
STATIC_API_TOKEN = "not_set_yet"

security = HTTPBearer()

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(hours=24)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(auth: HTTPAuthorizationCredentials = Security(security)):
    token = auth.credentials

    if token == STATIC_API_TOKEN:
        return {"user": "api_client", "type": "static"}

    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        logger.warning("action=auth_failed error=token_expired")
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Сессия истекла. Войдите заново."
        )
    except jwt.JWTError as e:
        logger.error("action=auth_failed error=invalid_token details=%s", str(e))
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен"
        )