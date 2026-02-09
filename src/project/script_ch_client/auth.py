import jwt
from datetime import datetime, timedelta, timezone
from fastapi import HTTPException, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

# Значения по умолчанию (будут перезаписаны из YAML в main.py)
SECRET_KEY = "not_set_yet"
ALGORITHM = "HS256"
STATIC_API_TOKEN = "not_set_yet"

security = HTTPBearer()

def create_access_token(data: dict):
    """Генерирует временный JWT токен на 24 часа"""
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(hours=24)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)

async def get_current_user(auth: HTTPAuthorizationCredentials = Security(security)):
    """Универсальная зависимость для проверки Bearer токена"""
    token = auth.credentials

    # 1. Проверка на статический API ключ
    if token == STATIC_API_TOKEN:
        return {"user": "api_client", "type": "static"}

    # 2. Проверка сессионного JWT
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        return payload
    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Сессия истекла. Войдите заново."
        )
    except jwt.JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Невалидный токен"
        )