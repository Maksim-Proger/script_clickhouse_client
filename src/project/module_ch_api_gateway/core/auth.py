from datetime import datetime, timedelta, timezone
from jose import jwt
from fastapi import HTTPException, status

def create_access_token(data: dict, secret_key: str, algorithm: str):
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(hours=24)
    to_encode.update({"exp": expire})
    return jwt.encode(to_encode, secret_key, algorithm=algorithm)
