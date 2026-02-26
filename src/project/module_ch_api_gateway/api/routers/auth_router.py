from fastapi import APIRouter, Depends, HTTPException, Body

from project.module_ch_api_gateway.api.dependencies.dependencies import get_config
from project.module_ch_api_gateway.core.auth import create_access_token

router = APIRouter(tags=["Auth"])


@router.post("/login")
async def login(
        data: dict = Body(...),
        config=Depends(get_config)
):
    if data.get("login") == "admin" and data.get("password") == "admin":
        token = create_access_token(
            data={"sub": "admin"},
            secret_key=config["auth"]["secret_key"],
            algorithm="HS256"
        )
        return {"access_token": token, "token_type": "bearer"}

    raise HTTPException(status_code=401, detail="Invalid credentials")
