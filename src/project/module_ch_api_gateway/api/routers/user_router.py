import logging

from asyncpg import UniqueViolationError
from fastapi import APIRouter, Depends, HTTPException

from project.module_ch_api_gateway.api.dependencies.dependencies import (
    get_current_user,
    get_user_service,
)
from project.module_ch_api_gateway.models.user_schemas import (
    ChangePasswordRequest,
    CreateUserRequest,
    DeleteUserRequest,
)

logger = logging.getLogger("ch-api-gateway")

router = APIRouter(prefix="/api/users", tags=["Users"])


@router.get("/")
async def list_users(
    user=Depends(get_current_user),
    user_service=Depends(get_user_service),
):
    users = await user_service.get_all_users()
    return [
        {
            "id": u["id"],
            "username": u["username"],
            "is_active": u["is_active"],
            "created_at": str(u["created_at"]),
        }
        for u in users
    ]


@router.post("/create")
async def create_user(
    data: CreateUserRequest,
    user=Depends(get_current_user),
    user_service=Depends(get_user_service),
):
    try:
        new_user = await user_service.create_user(data.username, data.password)
    except UniqueViolationError:
        raise HTTPException(status_code=409, detail="Пользователь уже существует")

    logger.info(
        "action=user_created by=%s new_user=%s",
        user.get("sub", "api_client"),
        data.username,
    )
    return {
        "status": "created",
        "user": {
            "id": new_user["id"],
            "username": new_user["username"],
            "is_active": new_user["is_active"],
            "created_at": str(new_user["created_at"]),
        },
    }


@router.post("/change-password")
async def change_password(
    data: ChangePasswordRequest,
    user=Depends(get_current_user),
    user_service=Depends(get_user_service),
):
    ok = await user_service.change_password(data.username, data.new_password)
    if not ok:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    logger.info(
        "action=password_changed by=%s target=%s",
        user.get("sub", "api_client"),
        data.username,
    )
    return {"ok": True}


@router.post("/delete")
async def delete_user(
    data: DeleteUserRequest,
    user=Depends(get_current_user),
    user_service=Depends(get_user_service),
):
    ok = await user_service.deactivate_user(data.username)
    if not ok:
        raise HTTPException(status_code=404, detail="Пользователь не найден")

    logger.info(
        "action=user_deactivated by=%s target=%s",
        user.get("sub", "api_client"),
        data.username,
    )
    return {"ok": True}
