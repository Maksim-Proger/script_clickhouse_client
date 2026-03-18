from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    login: str
    password: str


class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=3, max_length=150)
    password: str = Field(..., min_length=4)


class ChangePasswordRequest(BaseModel):
    username: str
    new_password: str = Field(..., min_length=4)


class DeleteUserRequest(BaseModel):
    username: str
