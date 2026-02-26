from pydantic import BaseModel


class ClickHouseConfig(BaseModel):
    host: str
    http_port: int
    timeout_sec: int


class AppConfig(BaseModel):
    api: dict
    cors: dict
    auth: dict
    nats: dict
    clickhouse: ClickHouseConfig
