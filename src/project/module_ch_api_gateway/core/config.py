from pydantic import BaseModel


class ClickHouseConfig(BaseModel):
    host: str
    http_port: int
    timeout_sec: int
    user: str
    password: str


class PostgresConfig(BaseModel):
    host: str
    port: int
    database: str
    user: str
    password: str
    min_connections: int = 2
    max_connections: int = 10


class AppConfig(BaseModel):
    api: dict
    cors: dict
    auth: dict
    nats: dict
    clickhouse: ClickHouseConfig
    postgres: PostgresConfig
