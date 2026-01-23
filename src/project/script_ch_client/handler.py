from project.script_ch_client.nats_client import NatsClient
from project.script_ch_client.ch_handler import read_from_clickhouse

nats_client = NatsClient()

async def handle_dg_request() -> None:
    await nats_client.publish_dg_load()

async def handle_ch_request(query: str) -> dict:
    return await read_from_clickhouse(query)

