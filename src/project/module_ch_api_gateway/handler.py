from project.module_ch_api_gateway.nats_client import NatsClient
from project.module_ch_api_gateway.ch_handler import read_from_clickhouse, build_blocked_ips_query, CHReadFilters


async def handle_dg_request(nats_client: NatsClient, data: dict) -> None:
    await nats_client.publish_dg_load(data)

async def handle_web_data(nats_client: NatsClient, data: dict) -> None:
    await nats_client.publish_web_data(data)

async def handle_ch_request(filters: CHReadFilters, ch_cfg: dict) -> dict:
    query = build_blocked_ips_query(filters)

    return await read_from_clickhouse(
        query=query,
        host=ch_cfg["host"],
        port=ch_cfg["http_port"],
        timeout_sec=ch_cfg["timeout_sec"],
    )
