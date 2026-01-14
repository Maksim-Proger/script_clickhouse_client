from project.script_ch_client.nats_client import NatsClient

nats_client = NatsClient()

async def handle_dg_request() -> None:
    # здесь только бизнес-действие
    # "пришёл HTTP-запрос → отправили команду"
    await nats_client.publish_dg_load()

