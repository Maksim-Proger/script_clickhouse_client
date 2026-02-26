class NatsService:
    def __init__(self, infra, dg_subject: str):
        self.infra = infra
        self.dg_subject = dg_subject

    async def request_data_load(self, params: dict):
        payload = {"action": "load", "params": params}
        await self.infra.publish(self.dg_subject, payload)

    async def publish_external_data(self, data: dict):
        await self.infra.publish("data.received", data)
