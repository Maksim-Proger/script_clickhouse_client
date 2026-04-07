

class NatsService:
    def __init__(self,
                 infra,
                 dg_subject: str,
                 pa_subject: str,
                 pa_timeout: float):
        self.infra = infra
        self.dg_subject = dg_subject
        self.pa_subject = pa_subject
        self.pa_timeout = pa_timeout

    async def request_data_load(self, params: dict):
        payload = {"action": "load", "params": params}
        await self.infra.publish(self.dg_subject, payload)

    async def request_pa_data_load(self, params: dict) -> dict:
        payload = {"action": "load", "params": params}
        return await self.infra.request(self.pa_subject, payload, timeout=self.pa_timeout)

    async def publish_external_data(self, data: dict):
        await self.infra.publish("data.received", data)
