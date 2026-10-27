import json


MAX_PAYLOAD_BYTES = 700_000


class NatsService:
    def __init__(self,
                 infra,
                 dg_subject: str,
                 pa_subject: str,
                 web_subject: str,
                 pa_timeout: float,
                 reputation_recalc_subject: str,
                 reputation_status_subject: str,
                 reputation_timeout: float):
        self.infra = infra
        self.dg_subject = dg_subject
        self.pa_subject = pa_subject
        self.web_subject = web_subject
        self.pa_timeout = pa_timeout
        self.reputation_recalc_subject = reputation_recalc_subject
        self.reputation_status_subject = reputation_status_subject
        self.reputation_timeout = reputation_timeout

    async def request_data_load(self, params: dict):
        payload = {"action": "load", "params": params}
        await self.infra.publish(self.dg_subject, payload)

    async def request_pa_data_load(self, params: dict) -> dict:
        payload = {"action": "load", "params": params}
        return await self.infra.request(self.pa_subject, payload, timeout=self.pa_timeout)

    async def publish_external_data(self, data):
        if isinstance(data, dict):
            records = data.get("records") or []
            source = data.get("source") or ""
            profile = data.get("profile") or ""
        elif isinstance(data, list):
            records, source, profile = data, "", ""
        else:
            await self.infra.publish(self.web_subject, data)
            return

        empty_size = len(json.dumps({"source": source, "profile": profile, "records": []}).encode())

        chunk = []
        size = empty_size
        for record in records:
            record_size = len(json.dumps(record).encode()) + 2
            if chunk and size + record_size > MAX_PAYLOAD_BYTES:
                await self._publish_chunk(chunk, source, profile)
                chunk = []
                size = empty_size
            chunk.append(record)
            size += record_size

        if chunk:
            await self._publish_chunk(chunk, source, profile)

    async def _publish_chunk(self, records: list, source: str, profile: str):
        await self.infra.publish(
            self.web_subject,
            {"source": source, "profile": profile, "records": records},
        )

    async def request_reputation_recalc(self) -> dict:
        return await self.infra.request(self.reputation_recalc_subject, {}, timeout=self.reputation_timeout)

    async def request_reputation_status(self) -> dict:
        return await self.infra.request(self.reputation_status_subject, {}, timeout=self.reputation_timeout)
