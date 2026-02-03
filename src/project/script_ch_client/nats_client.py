import json
from typing import Optional

from nats.aio.client import Client as NatsClientLib


class NatsClient:
    def __init__(self, url: str, dg_subject: str):
        self.url = url
        self.dg_subject = dg_subject

        self.nc: Optional[NatsClientLib] = None
        self.js = None

    async def connect(self) -> None:
        if self.nc and self.nc.is_connected:
            return

        self.nc = NatsClientLib()
        await self.nc.connect(self.url)
        self.js = self.nc.jetstream()

    async def close(self) -> None:
        if self.nc:
            await self.nc.close()
            self.nc = None
            self.js = None

    async def publish_dg_load(self) -> None:
        if not self.nc or not self.nc.is_connected:
            raise RuntimeError("NATS is not connected")

        payload = {"action": "load"}

        await self.js.publish(
            self.dg_subject,
            json.dumps(payload).encode()
        )

    async def publish_web_data(self, data: dict) -> None:
        if not self.nc or not self.nc.is_connected:
            raise RuntimeError("NATS is not connected")

        await self.js.publish(
            "data.received",
            json.dumps(data).encode()
        )
