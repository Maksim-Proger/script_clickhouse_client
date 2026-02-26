import json
from nats.aio.client import Client as NatsClientLib
from typing import Optional

class NatsInfrastructure:
    def __init__(self, url: str):
        self.url = url
        self.nc: Optional[NatsClientLib] = None
        self.js = None

    async def connect(self):
        if self.nc and self.nc.is_connected:
            return
        self.nc = NatsClientLib()
        await self.nc.connect(self.url)
        self.js = self.nc.jetstream()

    async def publish(self, subject: str, data: dict):
        if not self.nc or not self.nc.is_connected:
            raise RuntimeError("NATS is not connected")
        await self.js.publish(subject, json.dumps(data).encode())

    async def close(self):
        if self.nc:
            await self.nc.close()
            self.nc = None
            self.js = None