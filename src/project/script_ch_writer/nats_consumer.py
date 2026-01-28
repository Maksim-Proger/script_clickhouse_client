import asyncio
import json
from datetime import datetime

from nats.aio.client import Client as NatsClientLib
from clickhouse_driver import Client as CHClient


class NatsWriterConsumer:

    def __init__(self, config: dict):
        self.nats_cfg = config["nats"]
        self.ch_cfg = config["clickhouse"]
        self.batch_cfg = config["batch"]

        self.ch_client = CHClient(
            host=self.ch_cfg["host"],
            port=self.ch_cfg["port"],
            database=self.ch_cfg["database"],
            user=self.ch_cfg["user"],
            password=self.ch_cfg["password"],
        )

        self.batch_size = self.batch_cfg["size"]
        self.batch_interval = self.batch_cfg["interval_sec"]

        self.buffer = []
        self.lock = asyncio.Lock()

    async def flush_batch(self) -> None:
        async with self.lock:
            if not self.buffer:
                return
            batch = self.buffer[:]
            self.buffer.clear()

        await asyncio.to_thread(
            self.ch_client.execute,
            """
            INSERT INTO blocked_ips
            (blocked_at, ip_address, source, profile)
            VALUES
            """,
            batch
        )

    async def start(self) -> None:
        nc = NatsClientLib()
        await nc.connect(self.nats_cfg["url"])
        js = nc.jetstream()

        async def callback(msg):
            try:
                obj = json.loads(msg.data.decode())

                blocked_at = obj.get("blocked_at")
                ip_address = obj.get("ip_address")

                if not blocked_at or not ip_address:
                    await msg.ack()
                    return

                record = (
                    datetime.fromisoformat(blocked_at),
                    ip_address,
                    obj.get("source", ""),
                    obj.get("profile", ""),
                )

                async with self.lock:
                    self.buffer.append(record)

                if len(self.buffer) >= self.batch_size:
                    await self.flush_batch()

                await msg.ack()

            except Exception:
                await msg.ack()

        await js.subscribe(
            subject=self.nats_cfg["subject"],
            durable=self.nats_cfg["durable"],
            cb=callback
        )

        async def periodic_flush():
            while True:
                await asyncio.sleep(self.batch_interval)
                await self.flush_batch()

        await asyncio.gather(
            asyncio.Event().wait(),
            periodic_flush()
        )
