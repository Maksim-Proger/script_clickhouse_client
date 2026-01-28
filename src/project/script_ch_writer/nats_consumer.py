import asyncio
import json
import signal
from datetime import datetime
from typing import Optional

from clickhouse_driver import Client as CHClient
from nats.aio.client import Client as NatsClientLib


class NatsWriterConsumer:

    def __init__(self, config: dict):
        self.nats_cfg = config["nats"]
        self.ch_cfg = config["clickhouse"]
        self.batch_cfg = config["batch"]

        self.batch_size = self.batch_cfg["size"]
        self.batch_interval = self.batch_cfg["interval_sec"]

        self.buffer = []
        self.lock = asyncio.Lock()

        self.shutdown_event = asyncio.Event()
        self.is_shutting_down = False

        self.nc: Optional[NatsClientLib] = None
        self.js = None

        self.ch_client = CHClient(
            host=self.ch_cfg["host"],
            port=self.ch_cfg["port"],
            database=self.ch_cfg["database"],
            user=self.ch_cfg["user"],
            password=self.ch_cfg["password"],
        )

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

    def _on_signal(self) -> None:
        asyncio.create_task(self.shutdown())

    async def shutdown(self) -> None:
        if self.is_shutting_down:
            return

        self.is_shutting_down = True

        try:
            await self.flush_batch()
        except Exception:
            pass

        if self.nc:
            try:
                await self.nc.close()
            except Exception:
                pass

        try:
            self.ch_client.disconnect()
        except Exception:
            pass

        self.shutdown_event.set()

    async def start(self) -> None:
        loop = asyncio.get_running_loop()

        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, self._on_signal)

        self.nc = NatsClientLib()
        await self.nc.connect(self.nats_cfg["url"])
        self.js = self.nc.jetstream()

        async def callback(msg):
            if self.is_shutting_down:
                await msg.nak()
                return

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
                await msg.nak()

        await self.js.subscribe(
            subject=self.nats_cfg["subject"],
            durable=self.nats_cfg["durable"],
            cb=callback
        )

        async def periodic_flush():
            while not self.is_shutting_down:
                await asyncio.sleep(self.batch_interval)
                try:
                    await self.flush_batch()
                except Exception:
                    pass

        await asyncio.gather(
            self.shutdown_event.wait(),
            periodic_flush()
        )
