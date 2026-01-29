import asyncio
import signal
from typing import Optional

from nats.aio.client import Client as NatsClientLib

from project.script_ch_writer.batch_buffer import BatchBuffer
from project.script_ch_writer.ch_writer import ClickHouseWriter
from project.script_ch_writer.nats_handler import NatsMessageHandler


class NatsWriterConsumer:
    def __init__(self, config: dict):
        self.nats_cfg = config["nats"]
        self.batch_cfg = config["batch"]

        self.shutdown_event = asyncio.Event()
        self.is_shutting_down = False

        self.nc: Optional[NatsClientLib] = None
        self.js = None

        self.buffer = BatchBuffer(
            batch_size=self.batch_cfg["size"],
            max_buffer_size=self.batch_cfg["max_buffer_size"],
        )

        self.writer = ClickHouseWriter(config["clickhouse"])

        self.handler = NatsMessageHandler(
            buffer=self.buffer,
            writer=self.writer,
            is_shutting_down_fn=lambda: self.is_shutting_down,
        )

    def _on_signal(self) -> None:
        asyncio.create_task(self.shutdown())

    async def shutdown(self) -> None:
        if self.is_shutting_down:
            return

        self.is_shutting_down = True

        try:
            await self.handler.flush()
        except Exception:
            pass

        if self.nc:
            try:
                await self.nc.close()
            except Exception:
                pass

        try:
            self.writer.close()
        except Exception:
            pass

        self.shutdown_event.set()

    async def start(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                self._on_signal
            )

        self.nc = NatsClientLib()
        await self.nc.connect(self.nats_cfg["url"])
        self.js = self.nc.jetstream()

        async def callback(msg):
            await self.handler.handle(msg)

        await self.js.subscribe(
            subject=self.nats_cfg["subject"],
            durable=self.nats_cfg["durable"],
            cb=callback
        )

        async def periodic_flush():
            while not self.is_shutting_down:
                await asyncio.sleep(self.batch_cfg["interval_sec"])
                try:
                    await self.handler.flush()
                except Exception:
                    pass

        await asyncio.gather(
            self.shutdown_event.wait(),
            periodic_flush()
        )
