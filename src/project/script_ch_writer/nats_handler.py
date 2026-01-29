import json
from datetime import datetime
from nats.aio.msg import Msg

from batch_buffer import BatchBuffer
from ch_writer import ClickHouseWriter


class NatsMessageHandler:
    def __init__(
        self,
        buffer: BatchBuffer,
        writer: ClickHouseWriter,
        is_shutting_down_fn,
    ):
        self.buffer = buffer
        self.writer = writer
        self.is_shutting_down = is_shutting_down_fn

    async def handle(self, msg: Msg) -> None:
        if self.is_shutting_down():
            await msg.nak()
            return

        if not await self.buffer.can_accept():
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

            await self.buffer.add(record)

            if await self.buffer.should_flush():
                await self.flush()

            await msg.ack()

        except Exception:
            await msg.nak()

    async def flush(self) -> None:
        batch = await self.buffer.snapshot()
        if not batch:
            return

        try:
            await self.writer.write(batch)
        except Exception:
            raise
        else:
            await self.buffer.drop_written(len(batch))
