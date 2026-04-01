import asyncio
import json
import logging
import re
from datetime import datetime

from nats.aio.msg import Msg

from project.module_ch_loader.core.batch_buffer import BatchBuffer
from project.module_ch_loader.infrastructure.ch_writer import ClickHouseWriter

logger = logging.getLogger("ch-loader.handler")

_IPV4_REGEX = re.compile(
    r'^(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}$'
)


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
        self._flush_lock = asyncio.Lock()

    async def handle(self, msg: Msg) -> None:
        if self.is_shutting_down():
            await msg.nak()
            return

        if not await self.buffer.can_accept():
            logger.warning("action=buffer_full message=rejecting_new_msg")
            await msg.nak()
            return

        try:
            obj = json.loads(msg.data.decode())
            blocked_at = obj.get("blocked_at")
            ip_address = obj.get("ip_address")

            if not blocked_at or not ip_address:
                logger.warning("action=invalid_msg_format error=missing_required_fields payload=%s", obj)
                await msg.ack()
                return

            if not _IPV4_REGEX.match(ip_address):
                logger.warning("action=invalid_ip_skipped ip=%s", ip_address)
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

        except json.JSONDecodeError as e:
            logger.error("action=msg_decode_failed error=%s", str(e))
            await msg.ack()
        except Exception as e:
            logger.error("action=msg_handle_failed error=%s", str(e))
            await msg.nak()

    async def flush(self) -> None:
        async with self._flush_lock:
            batch = await self.buffer.snapshot()
            if not batch:
                return

            try:
                await self.writer.write(batch)
            except Exception as e:
                error_str = str(e)
                if "Cannot parse" in error_str or "Code: 44" in error_str:
                    logger.error(
                        "action=flush_failed_data_error dropped=%d error=%s",
                        len(batch), error_str
                    )
                    await self.buffer.drop_written(len(batch))
                else:
                    logger.error(
                        "action=flush_failed_retryable error=%s",
                        error_str
                    )
                raise
            else:
                await self.buffer.drop_written(len(batch))
