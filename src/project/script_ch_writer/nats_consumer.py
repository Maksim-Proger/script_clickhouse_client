import asyncio
import json
from datetime import datetime

from nats.aio.client import Client as NATS
from clickhouse_driver import Client as CHClient

BATCH_SIZE = 100
BATCH_INTERVAL = 1.0

class NatsWriterConsumer:

    def __init__(self):
        self.ch_client = CHClient(
            host='localhost',
            port=9000,
            database='feedgen',
            user='default',
            password=''
        )
        self.buffer = []
        self.lock = asyncio.Lock()

    async def flush_batch(self):
        async with self.lock:
            if not self.buffer:
                return
            batch = self.buffer[:]
            self.buffer.clear()

        await asyncio.to_thread(
            self.ch_client.execute,
            'INSERT INTO blocked_ips (blocked_at, ip_address, source, profile) VALUES',
            batch
        )

    async def start(self):
        nc = NATS()
        await nc.connect("nats://localhost:4222")
        js = nc.jetstream()

        async def callback(msg):
            try:
                data = msg.data.decode()
                obj = json.loads(data)
                blocked_at_str = obj.get("blocked_at")
                ip_address = obj.get("ip_address")
                if not blocked_at_str or not ip_address:
                    print("Invalid message, skipping:", data)
                    await msg.ack()
                    return

                blocked_at = datetime.fromisoformat(blocked_at_str)
                source = obj.get("source", "")
                profile = obj.get("profile", "")

                async with self.lock:
                    self.buffer.append((blocked_at, ip_address, source, profile))

                if len(self.buffer) >= BATCH_SIZE:
                    await self.flush_batch()

                await msg.ack()
            except Exception as e:
                print("Error processing message:", e)
                await msg.ack()

        await js.subscribe(
            subject="ch.write.raw",
            durable="ch_writer",
            cb=callback
        )

        async def periodic_flush():
            while True:
                await asyncio.sleep(BATCH_INTERVAL)
                await self.flush_batch()

        await asyncio.gather(
            asyncio.Event().wait(),
            periodic_flush()
        )
