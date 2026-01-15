import asyncio

from nats.aio.client import Client as NATS

class NatsWriterConsumer:
    async def start(self):
        nc = NATS()
        await nc.connect("nats://localhost:4222")

        async def callback(msg):
            data = msg.data.decode()
            print("WRITE:", data)

        await nc.subscribe("ch.write.raw", cb=callback)
        await asyncio.Event().wait()
