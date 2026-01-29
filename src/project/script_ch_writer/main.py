import asyncio
from consumer import NatsWriterConsumer


def main(config: dict) -> None:
    consumer = NatsWriterConsumer(config)
    asyncio.run(consumer.start())
