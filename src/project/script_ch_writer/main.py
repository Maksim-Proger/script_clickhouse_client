import asyncio
from project.script_ch_writer.nats_consumer import NatsWriterConsumer


def main(config: dict) -> None:
    consumer = NatsWriterConsumer(config)
    asyncio.run(consumer.start())
