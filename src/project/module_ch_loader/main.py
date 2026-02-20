import asyncio

from project.module_ch_loader.consumer import NatsWriterConsumer
from project.utils.logging_formatter import setup_logging


def main(config: dict) -> None:
    logger = setup_logging("ch-writer")
    logger.info("action=process_start status=initializing")

    consumer = NatsWriterConsumer(config)
    asyncio.run(consumer.start())
