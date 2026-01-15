import asyncio

from project.script_ch_writer.nats_consumer import NatsWriterConsumer

def main():
    consumer = NatsWriterConsumer()
    asyncio.run(consumer.start())

if __name__ == "__main__":
    main()
