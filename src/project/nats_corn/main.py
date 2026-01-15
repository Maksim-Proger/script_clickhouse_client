import time
import threading
import asyncio
import json

from nats.aio.client import Client as NATS
from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.parser.parser import parse_input
from project.nats_corn.nats_consumer import NatsDgConsumer

AB_INTERVAL: float = 20.0

def ab_loop():
    ab_client = AbClient()
    nc = NATS()
    asyncio.run(nc.connect("nats://localhost:4222"))

    while True:
        raw_data_ab = ab_client.get_data()
        ips_ab = parse_input(raw_data_ab)

        payload = {
            "source": "AB",
            "ips": ips_ab,
        }

        nc.publish(
            "ch.write.raw",
            json.dumps(payload).encode() # А зачем на тут json?
        )

        time.sleep(AB_INTERVAL)

def start_nats_consumer():
    consumer = NatsDgConsumer()
    asyncio.run(consumer.start())


def main():
    threading.Thread(target=ab_loop, daemon=True).start()
    start_nats_consumer()

if __name__ == "__main__":
    main()
