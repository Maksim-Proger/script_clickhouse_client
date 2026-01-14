import time
import threading
import asyncio

from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.parser.parser import parse_input
from project.nats_corn.nats_consumer import NatsDgConsumer

AB_INTERVAL: float = 20.0

def ab_loop():
    ab_client = AbClient()

    while True:
        # AB — каждые 20 секунд
        try:
            raw_data_ab = ab_client.get_data()
            ips_ab = parse_input(raw_data_ab)
            print("AB:", ";".join(ips_ab))
        except Exception as e:
            print(f"AB error: {e}")

        time.sleep(AB_INTERVAL)

def start_nats_consumer():
    consumer = NatsDgConsumer()
    asyncio.run(consumer.start())


def main():
    threading.Thread(target=ab_loop, daemon=True).start()
    start_nats_consumer()

if __name__ == "__main__":
    main()
