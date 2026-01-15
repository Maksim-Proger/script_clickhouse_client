import time
import threading
import asyncio
import json

from nats.aio.client import Client as NATS
from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.parser.parser import parse_input
from project.nats_corn.nats_consumer import NatsDgConsumer

AB_INTERVAL: float = 20.0

async def ab_loop(nc):
    ab_client = AbClient()

    while True:
        try:
            raw_data_ab = ab_client.get_data()
            ips_ab = parse_input(raw_data_ab)

            payload = {
                "source": "AB",
                "ips": ips_ab,
            }

            # **await обязательно**, json нужен чтобы сериализовать список в строку
            await nc.publish(
                "ch.write.raw",
                json.dumps(payload).encode()
            )
        except Exception as e:
            # временно логируем
            print(f"AB error: {e}")

        await asyncio.sleep(AB_INTERVAL)

async def main():
    # создаём общий клиент NATS для AB и DG
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # запускаем AB-цикл как задачу
    asyncio.create_task(ab_loop(nc))

    # запускаем DG consumer
    consumer = NatsDgConsumer(nc)
    await consumer.start()


if __name__ == "__main__":
    asyncio.run(main())
