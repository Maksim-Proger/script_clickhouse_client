import asyncio
import json

from nats.aio.client import Client as NATS
from project.nats_corn.http.src1_client import AbClient
from project.nats_corn.nats_consumer import NatsDgConsumer
from project.nats_corn.parser.parser import parse_input


AB_INTERVAL: float = 20.0

async def ab_loop(nc: NATS) -> None:
    """
    Периодически забирает данные из AB и публикует их в NATS
    """
    ab_client = AbClient()

    while True:
        try:
            raw_data_ab = ab_client.get_data()
            ips_ab = parse_input(raw_data_ab)

            payload = {
                "source": "AB",
                "ips": ips_ab,
            }

            await nc.publish(
                "ch.write.raw",
                json.dumps(payload).encode()
            )

        except Exception as e:
            # временно — stdout, позже заменим на logging
            print(f"[NATS-CORN][AB] error: {e}")

        await asyncio.sleep(AB_INTERVAL)

async def _run() -> None:
    """
    Основная async-логика сервиса
    """
    nc = NATS()
    await nc.connect("nats://localhost:4222")

    # запускаем AB-цикл в фоне
    asyncio.create_task(ab_loop(nc))

    # запускаем DG consumer (блокирующий)
    consumer = NatsDgConsumer(nc)
    await consumer.start()

def main() -> None:
    """
    Синхронная точка входа для launcher / entrypoint
    """
    asyncio.run(_run())

if __name__ == "__main__":
    main()
