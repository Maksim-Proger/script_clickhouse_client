import asyncio
import json

from project.utils.lifecycle import Lifecycle

PUBLISH_BATCH_SIZE = 500


async def publish_records(nc, subject: str, records: list, lifecycle: Lifecycle) -> None:
    batch = []
    for record in records:
        if lifecycle.is_shutting_down:
            break
        batch.append(nc.publish(subject, json.dumps(record).encode()))
        if len(batch) >= PUBLISH_BATCH_SIZE:
            await asyncio.gather(*batch)
            batch.clear()
    if batch:
        await asyncio.gather(*batch)
