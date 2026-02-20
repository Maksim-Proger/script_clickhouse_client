import asyncio
from typing import List, Tuple


class BatchBuffer:
    def __init__(self, batch_size: int, max_buffer_size: int):
        self.batch_size = batch_size
        self.max_buffer_size = max_buffer_size

        self.buffer: List[Tuple] = []
        self.lock = asyncio.Lock()

    async def can_accept(self) -> bool:
        async with self.lock:
            return len(self.buffer) < self.max_buffer_size

    async def add(self, record: Tuple) -> None:
        async with self.lock:
            self.buffer.append(record)

    async def should_flush(self) -> bool:
        async with self.lock:
            return len(self.buffer) >= self.batch_size

    async def snapshot(self) -> List[Tuple]:
        async with self.lock:
            return self.buffer[:]

    async def drop_written(self, count: int) -> None:
        async with self.lock:
            self.buffer = self.buffer[count:]

    async def is_empty(self) -> bool:
        async with self.lock:
            return not self.buffer
