import asyncio
import signal


class Lifecycle:
    def __init__(self):
        self.shutdown_event = asyncio.Event()
        self.is_shutting_down = False

    def install_signal_handlers(self) -> None:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(
                sig,
                self._on_signal
            )

    def _on_signal(self) -> None:
        asyncio.create_task(self.shutdown())

    async def shutdown(self) -> None:
        if self.is_shutting_down:
            return

        self.is_shutting_down = True
        self.shutdown_event.set()
