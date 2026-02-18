import json
from nats.aio.client import Client as NatsClient
from project.nats_corn.lifecycle import Lifecycle
from project.nats_corn.dg_manager import DgSourceManager

class NatsDgConsumer:
    def __init__(
            self,
            nc: NatsClient,
            config: dict,
            lifecycle: Lifecycle,
            dg_manager: DgSourceManager,
    ):
        self.nc = nc
        self.dg_manager = dg_manager
        self.subject = config["nats"]["dg_consumer"]["subject"]
        self.durable = config["nats"]["dg_consumer"]["durable"]
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return

        try:
            # Получаем данные, которые прислал фронтенд через script_ch_client
            payload = json.loads(msg.data.decode())

            # Если пришла команда на загрузку данных
            if payload.get("action") == "load":
                # Передаем весь объект в менеджер для ручного выполнения
                await self.dg_manager.run_manual(payload)

            await msg.ack()
        except Exception as e:
            print(f"Error in NatsDgConsumer handle_msg: {e}")
            await msg.nak()

    async def start(self) -> None:
        js = self.nc.jetstream()

        # Подписываемся на команды из NATS
        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=self.handle_msg
        )

        # Держим консьюмер запущенным до сигнала остановки
        await self.lifecycle.shutdown_event.wait()
