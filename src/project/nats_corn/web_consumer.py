import json
from project.nats_corn.lifecycle import Lifecycle
from nats.aio.client import Client as  NatsClient
from project.nats_corn.parser.parser import parse_input

class NatsWebConsumer:
    def __init__(
        self,
        nc: NatsClient,
        config: dict,
        lifecycle: Lifecycle,
    ):
        self.nc = nc
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.subject = "data.received"  # Subject, на который публикуются данные из script_ch_client
        self.durable = "web_consumer_durable"  # Можно настроить в config, если нужно
        self.lifecycle = lifecycle

    async def handle_msg(self, msg):
        if self.lifecycle.is_shutting_down:
            await msg.nak()
            return
        try:
            payload = json.loads(msg.data.decode())
            records = parse_input(
                data=json.dumps(payload),
                source="web_interface",
                dt_format=self.dt_format
            )
            for record in records:
                await self.nc.publish(
                    "ch.write.raw",
                    json.dumps(record).encode()
                )
            # print("Parsed and published to ch.write.raw:", records)
            await msg.ack()
        except Exception:
            await msg.nak()

    async def start(self) -> None:
        js = self.nc.jetstream()
        await js.subscribe(
            subject=self.subject,
            durable=self.durable,
            cb=self.handle_msg
        )
        await self.lifecycle.shutdown_event.wait()