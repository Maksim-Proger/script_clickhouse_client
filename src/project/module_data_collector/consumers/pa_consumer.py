import json
import logging

logger = logging.getLogger("data-collector.pa_consumer")


class NatsPaConsumer:

    def __init__(self, nc, config: dict, lifecycle, dg_manager):
        self.nc = nc
        self.dg_manager = dg_manager
        self.subject = config["nats"]["pa_consumer"]["subject"]
        self.lifecycle = lifecycle

    async def handle_msg(self, msg) -> None:
        if self.lifecycle.is_shutting_down:
            return

        reply = msg.reply

        try:
            payload = json.loads(msg.data.decode())
            logger.info(
                "action=pa_request_received subject=%s profile=%s",
                self.subject,
                payload.get("params", {}).get("name", "unknown"),
            )

            records = await self.dg_manager.run_pa(payload)

            if reply:
                await self.nc.publish(
                    reply,
                    json.dumps({"status": "ok", "data": records, "total": len(records)}).encode(),
                )

        except Exception as e:
            logger.error("action=pa_request_failed error=%s", str(e))
            if reply:
                await self.nc.publish(
                    reply,
                    json.dumps({"status": "error", "message": str(e)}).encode(),
                )

    async def start(self) -> None:
        await self.nc.subscribe(self.subject, cb=self.handle_msg)
        logger.info("action=pa_consumer_init subject=%s", self.subject)
        await self.lifecycle.shutdown_event.wait()
