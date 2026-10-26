import asyncio
import logging
from datetime import datetime, timezone

from project.utils.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import stream_extract_records, _is_in_period
from project.module_data_collector.publisher import PUBLISH_BATCH_SIZE, publish_records
from project.utils.http.async_client import BaseAsyncHttpClient, HttpxAsyncReader

logger = logging.getLogger("data-collector.dg_manager")

_PA_REPLY_LIMIT = 1_000_000


def _parse_period_to_unix(period: dict) -> dict:
    def _to_unix(value) -> int:
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str):
            dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            return int(dt.replace(tzinfo=timezone.utc).timestamp())
        raise ValueError(f"Неподдерживаемый формат периода: {value!r}")

    return {
        "from": _to_unix(period["from"]),
        "to": _to_unix(period["to"]),
    }


class DgSourceManager:
    def __init__(self, nc, config: dict, lifecycle: Lifecycle):
        self.nc = nc
        self.lifecycle = lifecycle

        self.defaults = config.get("dg_defaults", {})
        self.sources = {src["name"]: src for src in config.get("dg_sources", [])}
        self.dt_format = config["parser"]["clickhouse_dt_format"]
        self.max_period_days = config["pa_request"]["max_period_days"]
        self.raw_subject = config["nats"]["raw_subject"]
        self._workers: list[asyncio.Task] = []

        self.client = BaseAsyncHttpClient(
            timeout=self.defaults.get("timeout", 10),
            verify_ssl=self.defaults.get("verify_ssl", False),
        )

    async def _stream_and_process(self,
                                  name: str,
                                  data: dict,
                                  filter_expired: bool = True,
                                  period: dict | None = None,
                                  on_record=None) -> None:
        url = self.defaults.get("url", "")
        headers = self.defaults.get("headers", {})
        payload = {"action": self.defaults.get("action", "list"), "name": name, "data": data}

        last_error = None
        response_cm = None
        response = None

        for attempt in range(1, 4):
            try:
                logger.info("action=execute_request profile=%s attempt=%d", name, attempt)
                response_cm = self.client.post_stream(url, headers=headers, data=payload)
                response = await response_cm.__aenter__()
                response.raise_for_status()
                break
            except Exception as e:
                last_error = e
                if response_cm is not None:
                    await response_cm.__aexit__(type(e), e, e.__traceback__)
                    response_cm = None
                logger.warning(
                    "action=request_failed profile=%s attempt=%d error=%s",
                    name, attempt, str(e)
                )
                if attempt < 3:
                    await asyncio.sleep(1)
        else:
            logger.error(
                "action=request_all_attempts_failed profile=%s error=%s",
                name, str(last_error)
            )
            raise last_error

        content_length = response.headers.get("content-length", "unknown")
        logger.info("action=stream_start profile=%s content_length=%s", name, content_length)

        stats: dict = {}
        records_found = 0
        batches_sent = 0
        batch = []

        try:
            reader = HttpxAsyncReader(response)
            async for record in stream_extract_records(
                    reader,
                    source="dosgate",
                    profile=name,
                    dt_format=self.dt_format,
                    filter_expired=filter_expired,
                    period=period,
                    stats=stats,
            ):
                if self.lifecycle.is_shutting_down:
                    break

                records_found += 1
                batch.append(record)

                if on_record is not None:
                    on_record(record)

                if len(batch) >= PUBLISH_BATCH_SIZE:
                    await publish_records(self.nc, self.raw_subject, batch, self.lifecycle)
                    batch.clear()
                    batches_sent += 1

            if batch:
                await publish_records(self.nc, self.raw_subject, batch, self.lifecycle)
                batches_sent += 1
        finally:
            await response_cm.__aexit__(None, None, None)

        objects_read = stats.get("objects_read", 0)

        if objects_read == 0 and content_length not in ("0", "unknown"):
            logger.warning(
                "action=unexpected_response_shape profile=%s content_length=%s "
                "message='no objects found under marks - dg response shape may have changed'",
                name, content_length,
            )

        logger.info(
            "action=stream_done profile=%s content_length=%s objects_read=%d records_found=%d batches_sent=%d",
            name, content_length, objects_read, records_found, batches_sent,
        )

    def _clamp_period(self, period: dict, profile: str) -> dict:
        period_to = int(period["to"])
        period_from = int(period["from"])
        min_from = period_to - int(self.max_period_days * 86400)
        if period_from >= min_from:
            return period
        logger.info(
            "action=period_clamped profile=%s original_from=%d clamped_from=%d",
            profile, period_from, min_from,
        )
        return {"from": min_from, "to": period_to}

    async def run_automated(self, name: str):
        cfg = self.sources.get(name)
        if not cfg:
            return
        await self._stream_and_process(name=name, data=cfg.get("payload_data", {}))

    async def run_manual(self, payload_from_front: dict):
        ui_params = payload_from_front.get("params", {})

        profile_name = ui_params.get("name", "manual-request")
        front_data_filters = ui_params.get("data", {}).copy()
        filter_expired = ui_params.get("filter_expired", False)

        period = front_data_filters.pop("period", None)
        if period:
            period = self._clamp_period(period, profile_name)

        front_data_filters["type"] = self.defaults.get("type", "shost")
        front_data_filters["value"] = self.defaults.get("value", "1")

        logger.info(
            "action=manual_request_start profile=%s data_keys=%s filter_expired=%s period=%s",
            profile_name,
            list(front_data_filters.keys()),
            filter_expired,
            period,
        )

        await self._stream_and_process(
            name=profile_name,
            data=front_data_filters,
            filter_expired=filter_expired,
            period=period,
        )

    async def run_pa(self, payload_from_simple: dict) -> list[dict]:
        params = payload_from_simple.get("params", {})
        profile_name = params.get("name", "unknown")
        period = params.get("period")
        ip = params.get("ip")

        if period:
            period = self._clamp_period(_parse_period_to_unix(period), profile_name)

        logger.info(
            "action=pa_request_start profile=%s period=%s ip=%s",
            profile_name, period, ip,
        )

        dedup: dict[tuple, dict] = {}
        capped = False

        def _on_record(record: dict) -> None:
            nonlocal capped

            if period and not _is_in_period(record["blocked_at"], period):
                return
            if ip and record["ip_address"] != ip:
                return

            key = (record["ip_address"], record["source"], record["profile"])
            existing = dedup.get(key)
            if existing is not None:
                if record["blocked_at"] < existing["first_detected"]:
                    existing["first_detected"] = record["blocked_at"]
                return

            if len(dedup) >= _PA_REPLY_LIMIT:
                capped = True
                return

            dedup[key] = {
                "ip_address": record["ip_address"],
                "first_detected": record["blocked_at"],
                "source": record["source"],
                "profile": record["profile"],
            }

        await self._stream_and_process(
            name=profile_name,
            data={"id": "2-255", "type": self.defaults.get("type", "shost")},
            filter_expired=False,
            on_record=_on_record,
        )

        logger.info(
            "action=pa_request_done profile=%s reply_count=%d capped=%s",
            profile_name, len(dedup), capped,
        )

        return list(dedup.values())

    async def _worker_loop(self, name: str, interval: int):
        while not self.lifecycle.is_shutting_down:
            try:
                await self.run_automated(name)
            except Exception as e:
                logger.error("action=worker_loop_error profile=%s error=%s", name, str(e))
            await asyncio.sleep(interval)

    async def start(self):
        await self.client.connect()
        for name, cfg in self.sources.items():
            interval = cfg.get("schedule", 0)
            if interval > 0:
                logger.info("action=worker_init profile=%s interval=%ds", name, interval)
                self._workers.append(asyncio.create_task(self._worker_loop(name, interval)))

    async def stop(self):
        for task in self._workers:
            task.cancel()
        await self.client.close()
        logger.info("action=dg_manager_stopped")
