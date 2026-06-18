import asyncio
import json
import logging
from datetime import datetime, timezone

from project.module_data_collector.http.src2_client import DgClient
from project.module_data_collector.lifecycle import Lifecycle
from project.module_data_collector.parser.parser import stream_extract_records, _is_in_period
from project.utils.http.async_client import HttpxAsyncReader

logger = logging.getLogger("data-collector.dg_manager")

_PUBLISH_BATCH_SIZE = 500
_PA_REPLY_LIMIT = 1_000_000


async def _publish_records(nc,
                           records: list,
                           lifecycle: Lifecycle,
                           subject: str = "ch.write.raw") -> None:
    batch = []
    for record in records:
        if lifecycle.is_shutting_down:
            break
        batch.append(nc.publish(subject, json.dumps(record).encode()))
        if len(batch) >= _PUBLISH_BATCH_SIZE:
            await asyncio.gather(*batch)
            batch.clear()
    if batch:
        await asyncio.gather(*batch)


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
    def __init__(self,
                 nc,
                 config: dict,
                 lifecycle: Lifecycle):
        self.nc = nc
        self.lifecycle = lifecycle

        self.defaults = config.get("dg_defaults", {})
        self.sources = {src["name"]: src for src in config.get("dg_sources", [])}
        self.dt_format = config.get("parser", {}).get("clickhouse_dt_format", "%Y-%m-%dT%H:%M:%S")
        self.max_period_days = config.get("pa_request", {}).get("max_period_days", 7)

        dg_timeout = self.defaults.get("timeout", 10)
        self.client = DgClient(timeout=dg_timeout, verify_ssl=self.defaults.get("verify_ssl", False))

    async def _stream_and_process(self,
                                  name: str,
                                  url: str,
                                  headers: dict,
                                  payload: dict,
                                  filter_expired: bool = True,
                                  period: dict | None = None,
                                  on_record=None) -> dict:

        last_error = None
        response_cm = None
        response = None

        for attempt in range(1, 4):
            try:
                logger.info("action=execute_request profile=%s attempt=%d", name, attempt)
                response_cm = self.client.stream_fetch_data(url, headers, payload)
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
                batch.append(self.nc.publish("ch.write.raw", json.dumps(record).encode()))

                if on_record is not None:
                    on_record(record)

                if len(batch) >= _PUBLISH_BATCH_SIZE:
                    await asyncio.gather(*batch)
                    batch.clear()
                    batches_sent += 1

            if batch:
                await asyncio.gather(*batch)
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

        return {
            "content_length": content_length,
            "objects_read": objects_read,
            "records_found": records_found,
            "batches_sent": batches_sent,
        }

    async def run_automated(self, name: str):
        cfg = self.sources.get(name)
        if not cfg:
            return

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": name,
            "data": cfg.get("payload_data", {})
        }

        await self._stream_and_process(
            name=name,
            url=self.defaults.get("url", ""),
            headers=self.defaults.get("headers", {}),
            payload=final_payload,
        )

    async def run_manual(self, payload_from_front: dict):

        ui_params = payload_from_front.get("params", {})

        profile_name = ui_params.get("name", "manual-request")
        front_data_filters = ui_params.get("data", {}).copy()
        filter_expired = ui_params.get("filter_expired", False)

        period = front_data_filters.pop("period", None)

        if period:
            max_seconds = int(self.max_period_days * 86400)
            period_to = int(period["to"])
            period_from = int(period["from"])
            min_from = period_to - max_seconds
            if period_from < min_from:
                logger.info(
                    "action=period_clamped profile=%s original_from=%d clamped_from=%d",
                    profile_name, period_from, min_from
                )
                period = {"from": min_from, "to": period_to}

        front_data_filters["type"] = self.defaults.get("type", "shost")
        front_data_filters["value"] = self.defaults.get("value", "1")

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": profile_name,
            "data": front_data_filters
        }

        logger.info(
            "action=manual_request_start profile=%s data_keys=%s filter_expired=%s period=%s",
            profile_name,
            list(front_data_filters.keys()),
            filter_expired,
            period,
        )

        await self._stream_and_process(
            name=profile_name,
            url=self.defaults.get("url", ""),
            headers=self.defaults.get("headers", {}),
            payload=final_payload,
            filter_expired=filter_expired,
            period=period,
        )

    async def run_pa(self, payload_from_simple: dict) -> list[dict]:
        profile_name = payload_from_simple.get("params", {}).get("name", "unknown")
        period = payload_from_simple.get("params", {}).get("period", None)
        ip = payload_from_simple.get("params", {}).get("ip", None)

        if period:
            period = _parse_period_to_unix(period)
            max_seconds = int(self.max_period_days * 86400)
            period_to = int(period["to"])
            period_from = int(period["from"])
            min_from = period_to - max_seconds
            if period_from < min_from:
                logger.info(
                    "action=period_clamped profile=%s original_from=%d clamped_from=%d",
                    profile_name, period_from, min_from
                )
                period = {"from": min_from, "to": period_to}

        final_payload = {
            "action": self.defaults.get("action", "list"),
            "name": profile_name,
            "data": {
                "id": "2-255",
                "type": self.defaults.get("type", "shost"),
            },
        }

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

        stats = await self._stream_and_process(
            name=profile_name,
            url=self.defaults.get("url", ""),
            headers=self.defaults.get("headers", {}),
            payload=final_payload,
            filter_expired=False,
            period=None,
            on_record=_on_record,
        )

        logger.info(
            "action=pa_request_done profile=%s content_length=%s objects_read=%d records_found=%d "
            "reply_count=%d capped=%s",
            profile_name, stats["content_length"], stats["objects_read"], stats["records_found"],
            len(dedup), capped,
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
                asyncio.create_task(self._worker_loop(name, interval))

    async def stop(self):
        await self.client.close()
        logger.info("action=dg_manager_stopped")
