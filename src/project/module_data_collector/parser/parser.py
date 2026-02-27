import json
import re
from datetime import datetime, timezone, timedelta  # Добавили timedelta
from typing import Any, List

IP_REGEX = re.compile(
    r'\b(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}\b'
)


def _extract_records(obj: Any,
                     result: List[dict],
                     source: str,
                     profile: str,
                     dt_format: str) -> None:
    if isinstance(obj, dict):
        ip_candidates: List[str] = []

        for k, v in obj.items():
            if k == "l3_src" and isinstance(v, str):
                ip_candidates.append(v)
            elif isinstance(v, str):
                ip_candidates.extend(IP_REGEX.findall(v))

        blocked_at_raw = obj.get("blocked_at") or obj.get("date") or obj.get("timestamp")

        if blocked_at_raw:
            blocked_at = _parse_datetime(blocked_at_raw, dt_format)
        elif "age" in obj:
            try:
                age_seconds = int(obj["age"])
                dt = datetime.now(timezone.utc) - timedelta(seconds=age_seconds)
                blocked_at = dt.strftime(dt_format)
            except (ValueError, TypeError):
                blocked_at = datetime.now(timezone.utc).strftime(dt_format)
        else:
            blocked_at = datetime.now(timezone.utc).strftime(dt_format)

        for ip in ip_candidates:
            result.append({
                "ip_address": ip,
                "blocked_at": blocked_at,
                "source": source,
                "profile": obj.get("profile", profile)
            })

        for v in obj.values():
            _extract_records(v, result, source, profile, dt_format)

    elif isinstance(obj, list):
        for item in obj:
            _extract_records(item, result, source, profile, dt_format)


def _parse_datetime(value: Any, dt_format: str) -> str:
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt).strftime(dt_format)
            except ValueError:
                pass
    return datetime.now(timezone.utc).strftime(dt_format)


def parse_input(data: str,
                source: str,
                profile: str = "",
                dt_format: str = "%Y-%m-%d %H:%M:%S") -> List[dict]:
    if not data or not data.strip():
        return []

    records: List[dict] = []
    try:
        parsed = json.loads(data)
        _extract_records(parsed, records, source, profile, dt_format)
    except Exception:
        now = datetime.now(timezone.utc).strftime(dt_format)
        for ip in IP_REGEX.findall(data):
            records.append({
                "ip_address": ip,
                "blocked_at": now,
                "source": source,
                "profile": profile
            })
    return records
