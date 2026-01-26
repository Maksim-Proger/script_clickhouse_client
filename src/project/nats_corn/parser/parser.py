import json
import re
from datetime import datetime
from typing import Any, List

IP_REGEX = re.compile(
    r'\b(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}\b'
)

def _extract_records(obj: Any, result: List[dict], source: str, profile: str) -> None:
    if isinstance(obj, dict):
        ip_candidates = []
        for k, v in obj.items():
            if isinstance(v, str):
                ip_candidates.extend(IP_REGEX.findall(v))

        blocked_at_raw = obj.get("blocked_at") or obj.get("date") or obj.get("timestamp")
        blocked_at = _parse_datetime(blocked_at_raw)

        for ip in ip_candidates:
            result.append({
                "ip_address": ip,
                "blocked_at": blocked_at,
                "source": source,
                "profile": obj.get("profile", profile)
            })

        for v in obj.values():
            _extract_records(v, result, source, profile)

    elif isinstance(obj, list):
        for item in obj:
            _extract_records(item, result, source, profile)

def _parse_datetime(value: Any) -> datetime:
    if isinstance(value, str):
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                pass
    return datetime.utcnow()

def parse_input(data: str, source: str, profile: str = "") -> List[dict]:
    if not data or not data.strip():
        return []

    records: List[dict] = []

    try:
        parsed = json.loads(data)
        _extract_records(parsed, records, source, profile)
    except Exception:
        for ip in IP_REGEX.findall(data):
            records.append({
                "ip_address": ip,
                "blocked_at": datetime.utcnow(),
                "source": source,
                "profile": profile
            })

    return records
