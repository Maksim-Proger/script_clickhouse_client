import json
import re
from typing import Any, List

IP_REGEX = re.compile(
    r'\b(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}\b'
)


def _extract_ips_from_json(obj: Any, result: List[str]) -> None:
    if isinstance(obj, dict):
        for value in obj.values():
            _extract_ips_from_json(value, result)

    elif isinstance(obj, list):
        for item in obj:
            _extract_ips_from_json(item, result)

    elif isinstance(obj, str):
        result.extend(IP_REGEX.findall(obj))


def parse_input(data: str) -> List[str]:
    if not data or not data.strip():
        return []

    result: List[str] = []

    result.extend(IP_REGEX.findall(data))

    try:
        parsed = json.loads(data)
        _extract_ips_from_json(parsed, result)
    except Exception:
        pass

    return result
