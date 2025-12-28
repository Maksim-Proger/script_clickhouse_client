import json
import re
from typing import Any, List


# IPv4 строгий шаблон
IP_REGEX = re.compile(
    r'\b(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)'
    r'(?:\.(?:25[0-5]|2[0-4]\d|1\d{2}|[1-9]?\d)){3}\b'
)

# JSON-блоки: {}, []
JSON_BLOCK_REGEX = re.compile(
    r'(\{.*?\}|\[.*?\])',
    re.DOTALL
)


def _extract_ips_from_text(text: str) -> List[str]:
    """Извлекает IP из произвольного текста"""
    return IP_REGEX.findall(text)


def _extract_ips_from_json(obj: Any) -> List[str]:
    """Рекурсивно извлекает IP из JSON-структуры"""
    ips: List[str] = []

    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "ip" and isinstance(value, str):
                ips.append(value)
            else:
                ips.extend(_extract_ips_from_json(value))

    elif isinstance(obj, list):
        for item in obj:
            ips.extend(_extract_ips_from_json(item))

    return ips


def parse_input(data: str) -> List[str]:
    """
    Основная точка входа.
    Принимает смешанный вход:
    - обычный текст
    - JSON-объекты
    - JSON-массивы
    Возвращает список IP-адресов
    """
    if not data or not data.strip():
        return []

    result: List[str] = []

    # 1. Извлекаем и обрабатываем все JSON-блоки
    for match in JSON_BLOCK_REGEX.finditer(data):
        block = match.group(0)

        try:
            parsed = json.loads(block)
            result.extend(_extract_ips_from_json(parsed))
        except json.JSONDecodeError:
            # если вдруг кривой JSON — игнорируем
            continue

    # 2. Убираем JSON-блоки из текста
    cleaned_text = JSON_BLOCK_REGEX.sub(" ", data)

    # 3. Извлекаем IP из оставшегося текста
    result.extend(_extract_ips_from_text(cleaned_text))

    return result
