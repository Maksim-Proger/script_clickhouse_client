from pathlib import Path
from typing import Any, Dict

import yaml


def load_yaml(file_path: str) -> Dict[str, Any]:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {file_path}")
    with path.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)
