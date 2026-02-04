from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


def load_config(config_path: str | None = None) -> dict[str, Any]:
    """
    Load .env and YAML config.

    YAML is optional; if omitted, defaults are used.
    """
    load_dotenv()
    if not config_path:
        return {}

    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Config file not found: {path}")

    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ValueError("Config root must be a mapping/object.")
    return data
