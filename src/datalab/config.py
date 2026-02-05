from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml
from dotenv import load_dotenv


DEFAULT_APP_CONFIG_PATH = "config/config.yaml"
VALID_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR"}
KNOWN_SECTIONS = {"clean", "crawl", "analyze", "oneclick"}


class ConfigValidationError(ValueError):
    """Raised when YAML/env configuration is invalid."""


def _parse_scalar(value: str) -> Any:
    lowered = value.strip().lower()
    if lowered in {"true", "false"}:
        return lowered == "true"
    try:
        if "." in value:
            return float(value)
        return int(value)
    except ValueError:
        return value


def _load_yaml(config_path: str | None) -> dict[str, Any]:
    if not config_path:
        return {}
    path = Path(config_path)
    if not path.exists():
        raise ConfigValidationError(f"Config file not found: {path}")
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    if not isinstance(data, dict):
        raise ConfigValidationError("Config root must be a mapping/object.")
    return data


def _apply_env_overrides(section: str, data: dict[str, Any]) -> dict[str, Any]:
    out = dict(data)
    prefix = f"DATALAB_{section.upper()}_"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        config_key = key[len(prefix) :].lower()
        out[config_key] = _parse_scalar(value)
    return out


def _validate_section_types(section: str, values: dict[str, Any]) -> None:
    if "log_level" in values and str(values["log_level"]).upper() not in VALID_LOG_LEVELS:
        raise ConfigValidationError(
            f"Invalid log_level for section '{section}': {values['log_level']}. "
            f"Expected one of {sorted(VALID_LOG_LEVELS)}."
        )
    for int_key in ("pages", "topk"):
        if int_key in values and values[int_key] is not None:
            try:
                ivalue = int(values[int_key])
            except Exception as exc:
                raise ConfigValidationError(
                    f"Invalid '{int_key}' for section '{section}': {values[int_key]}"
                ) from exc
            if ivalue < 1:
                raise ConfigValidationError(
                    f"'{int_key}' must be >= 1 for section '{section}', got {ivalue}."
                )
    for float_key in ("sleep_sec", "timeout_sec"):
        if float_key in values and values[float_key] is not None:
            try:
                fvalue = float(values[float_key])
            except Exception as exc:
                raise ConfigValidationError(
                    f"Invalid '{float_key}' for section '{section}': {values[float_key]}"
                ) from exc
            if fvalue < 0:
                raise ConfigValidationError(
                    f"'{float_key}' must be >= 0 for section '{section}', got {fvalue}."
                )


def load_app_config(config_path: str | None = None) -> dict[str, Any]:
    """
    Load app config from YAML and .env environment variables.

    Missing config file is allowed when config_path is None.
    """
    load_dotenv()
    path = config_path
    if path is None and Path(DEFAULT_APP_CONFIG_PATH).exists():
        path = DEFAULT_APP_CONFIG_PATH

    data = _load_yaml(path)
    for key in data.keys():
        if key not in KNOWN_SECTIONS and key != "schema":
            raise ConfigValidationError(
                f"Unknown config section '{key}'. Expected one of {sorted(KNOWN_SECTIONS)}."
            )
        if key in KNOWN_SECTIONS and not isinstance(data[key], dict):
            raise ConfigValidationError(f"Section '{key}' must be a mapping/object.")
    return data


def resolve_section_config(
    section: str,
    *,
    app_config_path: str | None,
    cli_values: dict[str, Any],
    required_keys: set[str] | None = None,
) -> dict[str, Any]:
    if section not in KNOWN_SECTIONS:
        raise ConfigValidationError(f"Unknown section: {section}")

    app_config = load_app_config(app_config_path)
    section_data = app_config.get(section, {})
    if not isinstance(section_data, dict):
        raise ConfigValidationError(f"Section '{section}' must be a mapping/object.")

    merged = dict(section_data)
    merged = _apply_env_overrides(section, merged)
    for key, value in cli_values.items():
        if value is not None:
            merged[key] = value

    _validate_section_types(section, merged)
    for key in required_keys or set():
        value = merged.get(key)
        if value is None or str(value).strip() == "":
            raise ConfigValidationError(
                f"Missing required '{key}' for section '{section}'. "
                f"Provide CLI arg, config file value, or env override."
            )
    return merged


def _is_legacy_schema_config(data: dict[str, Any]) -> bool:
    if "schema" not in data:
        return False
    non_schema_keys = [k for k in data.keys() if k != "schema"]
    return len(non_schema_keys) == 0


def load_schema_config(config_path: str | None, app_config_path: str | None = None) -> dict[str, Any]:
    """
    Backward-compatible schema loading.

    Priority:
    1) legacy schema YAML path in `config_path` (root has `schema`)
    2) app config `clean.schema` section from `app_config_path`
    """
    if config_path:
        raw = _load_yaml(config_path)
        if _is_legacy_schema_config(raw):
            schema = raw.get("schema", {})
            if not isinstance(schema, dict):
                raise ConfigValidationError("Legacy schema config field 'schema' must be a mapping.")
            return schema

    app_cfg = load_app_config(app_config_path)
    clean_cfg = app_cfg.get("clean", {})
    schema = clean_cfg.get("schema", {})
    if schema is None:
        return {}
    if not isinstance(schema, dict):
        raise ConfigValidationError("App config 'clean.schema' must be a mapping.")
    return schema
