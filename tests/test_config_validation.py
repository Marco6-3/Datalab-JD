from pathlib import Path

import pytest

from datalab.config import (
    ConfigValidationError,
    load_app_config,
    resolve_section_config,
)


def test_resolve_section_config_success_and_cli_override(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
clean:
  input: data/raw
  output: data/clean_from_config
  topk: 3
  log_level: INFO
""".strip(),
        encoding="utf-8",
    )
    resolved = resolve_section_config(
        "clean",
        app_config_path=str(cfg),
        cli_values={"output": "data/clean_override", "input": None},
        required_keys={"input", "output"},
    )
    assert resolved["input"] == "data/raw"
    assert resolved["output"] == "data/clean_override"
    assert int(resolved["topk"]) == 3


def test_config_validation_error_on_missing_required(tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text("clean: {}", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="Missing required 'input'"):
        resolve_section_config(
            "clean",
            app_config_path=str(cfg),
            cli_values={},
            required_keys={"input"},
        )


def test_load_app_config_unknown_section_raises(tmp_path: Path):
    cfg = tmp_path / "bad.yaml"
    cfg.write_text("unexpected: {}", encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="Unknown config section"):
        load_app_config(str(cfg))


def test_env_override_applies(monkeypatch: pytest.MonkeyPatch, tmp_path: Path):
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        """
crawl:
  seed_url: https://example.com/page={page}
  output: data/raw/a.csv
  pages: 1
""".strip(),
        encoding="utf-8",
    )
    monkeypatch.setenv("DATALAB_CRAWL_PAGES", "4")
    resolved = resolve_section_config(
        "crawl",
        app_config_path=str(cfg),
        cli_values={},
        required_keys={"seed_url", "output"},
    )
    assert int(resolved["pages"]) == 4
