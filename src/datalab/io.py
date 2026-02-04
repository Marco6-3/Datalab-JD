from __future__ import annotations

from pathlib import Path

import pandas as pd

from datalab.exceptions import DataReadError

SUPPORTED_SUFFIXES = {".csv", ".jsonl", ".xlsx", ".xls"}


def discover_input_files(input_path: str | Path) -> list[Path]:
    base = Path(input_path)
    if base.is_file():
        return [base] if base.suffix.lower() in SUPPORTED_SUFFIXES else []
    if not base.exists():
        return []
    return sorted(
        p for p in base.rglob("*") if p.is_file() and p.suffix.lower() in SUPPORTED_SUFFIXES
    )


def read_single_file(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".jsonl":
        return pd.read_json(path, lines=True)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise DataReadError(f"Unsupported file type: {path}")


def read_input_data(input_path: str | Path) -> pd.DataFrame:
    files = discover_input_files(input_path)
    if not files:
        raise DataReadError(f"No supported files found under: {input_path}")

    frames: list[pd.DataFrame] = []
    for file_path in files:
        frame = read_single_file(file_path)
        frame["__source_file"] = file_path.name
        frames.append(frame)
    return pd.concat(frames, ignore_index=True, sort=False)
