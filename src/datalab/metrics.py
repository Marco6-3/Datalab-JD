from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

KEY_COLUMNS = [
    "url",
    "title",
    "company",
    "city",
    "salary_min_k",
    "salary_max_k",
    "salary_months",
    "salary_is_negotiable",
    "exp_min_years",
    "exp_max_years",
    "edu_level",
]


def _as_rate(value: float) -> float:
    return round(float(min(max(value, 0.0), 1.0)), 6)


def _compute_parse_rate(df: pd.DataFrame) -> float:
    if len(df) == 0:
        return 0.0
    salary_ok = (
        df.get("salary_min_k", pd.Series([pd.NA] * len(df))).notna()
        | df.get("salary_max_k", pd.Series([pd.NA] * len(df))).notna()
    )
    exp_ok = (
        df.get("exp_min_years", pd.Series([pd.NA] * len(df))).notna()
        | df.get("exp_max_years", pd.Series([pd.NA] * len(df))).notna()
    )
    edu_series = df.get("edu_level", pd.Series([pd.NA] * len(df))).astype("string").str.lower()
    edu_ok = edu_series.notna() & ~edu_series.isin(["unknown", "other", ""])
    return _as_rate((salary_ok.mean() + exp_ok.mean() + edu_ok.mean()) / 3)


def compute_metrics(raw_df: pd.DataFrame, cleaned_df: pd.DataFrame) -> dict[str, Any]:
    raw_rows = int(len(raw_df))
    cleaned_rows = int(len(cleaned_df))

    missing_rate: dict[str, float] = {}
    for col in KEY_COLUMNS:
        if col in cleaned_df.columns:
            rate = cleaned_df[col].isna().mean()
        else:
            rate = 1.0
        missing_rate[col] = _as_rate(rate)

    negotiable_rate = 0.0
    if "salary_is_negotiable" in cleaned_df.columns and cleaned_rows > 0:
        series = cleaned_df["salary_is_negotiable"].astype("boolean")
        negotiable_rate = _as_rate(series.fillna(False).mean())

    duplicates_rate = 0.0
    if raw_rows > 0:
        duplicates_rate = _as_rate(max(raw_rows - cleaned_rows, 0) / raw_rows)

    return {
        "row_count_raw": raw_rows,
        "row_count_cleaned": cleaned_rows,
        "parse_rate": _as_rate(_compute_parse_rate(cleaned_df)),
        "negotiable_rate": negotiable_rate,
        "duplicates_rate": duplicates_rate,
        "missing_rate": missing_rate,
    }


def write_metrics(metrics: dict[str, Any], output_dir: str | Path) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "metrics.json"
    path.write_text(json.dumps(metrics, ensure_ascii=True, indent=2), encoding="utf-8")
    return path
