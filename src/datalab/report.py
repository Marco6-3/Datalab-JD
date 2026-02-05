from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype


def _render_distribution(series: pd.Series, topk: int) -> str:
    if is_numeric_dtype(series):
        desc = series.describe(percentiles=[0.25, 0.5, 0.75]).to_dict()
        items = ", ".join(f"{k}={round(v, 4)}" for k, v in desc.items() if pd.notna(v))
        return f"numeric stats: {items}"
    if is_datetime64_any_dtype(series):
        return f"min={series.min()}, max={series.max()}"
    top_values = series.astype(str).value_counts(dropna=False).head(topk)
    pairs = ", ".join(f"{idx} ({cnt})" for idx, cnt in top_values.items())
    return f"top{topk}: {pairs}"


def _render_missing_rate_table(missing_rate: dict[str, Any]) -> list[str]:
    headers = "| column | missing_rate |"
    align = "| --- | --- |"
    rows = [headers, align]
    for col, rate in sorted(missing_rate.items()):
        rows.append(f"| {col} | {float(rate):.4f} |")
    return rows


def build_quality_report(
    df: pd.DataFrame, topk: int = 5, metrics: dict[str, Any] | None = None
) -> str:
    lines: list[str] = []
    lines.append("# Data Quality Report")
    lines.append("")
    lines.append("## Overview")
    lines.append(f"- Rows: {len(df)}")
    lines.append(f"- Columns: {len(df.columns)}")
    lines.append("")
    if metrics:
        lines.append("## Metrics Summary")
        lines.append(f"- parse_rate: {float(metrics.get('parse_rate', 0.0)):.4f}")
        lines.append(f"- negotiable_rate: {float(metrics.get('negotiable_rate', 0.0)):.4f}")
        lines.append(f"- duplicates_rate: {float(metrics.get('duplicates_rate', 0.0)):.4f}")
        lines.append("")
        lines.append("### Key Column Missing Rate")
        lines.extend(_render_missing_rate_table(metrics.get("missing_rate", {})))
        lines.append("")
    lines.append("## Column Details")
    for col in df.columns:
        series = df[col]
        missing_rate = series.isna().mean() * 100
        dtype = str(series.dtype)
        dist = _render_distribution(series, topk=topk)
        lines.append(f"### `{col}`")
        lines.append(f"- dtype: {dtype}")
        lines.append(f"- missing_rate: {missing_rate:.2f}%")
        lines.append(f"- distribution: {dist}")
        lines.append("")
    return "\n".join(lines)


def write_quality_report(report_text: str, output_dir: str | Path) -> Path:
    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    report_path = out_dir / "data_quality_report.md"
    report_path.write_text(report_text, encoding="utf-8")
    return report_path
