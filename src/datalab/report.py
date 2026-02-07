from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd
from pandas.api.types import is_datetime64_any_dtype, is_numeric_dtype


def _render_markdown_table(headers: list[str], rows: list[list[Any]]) -> list[str]:
    header = "| " + " | ".join(headers) + " |"
    align = "| " + " | ".join(["---"] * len(headers)) + " |"
    lines = [header, align]
    for row in rows:
        cells = [str(cell).replace("|", "\\|") for cell in row]
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _render_distribution(series: pd.Series, topk: int) -> str:
    if is_numeric_dtype(series):
        desc = series.describe(percentiles=[0.5, 0.9]).to_dict()
        wanted = ["min", "50%", "90%", "max"]
        items = []
        for key in wanted:
            value = desc.get(key)
            if value is not None and pd.notna(value):
                items.append(f"{key}={round(float(value), 4)}")
        return "numeric stats: " + ", ".join(items)
    if is_datetime64_any_dtype(series):
        return f"min={series.min()}, max={series.max()}"
    top_values = series.astype(str).value_counts(dropna=False).head(topk)
    pairs = "; ".join(f"{idx} ({cnt})" for idx, cnt in top_values.items())
    return f"top{topk}: {pairs}"


def _render_missing_rate_table(missing_rate: dict[str, Any]) -> list[str]:
    rows: list[list[Any]] = []
    for col, rate in sorted(missing_rate.items(), key=lambda x: x[1], reverse=True):
        rows.append([col, f"{float(rate):.2%}"])
    return _render_markdown_table(["column", "missing_rate"], rows)


def build_quality_report(
    df: pd.DataFrame, topk: int = 5, metrics: dict[str, Any] | None = None
) -> str:
    lines: list[str] = [
        "# Data Quality Report",
        "",
        "## Contents",
        "- [Overview](#overview)",
        "- [Metrics Summary](#metrics-summary)",
        "- [Column Details](#column-details)",
        "",
        "## Overview",
    ]
    overview_rows = [
        ["Rows", len(df)],
        ["Columns", len(df.columns)],
    ]
    lines.extend(_render_markdown_table(["item", "value"], overview_rows))
    lines.append("")
    if metrics:
        lines.append("## Metrics Summary")
        metric_rows = [
            ["row_count_raw", int(metrics.get("row_count_raw", 0))],
            ["row_count_cleaned", int(metrics.get("row_count_cleaned", 0))],
            ["parse_rate", f"{float(metrics.get('parse_rate', 0.0)):.2%}"],
            ["negotiable_rate", f"{float(metrics.get('negotiable_rate', 0.0)):.2%}"],
            ["duplicates_rate", f"{float(metrics.get('duplicates_rate', 0.0)):.2%}"],
        ]
        lines.extend(_render_markdown_table(["metric", "value"], metric_rows))
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
