from __future__ import annotations

import argparse
import logging
from pathlib import Path
from typing import Any

import pandas as pd

from datalab.config import ConfigValidationError, resolve_section_config
from datalab.logging_utils import setup_logging

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = [
    "city",
    "salary_min_k",
    "salary_max_k",
    "salary_months",
    "salary_is_negotiable",
    "exp_min_years",
    "exp_max_years",
    "url",
    "title",
    "company",
]

EXPERIENCE_BUCKETS = ["0-1y", "1-3y", "3-5y", "5-10y", "10y+", "unknown"]


def _is_missing(value: Any) -> bool:
    return value is None or pd.isna(value)


def _as_float(value: Any) -> float | None:
    if _is_missing(value):
        return None
    return float(value)


def _as_bool(value: Any) -> bool | None:
    if _is_missing(value):
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        if value == 1:
            return True
        if value == 0:
            return False
    text = str(value).strip().lower()
    if text in {"true", "1", "yes", "y", "t"}:
        return True
    if text in {"false", "0", "no", "n", "f"}:
        return False
    raise ValueError(f"Invalid salary_is_negotiable value: {value}")


def bucket_experience(exp_min_years: Any, exp_max_years: Any) -> str:
    min_years = _as_float(exp_min_years)
    max_years = _as_float(exp_max_years)
    if min_years is None and max_years is None:
        return "unknown"

    values = [v for v in [min_years, max_years] if v is not None]
    representative = sum(values) / len(values)
    representative = max(0.0, representative)
    if representative <= 1:
        return "0-1y"
    if representative <= 3:
        return "1-3y"
    if representative <= 5:
        return "3-5y"
    if representative <= 10:
        return "5-10y"
    return "10y+"


def compute_mid_k(salary_min_k: Any, salary_max_k: Any, salary_months: Any) -> float | None:
    min_k = _as_float(salary_min_k)
    max_k = _as_float(salary_max_k)
    if min_k is None and max_k is None:
        return None

    if min_k is None:
        mid_k = max_k
    elif max_k is None:
        mid_k = min_k
    else:
        mid_k = (min_k + max_k) / 2

    months = _as_float(salary_months)
    if months is None or months <= 0:
        months = 12.0
    return float(mid_k) * months / 12.0


def ensure_required_columns(df: pd.DataFrame) -> None:
    missing = [col for col in REQUIRED_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(
            "Missing required columns for JD analysis: " + ", ".join(sorted(missing))
        )


def city_exp_summary(df: pd.DataFrame) -> pd.DataFrame:
    work = df.copy()
    if "mid_k" not in work.columns:
        work["mid_k"] = work.apply(
            lambda row: compute_mid_k(
                row.get("salary_min_k"),
                row.get("salary_max_k"),
                row.get("salary_months"),
            ),
            axis=1,
        )
    if "exp_bucket" not in work.columns:
        work["exp_bucket"] = work.apply(
            lambda row: bucket_experience(row.get("exp_min_years"), row.get("exp_max_years")),
            axis=1,
        )

    work["negotiable_bool"] = work["salary_is_negotiable"].apply(_as_bool)
    work["negotiable_float"] = work["negotiable_bool"].astype("float64")
    grouped = (
        work.groupby(["city", "exp_bucket"], dropna=False)
        .agg(
            n_jobs=("url", "size"),
            p50_mid_k=("mid_k", "median"),
            p90_mid_k=("mid_k", lambda s: s.quantile(0.9)),
            negotiable_rate=("negotiable_float", "mean"),
        )
        .reset_index()
    )

    bucket_order = {bucket: idx for idx, bucket in enumerate(EXPERIENCE_BUCKETS)}
    grouped["bucket_order"] = grouped["exp_bucket"].map(lambda x: bucket_order.get(x, 999))
    grouped = grouped.sort_values(["city", "bucket_order"]).drop(columns=["bucket_order"])

    grouped["p50_mid_k"] = grouped["p50_mid_k"].round(2)
    grouped["p90_mid_k"] = grouped["p90_mid_k"].round(2)
    grouped["negotiable_rate"] = grouped["negotiable_rate"].round(4)
    return grouped


def _format_cell(value: Any) -> str:
    if pd.isna(value):
        return "-"
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _render_table(headers: list[str], rows: list[list[Any]]) -> str:
    if not rows:
        return "No data."
    header_line = "| " + " | ".join(headers) + " |"
    align_line = "| " + " | ".join(["---"] * len(headers)) + " |"
    body = []
    for row in rows:
        cells = [(_format_cell(v)).replace("|", "\\|") for v in row]
        body.append("| " + " | ".join(cells) + " |")
    return "\n".join([header_line, align_line, *body])


def build_jd_market_report(df: pd.DataFrame) -> str:
    work = df.copy()
    if "raw_salary_text" not in work.columns:
        work["raw_salary_text"] = work.get("salary_text", pd.Series(["UNKNOWN"] * len(work)))
    if "fetched_at" not in work.columns:
        work["fetched_at"] = "UNKNOWN"
    work["mid_k"] = work.apply(
        lambda row: compute_mid_k(
            row.get("salary_min_k"),
            row.get("salary_max_k"),
            row.get("salary_months"),
        ),
        axis=1,
    )
    work["exp_bucket"] = work.apply(
        lambda row: bucket_experience(row.get("exp_min_years"), row.get("exp_max_years")),
        axis=1,
    )
    work["negotiable_bool"] = work["salary_is_negotiable"].apply(_as_bool)

    total_jobs = int(len(work))
    unique_companies = int(work["company"].nunique(dropna=True))
    unique_cities = int(work["city"].nunique(dropna=True))
    jobs_with_salary = int(work["mid_k"].notna().sum())
    negotiable_rate = float(work["negotiable_bool"].mean()) if total_jobs else 0.0

    summary = city_exp_summary(work)
    summary_rows = [
        [
            row["city"],
            row["exp_bucket"],
            int(row["n_jobs"]),
            row["p50_mid_k"],
            row["p90_mid_k"],
            row["negotiable_rate"],
        ]
        for _, row in summary.iterrows()
    ]

    top_jobs = (
        work[work["mid_k"].notna()]
        .sort_values("mid_k", ascending=False)
        .head(20)
        .reset_index(drop=True)
    )
    top_rows = [
        [
            idx + 1,
            row["mid_k"],
            row["city"],
            row["title"],
            row["company"],
            row["url"],
            row.get("fetched_at", "UNKNOWN"),
            row.get("raw_salary_text", "UNKNOWN"),
        ]
        for idx, (_, row) in enumerate(top_jobs.iterrows())
    ]

    skill_rows: list[list[Any]] = []
    if "skill_tags" in work.columns:
        skill = work[["city", "exp_bucket", "skill_tags"]].copy()
        skill["skill_tags"] = skill["skill_tags"].fillna("").astype(str)
        skill = skill.assign(skill_tag=skill["skill_tags"].str.split("|")).explode("skill_tag")
        skill = skill[skill["skill_tag"].astype(str).str.len() > 0]
        if not skill.empty:
            skill_heat = (
                skill.groupby(["city", "exp_bucket", "skill_tag"], dropna=False)
                .size()
                .reset_index(name="n_jobs")
                .sort_values("n_jobs", ascending=False)
                .head(50)
            )
            skill_rows = [
                [row["city"], row["exp_bucket"], row["skill_tag"], int(row["n_jobs"])]
                for _, row in skill_heat.iterrows()
            ]

    lines = [
        "# JD Market Report",
        "",
        "## 1) Sample Overview",
        f"- total_jobs: {total_jobs}",
        f"- unique_companies: {unique_companies}",
        f"- unique_cities: {unique_cities}",
        f"- jobs_with_salary_mid_k: {jobs_with_salary}",
        f"- overall_negotiable_rate: {negotiable_rate:.4f}",
        "",
        "## 2) City x Experience Table",
        _render_table(
            ["city", "exp_bucket", "n_jobs", "p50_mid_k", "p90_mid_k", "negotiable_rate"],
            summary_rows,
        ),
        "",
        "## 3) Top20 High-Paying Jobs",
        _render_table(
            ["rank", "mid_k", "city", "title", "company", "url", "fetched_at", "raw_salary_text"],
            top_rows,
        ),
        "",
        "## 4) Skill Heatmap by City x Experience",
        _render_table(["city", "exp_bucket", "skill_tag", "n_jobs"], skill_rows),
        "",
    ]
    return "\n".join(lines)


def generate_jd_market_report(input_path: str | Path, output_path: str | Path) -> Path:
    in_path = Path(input_path)
    out_path = Path(output_path)
    logger.info("Reading parquet from %s", in_path)
    df = pd.read_parquet(in_path)
    ensure_required_columns(df)
    report = build_jd_market_report(df)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    logger.info("Wrote JD market report: %s", out_path)
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate JD market analysis report from parquet.")
    parser.add_argument("--config", required=False, help="Optional app config YAML path.")
    parser.add_argument("--input", required=False, help="Input cleaned parquet path.")
    parser.add_argument("--output", required=False, help="Output markdown report path.")
    parser.add_argument(
        "--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        resolved = resolve_section_config(
            "analyze",
            app_config_path=args.config,
            cli_values={
                "input": args.input,
                "output": args.output,
                "log_level": args.log_level,
            },
            required_keys={"input", "output"},
        )
        setup_logging(str(resolved.get("log_level", "INFO")))
        generate_jd_market_report(str(resolved["input"]), str(resolved["output"]))
    except ConfigValidationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
