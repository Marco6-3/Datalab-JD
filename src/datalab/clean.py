from __future__ import annotations

import argparse
import logging
from pathlib import Path

from datalab.cleaning import clean_dataframe
from datalab.config import ConfigValidationError, load_schema_config, resolve_section_config
from datalab.io import read_input_data
from datalab.logging_utils import setup_logging
from datalab.metrics import compute_metrics, write_metrics
from datalab.report import build_quality_report, write_quality_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DataLab cleaning pipeline.")
    parser.add_argument("--input", required=False, help="Input file or directory path.")
    parser.add_argument("--output", required=False, help="Output directory path.")
    parser.add_argument(
        "--config",
        required=False,
        help="Optional app config YAML path. Backward compatible with legacy schema YAML.",
    )
    parser.add_argument("--topk", type=int, default=None, help="Top K categories for report.")
    parser.add_argument(
        "--schema-config",
        required=False,
        help="Optional legacy schema YAML path (root must contain `schema`).",
    )
    parser.add_argument(
        "--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def run_pipeline(
    input_path: str,
    output_path: str,
    schema: dict[str, object] | None,
    topk: int,
    skill_dictionary: dict[str, list[str]] | None = None,
) -> None:
    logger.info("Reading raw data from %s", input_path)
    raw_df = read_input_data(input_path)
    logger.info("Loaded %s rows and %s columns", len(raw_df), len(raw_df.columns))

    cleaned = clean_dataframe(raw_df, schema=schema or {}, skill_dictionary=skill_dictionary)
    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = out_dir / "cleaned.parquet"
    cleaned.to_parquet(parquet_path, index=False)
    logger.info("Wrote cleaned parquet: %s", parquet_path)

    metrics = compute_metrics(raw_df=raw_df, cleaned_df=cleaned)
    metrics_path = write_metrics(metrics, out_dir)
    logger.info("Wrote metrics json: %s", metrics_path)

    report = build_quality_report(cleaned, topk=topk, metrics=metrics)
    report_path = write_quality_report(report, out_dir)
    logger.info("Wrote quality report: %s", report_path)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        resolved = resolve_section_config(
            "clean",
            app_config_path=args.config,
            cli_values={
                "input": args.input,
                "output": args.output,
                "topk": args.topk,
                "log_level": args.log_level,
            },
            required_keys={"input", "output"},
        )
        setup_logging(str(resolved.get("log_level", "INFO")))
        schema = load_schema_config(args.schema_config or args.config, app_config_path=args.config)
        skill_dictionary = resolved.get("skill_dictionary")
        run_pipeline(
            input_path=str(resolved["input"]),
            output_path=str(resolved["output"]),
            schema=schema,
            topk=int(resolved.get("topk", 5)),
            skill_dictionary=skill_dictionary if isinstance(skill_dictionary, dict) else None,
        )
    except ConfigValidationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
