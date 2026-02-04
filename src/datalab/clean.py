from __future__ import annotations

import argparse
import logging
from pathlib import Path

from datalab.cleaning import clean_dataframe
from datalab.config import load_config
from datalab.io import read_input_data
from datalab.logging_utils import setup_logging
from datalab.report import build_quality_report, write_quality_report

logger = logging.getLogger(__name__)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run DataLab cleaning pipeline.")
    parser.add_argument("--input", required=True, help="Input file or directory path.")
    parser.add_argument("--output", required=True, help="Output directory path.")
    parser.add_argument("--config", required=False, help="Optional YAML config path.")
    parser.add_argument("--topk", type=int, default=5, help="Top K categories for report.")
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def run_pipeline(input_path: str, output_path: str, config_path: str | None, topk: int) -> None:
    config = load_config(config_path)
    schema = config.get("schema", {})

    logger.info("Reading raw data from %s", input_path)
    raw_df = read_input_data(input_path)
    logger.info("Loaded %s rows and %s columns", len(raw_df), len(raw_df.columns))

    cleaned = clean_dataframe(raw_df, schema=schema)
    out_dir = Path(output_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    parquet_path = out_dir / "cleaned.parquet"
    cleaned.to_parquet(parquet_path, index=False)
    logger.info("Wrote cleaned parquet: %s", parquet_path)

    report = build_quality_report(cleaned, topk=topk)
    report_path = write_quality_report(report, out_dir)
    logger.info("Wrote quality report: %s", report_path)


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)
    run_pipeline(args.input, args.output, args.config, args.topk)


if __name__ == "__main__":
    main()
