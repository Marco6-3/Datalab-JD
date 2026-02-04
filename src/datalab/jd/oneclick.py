from __future__ import annotations

import argparse
import logging
import re
from pathlib import Path
from urllib.parse import urlparse

from datalab.clean import run_pipeline
from datalab.jd.analyze import generate_jd_market_report
from datalab.jd.crawl import crawl_jobs, write_raw_csv
from datalab.logging_utils import setup_logging

logger = logging.getLogger(__name__)

LIEPIN_SELECTORS = {
    "card": ".job-card-pc-container",
    "url": ".job-detail-box > a[href]",
    "title": ".job-title-box .ellipsis-1",
    "company": ".company-name",
    "city": ".job-dq-box .ellipsis-1",
    "publish_date": ".publish-date, .date",
    "salary_text": ".job-salary",
    "exp_text": ".job-labels-box .labels-tag:nth-of-type(1)",
    "edu_text": ".job-labels-box .labels-tag:nth-of-type(2)",
}


def detect_site(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if "liepin.com" in host:
        return "liepin"
    raise ValueError(
        f"Unsupported website: {host}. Current one-click mode supports liepin.com only."
    )


def build_liepin_seed_url(url: str) -> str:
    if "{page}" in url:
        return url
    base = url.split("?", 1)[0].rstrip("/")
    if re.search(r"/pn\d+$", base):
        return re.sub(r"/pn\d+$", "/pn{page}", base) + "/"
    return f"{base}/pn{{page}}/"


def resolve_crawl_plan(url: str) -> tuple[str, dict[str, str]]:
    site = detect_site(url)
    if site == "liepin":
        return build_liepin_seed_url(url), dict(LIEPIN_SELECTORS)
    raise ValueError(f"No crawl plan available for site: {site}")


def run_one_click(
    url: str,
    pages: int,
    output_dir: str,
    sleep_sec: float,
    timeout_sec: float,
    config_path: str | None,
    topk: int,
) -> dict[str, Path]:
    if pages < 1:
        raise ValueError(f"pages must be >= 1, got {pages}")

    out_dir = Path(output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    raw_csv_path = out_dir / "raw_crawled.csv"
    cleaned_parquet_path = out_dir / "cleaned.parquet"
    quality_report_path = out_dir / "data_quality_report.md"
    market_report_path = out_dir / "jd_market_report.md"

    seed_url, selectors = resolve_crawl_plan(url)
    logger.info("Detected source site and seed URL: %s", seed_url)
    raw_df = crawl_jobs(
        seed_url=seed_url,
        pages=pages,
        sleep_sec=sleep_sec,
        timeout_sec=timeout_sec,
        selectors=selectors,
    )
    write_raw_csv(raw_df, raw_csv_path)
    logger.info("Crawled raw rows: %s", len(raw_df))

    run_pipeline(str(raw_csv_path), str(out_dir), config_path=config_path, topk=topk)
    generate_jd_market_report(cleaned_parquet_path, market_report_path)

    return {
        "raw_csv": raw_csv_path,
        "cleaned_parquet": cleaned_parquet_path,
        "quality_report": quality_report_path,
        "market_report": market_report_path,
    }


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="One-click JD pipeline: crawl from URL, clean, and generate analysis report."
    )
    parser.add_argument("--url", required=True, help="JD list URL (currently supports liepin.com).")
    parser.add_argument("--pages", type=int, default=1, help="How many pages to crawl.")
    parser.add_argument(
        "--output-dir",
        default="data/oneclick",
        help="Output directory for raw data, cleaned parquet and reports.",
    )
    parser.add_argument("--sleep-sec", type=float, default=1.0, help="Sleep seconds between pages.")
    parser.add_argument("--timeout-sec", type=float, default=20.0, help="HTTP timeout per request.")
    parser.add_argument(
        "--config",
        default=None,
        help="Optional clean config YAML. Leave empty for liepin one-click default flow.",
    )
    parser.add_argument("--topk", type=int, default=5, help="Top K values in quality report.")
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)
    outputs = run_one_click(
        url=args.url,
        pages=args.pages,
        output_dir=args.output_dir,
        sleep_sec=args.sleep_sec,
        timeout_sec=args.timeout_sec,
        config_path=args.config,
        topk=args.topk,
    )
    for key, path in outputs.items():
        logger.info("%s: %s", key, path)


if __name__ == "__main__":
    main()
