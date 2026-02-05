from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any
from urllib.parse import parse_qsl, urlencode, urljoin, urlparse, urlunparse

import pandas as pd
import requests
from bs4 import BeautifulSoup, Tag

from datalab.config import ConfigValidationError, resolve_section_config
from datalab.logging_utils import setup_logging

logger = logging.getLogger(__name__)

RAW_COLUMNS = [
    "url",
    "title",
    "company",
    "city",
    "publish_date",
    "salary_text",
    "exp_text",
    "edu_text",
]

DEFAULT_SELECTORS = {
    "card": ".job-card",
    "url": "a[href]",
    "title": ".job-title, .title, a[href]",
    "company": ".company-name, .company",
    "city": ".job-city, .city",
    "publish_date": ".publish-date, .date",
    "salary_text": ".salary, .job-salary",
    "exp_text": ".exp, .experience",
    "edu_text": ".edu, .education",
}

SITE_SELECTOR_PRESETS = {
    "liepin.com": {
        "card": ".job-card-pc-container",
        "url": ".job-detail-box > a[href]",
        "title": ".job-title-box .ellipsis-1",
        "company": ".company-name",
        "city": ".job-dq-box .ellipsis-1",
        "salary_text": ".job-salary",
        "exp_text": ".job-labels-box .labels-tag:nth-of-type(1)",
        "edu_text": ".job-labels-box .labels-tag:nth-of-type(2)",
    }
}


def build_page_url(seed_url: str, page: int) -> str:
    if "{page}" in seed_url:
        return seed_url.format(page=page)

    parsed = urlparse(seed_url)
    query = dict(parse_qsl(parsed.query))
    query["page"] = str(page)
    return urlunparse(parsed._replace(query=urlencode(query)))


def _select_text(card: Tag, selector: str) -> str:
    node = card.select_one(selector)
    return node.get_text(" ", strip=True) if node else ""


def _select_url(card: Tag, selector: str, page_url: str) -> str:
    node = card.select_one(selector)
    if not node:
        return ""
    href = node.get("href", "").strip()
    return urljoin(page_url, href) if href else ""


def extract_jobs_from_html(html: str, page_url: str, selectors: dict[str, str]) -> list[dict[str, str]]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(selectors["card"])
    rows: list[dict[str, str]] = []
    for card in cards:
        rows.append(
            {
                "url": _select_url(card, selectors["url"], page_url),
                "title": _select_text(card, selectors["title"]),
                "company": _select_text(card, selectors["company"]),
                "city": _select_text(card, selectors["city"]),
                "publish_date": _select_text(card, selectors["publish_date"]),
                "salary_text": _select_text(card, selectors["salary_text"]),
                "exp_text": _select_text(card, selectors["exp_text"]),
                "edu_text": _select_text(card, selectors["edu_text"]),
            }
        )
    return rows


def crawl_jobs(
    seed_url: str,
    pages: int,
    sleep_sec: float,
    timeout_sec: float,
    selectors: dict[str, str],
    headers: dict[str, str] | None = None,
) -> pd.DataFrame:
    all_rows: list[dict[str, str]] = []
    request_headers = headers or {"User-Agent": "Mozilla/5.0 (DataLabBot/1.0)"}

    with requests.Session() as session:
        for page in range(1, pages + 1):
            page_url = build_page_url(seed_url, page)
            logger.info("Fetching page %s: %s", page, page_url)
            response = session.get(page_url, headers=request_headers, timeout=timeout_sec)
            response.raise_for_status()

            page_rows = extract_jobs_from_html(response.text, page_url, selectors)
            logger.info("Extracted %s jobs from page %s", len(page_rows), page)
            all_rows.extend(page_rows)
            if page < pages and sleep_sec > 0:
                time.sleep(sleep_sec)

    return pd.DataFrame(all_rows, columns=RAW_COLUMNS)


def _parse_selector_overrides(selector_items: list[str] | None) -> dict[str, str]:
    selectors: dict[str, str] = {}
    for item in selector_items or []:
        if "=" not in item:
            raise ConfigValidationError(
                f"Invalid selector override: {item}. Use key=css_selector"
            )
        key, selector = item.split("=", 1)
        key = key.strip()
        selector = selector.strip()
        if key not in DEFAULT_SELECTORS:
            valid_keys = ", ".join(sorted(DEFAULT_SELECTORS))
            raise ConfigValidationError(f"Unknown selector key '{key}'. Valid keys: {valid_keys}")
        if not selector:
            raise ConfigValidationError(f"Selector for key '{key}' cannot be empty.")
        selectors[key] = selector
    return selectors


def _validate_selector_map(selector_map: dict[str, Any] | None) -> dict[str, str]:
    if selector_map is None:
        return {}
    if not isinstance(selector_map, dict):
        raise ConfigValidationError("crawl.selectors must be a mapping/object.")
    out: dict[str, str] = {}
    for key, value in selector_map.items():
        if key not in DEFAULT_SELECTORS:
            valid_keys = ", ".join(sorted(DEFAULT_SELECTORS))
            raise ConfigValidationError(f"Unknown selector key '{key}'. Valid keys: {valid_keys}")
        if not isinstance(value, str) or not value.strip():
            raise ConfigValidationError(f"Selector for key '{key}' must be a non-empty string.")
        out[key] = value.strip()
    return out


def resolve_selectors(
    *,
    seed_url: str,
    selector_items: list[str] | None = None,
    config_selectors: dict[str, Any] | None = None,
) -> dict[str, str]:
    selectors = dict(DEFAULT_SELECTORS)
    host = urlparse(seed_url).netloc.lower()
    for domain, preset in SITE_SELECTOR_PRESETS.items():
        if domain in host:
            selectors.update(preset)
            break
    selectors.update(_validate_selector_map(config_selectors))
    selectors.update(_parse_selector_overrides(selector_items))
    return selectors


def write_raw_csv(df: pd.DataFrame, output_path: str | Path) -> Path:
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out_path, index=False, encoding="utf-8-sig")
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Crawl JD data from website into raw CSV.")
    parser.add_argument(
        "--config",
        required=False,
        help="Optional app config YAML path. Section: crawl.",
    )
    parser.add_argument(
        "--seed-url",
        required=False,
        help="Seed URL. Use {page} placeholder or a URL where ?page= is acceptable.",
    )
    parser.add_argument("--pages", type=int, default=None, help="How many pages to crawl.")
    parser.add_argument("--output", required=False, help="Output raw CSV path.")
    parser.add_argument("--sleep-sec", type=float, default=None, help="Sleep between pages.")
    parser.add_argument("--timeout-sec", type=float, default=None, help="HTTP timeout per request.")
    parser.add_argument(
        "--selector",
        action="append",
        default=[],
        help="Override selector in key=css format; can be repeated. Priority is CLI > config > site preset.",
    )
    parser.add_argument(
        "--log-level", default=None, choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def run_crawler(
    seed_url: str,
    pages: int,
    output_path: str,
    sleep_sec: float,
    timeout_sec: float,
    selector_items: list[str] | None = None,
    config_selectors: dict[str, Any] | None = None,
) -> Path:
    if pages < 1:
        raise ValueError(f"pages must be >= 1, got {pages}")

    selectors = resolve_selectors(
        seed_url=seed_url,
        selector_items=selector_items,
        config_selectors=config_selectors,
    )
    df = crawl_jobs(
        seed_url=seed_url,
        pages=pages,
        sleep_sec=sleep_sec,
        timeout_sec=timeout_sec,
        selectors=selectors,
    )
    output = write_raw_csv(df, output_path)
    logger.info("Wrote raw CSV: %s (rows=%s)", output, len(df))
    return output


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        resolved = resolve_section_config(
            "crawl",
            app_config_path=args.config,
            cli_values={
                "seed_url": args.seed_url,
                "pages": args.pages,
                "output": args.output,
                "sleep_sec": args.sleep_sec,
                "timeout_sec": args.timeout_sec,
                "log_level": args.log_level,
            },
            required_keys={"seed_url", "output"},
        )
        setup_logging(str(resolved.get("log_level", "INFO")))
        run_crawler(
            seed_url=str(resolved["seed_url"]),
            pages=int(resolved.get("pages", 1)),
            output_path=str(resolved["output"]),
            sleep_sec=float(resolved.get("sleep_sec", 1.0)),
            timeout_sec=float(resolved.get("timeout_sec", 20.0)),
            selector_items=args.selector,
            config_selectors=resolved.get("selectors"),
        )
    except ConfigValidationError as exc:
        raise SystemExit(f"Configuration error: {exc}") from exc


if __name__ == "__main__":
    main()
