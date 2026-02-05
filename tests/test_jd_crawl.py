from pathlib import Path

import pandas as pd
import pytest

from datalab.config import ConfigValidationError
from datalab.jd.crawl import (
    DEFAULT_SELECTORS,
    build_page_url,
    extract_jobs_from_html,
    resolve_selectors,
    write_raw_csv,
)


def test_build_page_url_supports_placeholder_and_query_append():
    assert build_page_url("https://example.com/jobs?page={page}", 3).endswith("page=3")
    assert build_page_url("https://example.com/jobs?kw=python", 2) == (
        "https://example.com/jobs?kw=python&page=2"
    )


def test_extract_jobs_from_html_maps_required_fields():
    html = """
    <html><body>
      <div class="job-card">
        <a href="/jobs/1" class="job-title">Data Engineer</a>
        <div class="company-name">ACME</div>
        <div class="job-city">Shenzhen</div>
        <div class="publish-date">2025-10-01</div>
        <div class="salary">20-30K 13x</div>
        <div class="exp">3-5y</div>
        <div class="edu">bachelor</div>
      </div>
    </body></html>
    """
    rows = extract_jobs_from_html(
        html,
        page_url="https://example.com/list?page=1",
        selectors=DEFAULT_SELECTORS,
    )
    assert len(rows) == 1
    row = rows[0]
    assert row["url"] == "https://example.com/jobs/1"
    assert row["title"] == "Data Engineer"
    assert row["company"] == "ACME"
    assert row["city"] == "Shenzhen"
    assert row["salary_text"] == "20-30K 13x"


def test_extract_jobs_from_html_handles_missing_optional_text():
    html = """
    <html><body>
      <div class="job-card">
        <a href="/jobs/2" class="job-title">ML Engineer</a>
      </div>
    </body></html>
    """
    rows = extract_jobs_from_html(
        html,
        page_url="https://example.com/list?page=1",
        selectors=DEFAULT_SELECTORS,
    )
    assert len(rows) == 1
    assert rows[0]["company"] == ""
    assert rows[0]["exp_text"] == ""
    assert rows[0]["edu_text"] == ""


def test_resolve_selectors_auto_applies_liepin_preset():
    selectors = resolve_selectors(seed_url="https://www.liepin.com/career/dianziruanjian/pn1/")
    assert selectors["card"] == ".job-card-pc-container"
    assert selectors["salary_text"] == ".job-salary"


def test_resolve_selectors_config_then_cli_override():
    selectors = resolve_selectors(
        seed_url="https://www.liepin.com/career/dianziruanjian/pn1/",
        config_selectors={"card": ".custom-card"},
        selector_items=["card=.cli-card"],
    )
    assert selectors["card"] == ".cli-card"


def test_resolve_selectors_invalid_config_raises():
    with pytest.raises(ConfigValidationError, match="non-empty string"):
        resolve_selectors(
            seed_url="https://www.liepin.com/career/dianziruanjian/pn1/",
            config_selectors={"card": ""},
        )


def test_resolve_selectors_invalid_key_raises():
    with pytest.raises(ConfigValidationError, match="Unknown selector key"):
        resolve_selectors(
            seed_url="https://www.liepin.com/career/dianziruanjian/pn1/",
            config_selectors={"unknown_key": ".x"},
        )


def test_write_raw_csv_persists_output(tmp_path: Path):
    df = pd.DataFrame(
        [
            {
                "url": "https://example.com/jobs/1",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "Shenzhen",
                "publish_date": "2025-10-01",
                "salary_text": "20-30K",
                "exp_text": "3-5y",
                "edu_text": "bachelor",
            }
        ]
    )
    out_path = tmp_path / "raw" / "jobs.csv"
    write_raw_csv(df, out_path)
    assert out_path.exists()
