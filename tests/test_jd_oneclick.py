from pathlib import Path

import pandas as pd
import pytest

from datalab.jd.oneclick import (
    build_liepin_seed_url,
    detect_site,
    resolve_crawl_plan,
    run_one_click,
)


def test_build_liepin_seed_url_from_base_and_pn_path():
    assert (
        build_liepin_seed_url("https://www.liepin.com/career/dianziruanjian/")
        == "https://www.liepin.com/career/dianziruanjian/pn{page}/"
    )
    assert (
        build_liepin_seed_url("https://www.liepin.com/career/dianziruanjian/pn3/")
        == "https://www.liepin.com/career/dianziruanjian/pn{page}/"
    )


def test_detect_site_and_plan_for_liepin():
    assert detect_site("https://www.liepin.com/career/dianziruanjian/") == "liepin"
    seed, selectors = resolve_crawl_plan("https://www.liepin.com/career/dianziruanjian/")
    assert seed.endswith("/pn{page}/")
    assert selectors["card"] == ".job-card-pc-container"


def test_detect_site_unsupported_raises():
    with pytest.raises(ValueError, match="Unsupported website"):
        detect_site("https://example.com/jobs")


def test_run_one_click_writes_outputs(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    raw_df = pd.DataFrame(
        [
            {
                "url": "https://www.liepin.com/job/1.shtml",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "Shenzhen",
                "publish_date": "",
                "salary_text": "20-30k·13薪",
                "exp_text": "3-5年",
                "edu_text": "本科",
            }
        ]
    )

    monkeypatch.setattr("datalab.jd.oneclick.crawl_jobs", lambda **kwargs: raw_df)

    outputs = run_one_click(
        url="https://www.liepin.com/career/dianziruanjian/",
        pages=1,
        output_dir=str(tmp_path / "run"),
        sleep_sec=0,
        timeout_sec=10,
        config_path=None,
        topk=5,
    )

    assert outputs["raw_csv"].exists()
    assert outputs["cleaned_parquet"].exists()
    assert outputs["quality_report"].exists()
    assert outputs["market_report"].exists()
