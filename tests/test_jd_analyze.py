from pathlib import Path

import pandas as pd
import pytest

from datalab.jd.analyze import (
    bucket_experience,
    city_exp_summary,
    compute_mid_k,
    generate_jd_market_report,
)


def test_bucket_experience_boundaries():
    assert bucket_experience(0, 1) == "0-1y"
    assert bucket_experience(1, 3) == "1-3y"
    assert bucket_experience(3, 5) == "3-5y"
    assert bucket_experience(5, 10) == "5-10y"
    assert bucket_experience(10.5, None) == "10y+"
    assert bucket_experience(None, None) == "unknown"


def test_compute_mid_k_with_month_adjustment():
    assert compute_mid_k(20, 30, 12) == 25.0
    assert compute_mid_k(None, 30, None) == 30.0
    assert compute_mid_k(20, 30, 13) == pytest.approx(27.0833, rel=1e-4)


def test_city_exp_summary_group_stats_and_rate_range():
    df = pd.DataFrame(
        [
            {
                "city": "Shenzhen",
                "salary_min_k": 10,
                "salary_max_k": 10,
                "salary_months": 12,
                "salary_is_negotiable": False,
                "exp_min_years": 1,
                "exp_max_years": 3,
                "url": "u1",
                "title": "t1",
                "company": "c1",
            },
            {
                "city": "Shenzhen",
                "salary_min_k": 20,
                "salary_max_k": 20,
                "salary_months": 12,
                "salary_is_negotiable": False,
                "exp_min_years": 1,
                "exp_max_years": 3,
                "url": "u2",
                "title": "t2",
                "company": "c2",
            },
            {
                "city": "Shenzhen",
                "salary_min_k": 30,
                "salary_max_k": 30,
                "salary_months": 12,
                "salary_is_negotiable": True,
                "exp_min_years": 1,
                "exp_max_years": 3,
                "url": "u3",
                "title": "t3",
                "company": "c3",
            },
        ]
    )
    out = city_exp_summary(df)
    row = out.iloc[0]
    assert row["city"] == "Shenzhen"
    assert row["exp_bucket"] == "1-3y"
    assert int(row["n_jobs"]) == 3
    assert row["p50_mid_k"] == 20.0
    assert row["p90_mid_k"] == pytest.approx(28.0, rel=1e-4)
    assert 0.0 <= float(row["negotiable_rate"]) <= 1.0


def test_generate_report_raises_on_missing_required_fields(tmp_path: Path):
    bad_df = pd.DataFrame(
        [
            {
                "city": "SZ",
                "salary_min_k": 20,
                "salary_max_k": 30,
                "salary_months": 12,
                "salary_is_negotiable": False,
                "exp_min_years": 1,
                "exp_max_years": 3,
                "url": "u1",
                "title": "Data Engineer",
            }
        ]
    )
    in_path = tmp_path / "bad.parquet"
    out_path = tmp_path / "jd_market_report.md"
    bad_df.to_parquet(in_path, index=False)

    with pytest.raises(ValueError, match="Missing required columns"):
        generate_jd_market_report(in_path, out_path)
