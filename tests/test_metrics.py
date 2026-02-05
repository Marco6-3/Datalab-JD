import json
from pathlib import Path

import pandas as pd

from datalab.metrics import KEY_COLUMNS, compute_metrics, write_metrics


def test_compute_metrics_schema_and_bounds():
    raw_df = pd.DataFrame(
        [
            {"url": "u1", "salary_text": "20-30k", "exp_text": "3-5y", "edu_text": "bachelor"},
            {"url": "u1", "salary_text": "20-30k", "exp_text": "3-5y", "edu_text": "bachelor"},
            {"url": "u2", "salary_text": "negotiable", "exp_text": "", "edu_text": ""},
        ]
    )
    cleaned_df = pd.DataFrame(
        [
            {
                "url": "u1",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "SZ",
                "salary_min_k": 20.0,
                "salary_max_k": 30.0,
                "salary_months": 12.0,
                "salary_is_negotiable": False,
                "exp_min_years": 3.0,
                "exp_max_years": 5.0,
                "edu_level": "bachelor",
            },
            {
                "url": "u2",
                "title": "ML Engineer",
                "company": "Beta",
                "city": "SH",
                "salary_min_k": None,
                "salary_max_k": None,
                "salary_months": None,
                "salary_is_negotiable": True,
                "exp_min_years": None,
                "exp_max_years": None,
                "edu_level": "unknown",
            },
        ]
    )
    metrics = compute_metrics(raw_df=raw_df, cleaned_df=cleaned_df)
    assert set(metrics.keys()) == {
        "row_count_raw",
        "row_count_cleaned",
        "parse_rate",
        "negotiable_rate",
        "duplicates_rate",
        "missing_rate",
    }
    assert set(metrics["missing_rate"].keys()) == set(KEY_COLUMNS)
    assert 0.0 <= metrics["parse_rate"] <= 1.0
    assert 0.0 <= metrics["negotiable_rate"] <= 1.0
    assert 0.0 <= metrics["duplicates_rate"] <= 1.0


def test_write_metrics_outputs_json(tmp_path: Path):
    metrics = {
        "row_count_raw": 2,
        "row_count_cleaned": 1,
        "parse_rate": 0.5,
        "negotiable_rate": 0.0,
        "duplicates_rate": 0.5,
        "missing_rate": {"url": 0.0},
    }
    path = write_metrics(metrics, tmp_path)
    assert path.exists()
    loaded = json.loads(path.read_text(encoding="utf-8"))
    assert loaded["parse_rate"] == 0.5
