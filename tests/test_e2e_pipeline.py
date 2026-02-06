from pathlib import Path

import pandas as pd

from datalab.clean import run_pipeline
from datalab.jd.analyze import generate_jd_market_report


def test_e2e_pipeline_on_sample_data(tmp_path: Path):
    sample_input = Path("data/sample")
    output_dir = tmp_path / "e2e_output"

    run_pipeline(
        input_path=str(sample_input),
        output_path=str(output_dir),
        schema=None,
        topk=5,
    )

    cleaned_path = output_dir / "cleaned.parquet"
    metrics_path = output_dir / "metrics.json"
    quality_report_path = output_dir / "data_quality_report.md"
    market_report_path = output_dir / "jd_market_report.md"

    assert cleaned_path.exists()
    assert metrics_path.exists()
    assert quality_report_path.exists()

    cleaned_df = pd.read_parquet(cleaned_path)
    required_columns = {
        "url",
        "title",
        "company",
        "city",
        "salary_min_k",
        "salary_max_k",
        "salary_months",
        "salary_is_negotiable",
        "exp_min_years",
        "exp_max_years",
        "edu_level",
    }
    assert required_columns.issubset(set(cleaned_df.columns))

    generate_jd_market_report(cleaned_path, market_report_path)
    assert market_report_path.exists()

    quality_report = quality_report_path.read_text(encoding="utf-8")
    market_report = market_report_path.read_text(encoding="utf-8")

    assert "## Metrics Summary" in quality_report
    assert "## 2) City x Experience Table" in market_report
    assert "| city | exp_bucket | n_jobs | p50_mid_k | p90_mid_k | negotiable_rate |" in market_report
