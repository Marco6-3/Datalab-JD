from pathlib import Path

import pandas as pd

from datalab.clean import run_pipeline
from datalab.io import read_input_data
from datalab.report import build_quality_report


def test_read_input_data_from_mixed_files(tmp_path: Path):
    raw = tmp_path / "raw"
    raw.mkdir()
    pd.DataFrame({"id": [1, 2], "amount": [10, 20]}).to_csv(raw / "a.csv", index=False)
    pd.DataFrame([{"id": 3, "amount": 30}]).to_json(raw / "b.jsonl", orient="records", lines=True)

    out = read_input_data(raw)
    assert len(out) == 3
    assert "__source_file" in out.columns


def test_build_quality_report_contains_core_sections():
    df = pd.DataFrame({"id": [1, 2], "city": ["A", "B"]})
    report = build_quality_report(
        df,
        topk=3,
        metrics={
            "parse_rate": 0.5,
            "negotiable_rate": 0.2,
            "duplicates_rate": 0.1,
            "missing_rate": {"city": 0.0},
        },
    )
    assert "# Data Quality Report" in report
    assert "## Overview" in report
    assert "## Metrics Summary" in report
    assert "## Column Details" in report


def test_run_pipeline_writes_expected_outputs(tmp_path: Path):
    raw = tmp_path / "raw"
    out = tmp_path / "clean"
    raw.mkdir()
    pd.DataFrame({"id": [1, 1, 2], "amount": [10, None, 9999]}).to_csv(raw / "data.csv", index=False)

    run_pipeline(str(raw), str(out), schema=None, topk=5)

    assert (out / "cleaned.parquet").exists()
    assert (out / "metrics.json").exists()
    assert (out / "data_quality_report.md").exists()
