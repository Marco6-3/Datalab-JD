import time
from pathlib import Path

import pandas as pd
from fastapi.testclient import TestClient

from datalab.api.app import create_app


def _poll_job_status(client: TestClient, job_id: str, timeout_sec: float = 5.0) -> dict:
    deadline = time.time() + timeout_sec
    while time.time() < deadline:
        resp = client.get(f"/pipeline/{job_id}")
        if resp.status_code != 200:
            raise AssertionError(f"Unexpected status code: {resp.status_code}")
        payload = resp.json()
        if payload["status"] in {"succeeded", "failed"}:
            return payload
        time.sleep(0.05)
    raise AssertionError(f"Job {job_id} did not finish in {timeout_sec} seconds")


def test_pipeline_api_happy_path(tmp_path: Path):
    raw_dir = tmp_path / "raw"
    out_dir = tmp_path / "out"
    db_path = tmp_path / "jobs.sqlite3"
    raw_dir.mkdir(parents=True, exist_ok=True)

    pd.DataFrame(
        [
            {
                "url": "https://example.com/job/1",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "Shenzhen",
                "publish_date": "2025-01-01",
                "salary_text": "20-30K 13x",
                "exp_text": "3-5y",
                "edu_text": "bachelor",
            }
        ]
    ).to_csv(raw_dir / "jobs.csv", index=False)

    app = create_app(job_db_path=str(db_path))
    with TestClient(app) as client:
        run_resp = client.post(
            "/pipeline/run",
            json={
                "input_path": str(raw_dir),
                "output_dir": str(out_dir),
                "topk": 5,
                "generate_market_report": True,
            },
            headers={"X-Request-ID": "test-request-id-1"},
        )

        assert run_resp.status_code == 200
        run_data = run_resp.json()
        assert run_data["status"] == "queued"
        assert run_data["job_id"]
        assert run_resp.headers["X-Request-ID"] == "test-request-id-1"

        status_data = _poll_job_status(client, run_data["job_id"])
        assert status_data["status"] == "succeeded"
        assert Path(status_data["outputs"]["cleaned_parquet"]).exists()
        assert Path(status_data["outputs"]["metrics_json"]).exists()
        assert Path(status_data["outputs"]["quality_report_md"]).exists()
        assert Path(status_data["outputs"]["market_report_md"]).exists()


def test_pipeline_api_failed_job_contains_error(tmp_path: Path):
    out_dir = tmp_path / "out_bad"
    db_path = tmp_path / "jobs_bad.sqlite3"
    app = create_app(job_db_path=str(db_path))

    with TestClient(app) as client:
        run_resp = client.post(
            "/pipeline/run",
            json={
                "input_path": str(tmp_path / "missing_input"),
                "output_dir": str(out_dir),
                "topk": 5,
                "generate_market_report": False,
            },
        )
        assert run_resp.status_code == 200
        job_id = run_resp.json()["job_id"]

        status_data = _poll_job_status(client, job_id)
        assert status_data["status"] == "failed"
        assert status_data["error_message"]
