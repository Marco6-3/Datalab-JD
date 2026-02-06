# DataLab

DataLab is a job-data pipeline project that supports:
- crawling job listings
- cleaning and structuring JD fields
- generating quality and market reports

Compatible CLI commands:
- `python -m datalab.jd.crawl`
- `python -m datalab.clean`
- `python -m datalab.jd.analyze`
- `python -m datalab.jd.oneclick`

## Quickstart

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

## One-Click Run

```bash
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 1 --output-dir data/oneclick_liepin
```

Outputs:
- `raw_crawled.csv`
- `cleaned.parquet`
- `metrics.json`
- `data_quality_report.md`
- `jd_market_report.md`

## Config System (PR1)

- Default config: `config/config.yaml`
- Env sample: `.env.example`
- Priority: `config.yaml < env < CLI`

## Crawl Selector Priority

For `crawl`, selector priority is:
1. CLI: `--selector key=value`
2. Config: `config/config.yaml` -> `crawl.selectors`
3. Built-in site preset (`liepin.com`)
4. Generic defaults

## Metrics (PR2)

Each `clean` run writes `metrics.json` with:
- `parse_rate`
- `missing_rate` per key column
- `negotiable_rate`
- `duplicates_rate`
- `row_count_raw` / `row_count_cleaned`

`data_quality_report.md` includes a `Metrics Summary` section.

## E2E Regression (PR3)

Sample dataset: `data/sample/jobs_sample.csv`

The test verifies:
- output files exist after `clean -> analyze`
- key columns exist in parquet
- key report sections exist

Run:

```bash
python -m pytest -q tests/test_e2e_pipeline.py
```

## API Skeleton + Async Job Store (PR4 + PR5)

FastAPI endpoints:
- `POST /pipeline/run`: enqueue a pipeline job and return `job_id`
- `GET /pipeline/{job_id}`: query job status and output paths

Job execution:
- asynchronous via thread pool
- status flow: `queued -> running -> succeeded/failed`
- persisted in SQLite job store (`data/api_jobs.sqlite3` by default)

Run API:

```bash
python -m uvicorn datalab.api.app:app --reload
```

API call example (PowerShell):

```powershell
$body = @{
  input_path = "data/sample"
  output_dir = "data/api_run"
  topk = 5
  generate_market_report = $true
} | ConvertTo-Json

$run = Invoke-RestMethod -Method Post -Uri "http://127.0.0.1:8000/pipeline/run" -ContentType "application/json" -Body $body
Invoke-RestMethod -Method Get -Uri ("http://127.0.0.1:8000/pipeline/" + $run.job_id)
```

## Docker (PR6)

Build image:

```bash
docker build -t datalab-api:latest .
```

Run API container:

```bash
docker run --rm -p 8000:8000 datalab-api:latest
```

Optional: persist API job DB and outputs to host:

```bash
docker run --rm -p 8000:8000 -v ${PWD}/data:/app/data datalab-api:latest
```

## Script List (No Makefile)

```bash
# crawl (from config)
python -m datalab.jd.crawl --config config/config.yaml

# clean
python -m datalab.clean --input data/raw/crawled_jobs.csv --output data/clean_liepin

# analyze
python -m datalab.jd.analyze --input data/clean_liepin/cleaned.parquet --output data/clean_liepin/jd_market_report.md

# one-click
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 1 --output-dir data/oneclick_liepin

# API tests
python -m pytest -q tests/test_api.py

# all tests
python -m pytest -q
```

## Sample Data

- `data/sample/jobs_sample.csv`
