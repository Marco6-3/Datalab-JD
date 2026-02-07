# DataLab

DataLab is a practical JD data pipeline with CLI, API, and dashboard:
- crawl job listings
- clean and enrich JD data
- analyze market trends
- serve pipeline jobs through FastAPI
- query analytics in DuckDB
- visualize results in Streamlit

## Quickstart CLI

```bash
python -m venv .venv
.venv\Scripts\activate
pip install -e .[dev]
```

Run one-click:

```bash
python -m datalab.jd.oneclick --url "https://www.liepin.com/career/dianziruanjian/" --pages 1 --output-dir data/oneclick_liepin
```

Run step by step:

```bash
python -m datalab.jd.crawl --config config/config.yaml
python -m datalab.clean --input data/raw/crawled_jobs.csv --output data/clean_liepin
python -m datalab.jd.analyze --input data/clean_liepin/cleaned.parquet --output data/clean_liepin/jd_market_report.md
```

## Quickstart API

Start API server:

```bash
python -m uvicorn datalab.api.app:app --reload
```

Open Web UI:

```text
http://127.0.0.1:8000/
```

The home page can submit jobs directly and auto-poll status.
If you prefer API docs, use:

```text
http://127.0.0.1:8000/docs
```

Call API (PowerShell):

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

Open web report page for a finished job:

```text
http://127.0.0.1:8000/pipeline/<job_id>/view
```

Job status flow:
- `queued`
- `running`
- `succeeded` or `failed`

Jobs are persisted in SQLite (`data/api_jobs.sqlite3` by default).

## Quickstart Dashboard

Build DuckDB first:

```bash
python -m datalab.db build --input data/clean_liepin/cleaned.parquet --output data/analytics/jobs.duckdb
```

Run Streamlit dashboard:

```bash
python -m streamlit run src/datalab/dashboard/app.py
```

Dashboard can read from:
- DuckDB table `jd_cleaned`
- fallback parquet path

## Docker

Build:

```bash
docker build -t datalab-api:latest .
```

Run API:

```bash
docker run --rm -p 8000:8000 datalab-api:latest
```

Persist output and job DB to host:

```bash
docker run --rm -p 8000:8000 -v ${PWD}/data:/app/data datalab-api:latest
```

## Config

Default file: `config/config.yaml`

Config priority:
- `config.yaml`
- env override (`.env.example`)
- CLI args

Crawler selector priority:
- CLI `--selector key=value`
- `crawl.selectors` in config
- built-in site preset (`liepin.com`)
- generic defaults

## Outputs

`clean` output:
- `cleaned.parquet`
- `metrics.json`
- `data_quality_report.md`

`analyze` output:
- `jd_market_report.md`

Top jobs include provenance fields:
- `url`
- `fetched_at`
- `raw_salary_text`

Data lineage document: `provenance.md`

## VSCode Report Reading Tips

- Open Markdown preview: `Ctrl+Shift+V`
- Open side-by-side preview: `Ctrl+K V`
- New report layout includes:
  - `Contents` section for quick navigation
  - compact Top20 table plus provenance detail list
  - metrics rendered as tables for easier scanning

## DuckDB Queries

`python -m datalab.db build ...` also writes query examples:
- `data/analytics/example_queries.md`

## Skill Tagging

Rule-based tagging is applied during clean step:
- output columns: `skill_tags`, `skill_tag_count`
- dictionary is configurable via `clean.skill_dictionary` in `config/config.yaml`
- report and dashboard include skill heatmap by city x experience

## Tests

Run all:

```bash
python -m pytest -q
```

Focused:

```bash
python -m pytest -q tests/test_e2e_pipeline.py
python -m pytest -q tests/test_api.py
python -m pytest -q tests/test_duckdb_build.py
```

## Sample Data

- `data/sample/jobs_sample.csv`
