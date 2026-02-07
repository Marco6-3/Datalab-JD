# Data Provenance

## Lineage Overview

1. Source crawl:
   - Command: `python -m datalab.jd.crawl ...`
   - Output: raw CSV with `fetched_at`, `url`, `title`, `company`, `city`, `salary_text`, `exp_text`, `edu_text`
2. Cleaning:
   - Command: `python -m datalab.clean ...`
   - Output: `cleaned.parquet`, `metrics.json`, `data_quality_report.md`
   - Key transforms: missing value normalization, type inference, JD feature extraction, dedupe, outlier clipping, schema checks, skill tagging
3. Analysis:
   - Command: `python -m datalab.jd.analyze ...`
   - Output: `jd_market_report.md`
   - Top jobs section includes provenance fields: `url`, `fetched_at`, `raw_salary_text`
4. API pipeline:
   - Endpoint: `POST /pipeline/run`
   - Job status and output paths tracked in SQLite (`data/api_jobs.sqlite3`)

## Audit Fields

- `url`: original posting link
- `fetched_at`: crawl timestamp in UTC ISO format
- `raw_salary_text`: salary text preserved from source data before structured parsing
