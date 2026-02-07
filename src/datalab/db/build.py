from __future__ import annotations

import argparse
import logging
from pathlib import Path

import duckdb

from datalab.logging_utils import setup_logging

logger = logging.getLogger(__name__)

EXAMPLE_SQL = {
    "city_exp_p50_p90": """
SELECT city,
       CASE
           WHEN COALESCE((exp_min_years + exp_max_years) / 2, 0) <= 1 THEN '0-1y'
           WHEN COALESCE((exp_min_years + exp_max_years) / 2, 0) <= 3 THEN '1-3y'
           WHEN COALESCE((exp_min_years + exp_max_years) / 2, 0) <= 5 THEN '3-5y'
           WHEN COALESCE((exp_min_years + exp_max_years) / 2, 0) <= 10 THEN '5-10y'
           ELSE '10y+'
       END AS exp_bucket,
       COUNT(*) AS n_jobs,
       quantile_cont((salary_min_k + salary_max_k) / 2, 0.5) AS p50_mid_k,
       quantile_cont((salary_min_k + salary_max_k) / 2, 0.9) AS p90_mid_k
FROM jd_cleaned
GROUP BY city, exp_bucket
ORDER BY city, exp_bucket;
""".strip(),
    "top_companies": """
SELECT company, COUNT(*) AS n_jobs
FROM jd_cleaned
GROUP BY company
ORDER BY n_jobs DESC
LIMIT 20;
""".strip(),
    "top_salary_jobs": """
SELECT title, company, city, ((salary_min_k + salary_max_k) / 2) AS mid_k, url
FROM jd_cleaned
WHERE salary_min_k IS NOT NULL OR salary_max_k IS NOT NULL
ORDER BY mid_k DESC
LIMIT 20;
""".strip(),
}


def build_duckdb(cleaned_parquet: str | Path, duckdb_path: str | Path) -> Path:
    parquet_path = Path(cleaned_parquet)
    if not parquet_path.exists():
        raise FileNotFoundError(f"Cleaned parquet not found: {parquet_path}")

    db_path = Path(duckdb_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as conn:
        conn.execute("DROP TABLE IF EXISTS jd_cleaned")
        conn.execute(
            "CREATE TABLE jd_cleaned AS SELECT * FROM read_parquet(?)",
            [str(parquet_path)],
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jd_city ON jd_cleaned(city)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_jd_company ON jd_cleaned(company)")
    logger.info("Built DuckDB at %s", db_path)
    return db_path


def write_example_queries(output_path: str | Path) -> Path:
    out_path = Path(output_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    lines: list[str] = ["# DuckDB Example Queries", ""]
    for name, query in EXAMPLE_SQL.items():
        lines.append(f"## {name}")
        lines.append("```sql")
        lines.append(query)
        lines.append("```")
        lines.append("")
    out_path.write_text("\n".join(lines), encoding="utf-8")
    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build DuckDB analytics db from cleaned parquet.")
    parser.add_argument("--input", required=True, help="Input cleaned parquet path.")
    parser.add_argument("--output", required=True, help="Output duckdb file path.")
    parser.add_argument(
        "--query-doc",
        default="data/analytics/example_queries.md",
        help="Where to write example SQL queries markdown.",
    )
    parser.add_argument(
        "--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"]
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.log_level)
    build_duckdb(args.input, args.output)
    query_doc = write_example_queries(args.query_doc)
    logger.info("Wrote query doc: %s", query_doc)


if __name__ == "__main__":
    main()

