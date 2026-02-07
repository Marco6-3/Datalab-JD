from pathlib import Path

import duckdb
import pandas as pd

from datalab.db.build import build_duckdb, write_example_queries


def test_build_duckdb_creates_file_and_query_works(tmp_path: Path):
    parquet_path = tmp_path / "cleaned.parquet"
    db_path = tmp_path / "jobs.duckdb"
    query_doc = tmp_path / "queries.md"

    pd.DataFrame(
        [
            {
                "url": "u1",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "Shenzhen",
                "salary_min_k": 20.0,
                "salary_max_k": 30.0,
                "salary_months": 12.0,
                "salary_is_negotiable": False,
                "exp_min_years": 3.0,
                "exp_max_years": 5.0,
            }
        ]
    ).to_parquet(parquet_path, index=False)

    out_db = build_duckdb(parquet_path, db_path)
    out_doc = write_example_queries(query_doc)
    assert out_db.exists()
    assert out_doc.exists()

    with duckdb.connect(str(out_db), read_only=True) as conn:
        count = conn.execute("SELECT COUNT(*) FROM jd_cleaned").fetchone()[0]
    assert count == 1
