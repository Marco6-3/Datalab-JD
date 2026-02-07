from pathlib import Path

import duckdb
import pandas as pd

from datalab.dashboard.app import load_dataframe


def test_dashboard_load_dataframe_from_parquet(tmp_path: Path):
    parquet_path = tmp_path / "cleaned.parquet"
    df = pd.DataFrame([{"city": "SZ", "company": "ACME"}])
    df.to_parquet(parquet_path, index=False)
    loaded = load_dataframe(duckdb_path=None, parquet_path=str(parquet_path))
    assert len(loaded) == 1
    assert "city" in loaded.columns


def test_dashboard_load_dataframe_from_duckdb(tmp_path: Path):
    db_path = tmp_path / "jobs.duckdb"
    with duckdb.connect(str(db_path)) as conn:
        conn.execute("CREATE TABLE jd_cleaned AS SELECT 'SZ' AS city, 'ACME' AS company")
    loaded = load_dataframe(duckdb_path=str(db_path), parquet_path=None)
    assert len(loaded) == 1
    assert loaded.iloc[0]["company"] == "ACME"
