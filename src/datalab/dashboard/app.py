from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st


def load_dataframe(duckdb_path: str | None, parquet_path: str | None) -> pd.DataFrame:
    if duckdb_path:
        db = Path(duckdb_path)
        if not db.exists():
            raise FileNotFoundError(f"DuckDB file not found: {db}")
        with duckdb.connect(str(db), read_only=True) as conn:
            return conn.execute("SELECT * FROM jd_cleaned").df()
    if parquet_path:
        parquet = Path(parquet_path)
        if not parquet.exists():
            raise FileNotFoundError(f"Parquet file not found: {parquet}")
        return pd.read_parquet(parquet)
    raise ValueError("Provide either duckdb_path or parquet_path.")


def _build_exp_bucket(df: pd.DataFrame) -> pd.Series:
    rep = (
        pd.to_numeric(df.get("exp_min_years"), errors="coerce").fillna(0)
        + pd.to_numeric(df.get("exp_max_years"), errors="coerce").fillna(0)
    ) / 2
    return pd.cut(
        rep,
        bins=[-1, 1, 3, 5, 10, 10_000],
        labels=["0-1y", "1-3y", "3-5y", "5-10y", "10y+"],
    ).astype("string").fillna("unknown")


def render_dashboard(df: pd.DataFrame) -> None:
    st.title("DataLab JD Dashboard")
    st.caption("Source: cleaned parquet / DuckDB")

    work = df.copy()
    work["mid_k"] = (
        pd.to_numeric(work.get("salary_min_k"), errors="coerce").fillna(0)
        + pd.to_numeric(work.get("salary_max_k"), errors="coerce").fillna(0)
    ) / 2
    work["exp_bucket"] = _build_exp_bucket(work)

    col1, col2, col3 = st.columns(3)
    col1.metric("Jobs", len(work))
    col2.metric("Cities", int(work.get("city", pd.Series(dtype="object")).nunique()))
    col3.metric("Companies", int(work.get("company", pd.Series(dtype="object")).nunique()))

    st.subheader("City Job Counts")
    city_counts = work.groupby("city", dropna=False).size().sort_values(ascending=False).head(20)
    st.bar_chart(city_counts)

    st.subheader("City x Experience Salary (p50/p90)")
    city_exp = (
        work.groupby(["city", "exp_bucket"], dropna=False)["mid_k"]
        .agg(p50_mid_k="median", p90_mid_k=lambda x: x.quantile(0.9), n_jobs="size")
        .reset_index()
        .sort_values(["city", "exp_bucket"])
    )
    st.dataframe(city_exp, use_container_width=True)

    if "skill_tags" in work.columns:
        st.subheader("Skill Heatmap by City x Experience")
        skill = work[["city", "exp_bucket", "skill_tags"]].copy()
        skill["skill_tags"] = skill["skill_tags"].fillna("").astype(str)
        skill = skill.assign(skill_tag=skill["skill_tags"].str.split("|")).explode("skill_tag")
        skill = skill[skill["skill_tag"].astype(str).str.len() > 0]
        skill_heat = (
            skill.groupby(["city", "exp_bucket", "skill_tag"], dropna=False)
            .size()
            .reset_index(name="n_jobs")
            .sort_values("n_jobs", ascending=False)
            .head(200)
        )
        st.dataframe(skill_heat, use_container_width=True)


def main() -> None:
    st.set_page_config(page_title="DataLab Dashboard", layout="wide")
    st.sidebar.header("Input")
    duckdb_path = st.sidebar.text_input("DuckDB path", value="data/analytics/jobs.duckdb")
    parquet_path = st.sidebar.text_input("Fallback parquet path", value="data/clean/cleaned.parquet")

    try:
        df = load_dataframe(duckdb_path=duckdb_path, parquet_path=parquet_path)
    except Exception as exc:
        st.error(str(exc))
        return
    render_dashboard(df)


if __name__ == "__main__":
    main()

