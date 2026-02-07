from __future__ import annotations

from typing import Any

import pandas as pd
from pandas.api.types import (
    is_bool_dtype,
    is_datetime64_any_dtype,
    is_numeric_dtype,
    is_object_dtype,
)

from datalab.exceptions import DataValidationError
from datalab.jd_features import extract_jd_features
from datalab.skill_tags import extract_skill_tags

MISSING_LIKE = {"", " ", "NA", "N/A", "null", "NULL", "None", "none"}


def normalize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    return df.replace(list(MISSING_LIKE), pd.NA)


def infer_object_types(df: pd.DataFrame, threshold: float = 0.9) -> pd.DataFrame:
    """
    Attempt numeric/datetime conversion for object columns when most non-null
    values can be parsed.
    """
    out = df.copy()
    for col in out.columns:
        if not is_object_dtype(out[col]):
            continue

        non_null = out[col].dropna()
        if non_null.empty:
            continue

        numeric = pd.to_numeric(non_null, errors="coerce")
        if numeric.notna().mean() >= threshold:
            out[col] = pd.to_numeric(out[col], errors="coerce")
            continue

        dt = pd.to_datetime(non_null, errors="coerce", format="mixed")
        if dt.notna().mean() >= threshold:
            out[col] = pd.to_datetime(out[col], errors="coerce", format="mixed")
    return out


def fill_missing_values(df: pd.DataFrame, skip_columns: set[str] | None = None) -> pd.DataFrame:
    out = df.copy()
    protected = skip_columns or set()
    for col in out.columns:
        if col in protected:
            continue
        series = out[col]
        if not series.isna().any():
            continue

        if is_numeric_dtype(series):
            non_null = series.dropna()
            fill_value = non_null.median() if not non_null.empty else 0
            out[col] = series.fillna(fill_value)
        elif is_bool_dtype(series):
            mode = series.mode(dropna=True)
            out[col] = series.fillna(mode.iloc[0] if not mode.empty else False)
        elif is_datetime64_any_dtype(series):
            out[col] = series.ffill().bfill()
        else:
            mode = series.mode(dropna=True)
            out[col] = series.fillna(mode.iloc[0] if not mode.empty else "UNKNOWN")
    return out


def remove_duplicates(df: pd.DataFrame, subset: list[str] | None = None) -> pd.DataFrame:
    if subset:
        available = [col for col in subset if col in df.columns]
        if available:
            return df.drop_duplicates(subset=available, ignore_index=True)
        return df.drop_duplicates(ignore_index=True)

    out = df.copy()
    if "url" in out.columns:
        url = out["url"].astype("string").str.strip()
        has_url = url.notna() & (url != "")
        fallback_cols = [col for col in ["title", "company", "city"] if col in out.columns]

        if fallback_cols:
            fallback_key = (
                out[fallback_cols]
                .fillna("")
                .astype("string")
                .apply(lambda row: "|".join(row.tolist()), axis=1)
            )
            key = pd.Series(index=out.index, dtype="string")
            key.loc[has_url] = "url|" + url.loc[has_url]
            key.loc[~has_url] = "fallback|" + fallback_key.loc[~has_url]
            out["_dedupe_key"] = key
            out = out.drop_duplicates(subset=["_dedupe_key"], ignore_index=True).drop(
                columns=["_dedupe_key"]
            )
            return out

        return out.drop_duplicates(subset=["url"], ignore_index=True)

    return out.drop_duplicates(ignore_index=True)


def clip_outliers_iqr(df: pd.DataFrame, factor: float = 1.5) -> pd.DataFrame:
    out = df.copy()
    for col in out.select_dtypes(include=["number"]).columns:
        if out[col].dropna().empty:
            continue
        q1 = out[col].quantile(0.25)
        q3 = out[col].quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            continue
        lower = q1 - factor * iqr
        upper = q3 + factor * iqr
        out[col] = out[col].clip(lower=lower, upper=upper)
    return out


def _coerce_bool_series(series: pd.Series) -> pd.Series:
    true_vals = {"true", "1", "yes", "y", "t"}
    false_vals = {"false", "0", "no", "n", "f"}
    normalized = series.astype("string").str.strip().str.lower()
    out = pd.Series(index=series.index, dtype="boolean")
    out.loc[normalized.isin(true_vals)] = True
    out.loc[normalized.isin(false_vals)] = False
    out.loc[series.isna()] = pd.NA
    invalid = ~(normalized.isin(true_vals | false_vals) | series.isna())
    if invalid.any():
        raise DataValidationError("Invalid boolean value found in schema conversion.")
    return out


def apply_schema(df: pd.DataFrame, schema: dict[str, Any] | None) -> pd.DataFrame:
    if not schema:
        return df
    out = df.copy()
    for col, dtype in schema.items():
        if col not in out.columns:
            raise DataValidationError(f"Configured column missing from data: {col}")
        try:
            if dtype == "datetime":
                out[col] = pd.to_datetime(out[col], errors="raise")
            elif dtype == "int":
                out[col] = pd.to_numeric(out[col], errors="raise").astype("Int64")
            elif dtype == "float":
                out[col] = pd.to_numeric(out[col], errors="raise").astype(float)
            elif dtype == "str":
                out[col] = out[col].astype(str)
            elif dtype == "bool":
                out[col] = _coerce_bool_series(out[col])
            else:
                raise DataValidationError(f"Unsupported schema dtype for {col}: {dtype}")
        except Exception as exc:
            raise DataValidationError(f"Type conversion failed for {col} -> {dtype}") from exc
    return out


def clean_dataframe(
    df: pd.DataFrame,
    schema: dict[str, Any] | None = None,
    skill_dictionary: dict[str, list[str]] | None = None,
) -> pd.DataFrame:
    out = normalize_missing_values(df)
    if "salary_text" in out.columns and "raw_salary_text" not in out.columns:
        out["raw_salary_text"] = out["salary_text"]
    if "fetched_at" not in out.columns:
        out["fetched_at"] = "UNKNOWN"
    out = infer_object_types(out)
    out = fill_missing_values(out, skip_columns={"url"})
    out = extract_jd_features(out)
    out = extract_skill_tags(out, skill_dictionary=skill_dictionary)
    out = remove_duplicates(out)
    out = clip_outliers_iqr(out)
    out = apply_schema(out, schema)
    return out
