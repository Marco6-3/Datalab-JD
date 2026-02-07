from __future__ import annotations

from typing import Iterable

import pandas as pd

DEFAULT_SKILL_DICTIONARY: dict[str, list[str]] = {
    "python": ["python", "py"],
    "sql": ["sql", "mysql", "postgres", "postgresql"],
    "spark": ["spark", "pyspark"],
    "airflow": ["airflow"],
    "hadoop": ["hadoop", "hive"],
    "docker": ["docker", "k8s", "kubernetes"],
    "aws": ["aws", "s3", "ec2", "emr"],
}


def _to_text(value: object) -> str:
    if value is None or value is pd.NA:
        return ""
    if isinstance(value, float) and pd.isna(value):
        return ""
    return str(value).strip().lower()


def _normalize_dictionary(skill_dictionary: dict[str, Iterable[str]] | None) -> dict[str, list[str]]:
    if not skill_dictionary:
        return DEFAULT_SKILL_DICTIONARY
    normalized: dict[str, list[str]] = {}
    for tag, keywords in skill_dictionary.items():
        key = str(tag).strip().lower()
        values = [str(k).strip().lower() for k in keywords if str(k).strip()]
        if key and values:
            normalized[key] = values
    return normalized or DEFAULT_SKILL_DICTIONARY


def extract_skill_tags(
    df: pd.DataFrame,
    skill_dictionary: dict[str, Iterable[str]] | None = None,
    text_columns: tuple[str, ...] = ("title", "salary_text", "exp_text", "edu_text"),
) -> pd.DataFrame:
    out = df.copy()
    dictionary = _normalize_dictionary(skill_dictionary)

    def _tag_row(row: pd.Series) -> str:
        text = " ".join(_to_text(row.get(col, "")) for col in text_columns)
        matched: list[str] = []
        for tag, keywords in dictionary.items():
            if any(keyword in text for keyword in keywords):
                matched.append(tag)
        return "|".join(sorted(set(matched)))

    out["skill_tags"] = out.apply(_tag_row, axis=1)
    out["skill_tag_count"] = out["skill_tags"].apply(lambda x: 0 if not x else len(x.split("|")))
    return out
