from __future__ import annotations

import re
from typing import Any

import pandas as pd


def _to_text(value: Any) -> str:
    if value is None or (isinstance(value, float) and pd.isna(value)) or value is pd.NA:
        return ""
    return str(value).strip()


def _to_k(value: float, unit: str) -> float:
    unit_norm = (unit or "").lower()
    if "万" in unit_norm or "w" == unit_norm:
        return value * 10
    if "k" in unit_norm or "千" in unit_norm:
        return value
    return value


def parse_salary(salary_text: Any) -> tuple[float | None, float | None, int | None, bool]:
    text = _to_text(salary_text)
    if not text:
        return (None, None, None, False)

    text_lower = text.lower()
    negotiable = any(token in text_lower for token in ["面议", "negotiable", "待定"])

    months_match = re.search(r"(\d{1,2})\s*薪", text_lower)
    months = int(months_match.group(1)) if months_match else None

    range_match = re.search(
        r"(\d+(?:\.\d+)?)\s*(k|千|w|万)?\s*[-~至到]\s*(\d+(?:\.\d+)?)\s*(k|千|w|万)?",
        text_lower,
    )
    if range_match:
        left = float(range_match.group(1))
        right = float(range_match.group(3))
        left_unit = range_match.group(2) or range_match.group(4) or "k"
        right_unit = range_match.group(4) or range_match.group(2) or "k"
        return (_to_k(left, left_unit), _to_k(right, right_unit), months, negotiable)

    single_match = re.search(r"(\d+(?:\.\d+)?)\s*(k|千|w|万)", text_lower)
    if single_match:
        val = _to_k(float(single_match.group(1)), single_match.group(2))
        if "以上" in text_lower or "+" in text_lower:
            return (val, None, months, negotiable)
        if "以下" in text_lower:
            return (None, val, months, negotiable)
        return (val, val, months, negotiable)

    return (None, None, months, negotiable)


def parse_experience(exp_text: Any) -> tuple[float | None, float | None]:
    text = _to_text(exp_text).lower()
    if not text:
        return (None, None)

    if any(token in text for token in ["不限", "无经验", "无需经验"]):
        return (None, None)
    if any(token in text for token in ["应届", "在校"]):
        return (0.0, 1.0)

    range_match = re.search(r"(\d+(?:\.\d+)?)\s*[-~至到]\s*(\d+(?:\.\d+)?)\s*年", text)
    if range_match:
        return (float(range_match.group(1)), float(range_match.group(2)))

    min_match = re.search(r"(\d+(?:\.\d+)?)\s*年以上", text)
    if min_match:
        return (float(min_match.group(1)), None)

    max_match = re.search(r"(\d+(?:\.\d+)?)\s*年以下", text)
    if max_match:
        return (0.0, float(max_match.group(1)))

    single_match = re.search(r"(\d+(?:\.\d+)?)\s*年", text)
    if single_match:
        year = float(single_match.group(1))
        return (year, year)

    return (None, None)


def normalize_education(edu_text: Any) -> str:
    text = _to_text(edu_text)
    if not text:
        return "unknown"

    if any(token in text for token in ["不限", "无要求"]):
        return "no_requirement"
    if "博士" in text:
        return "phd"
    if "硕士" in text:
        return "master"
    if "本科" in text:
        return "bachelor"
    if "大专" in text:
        return "associate"
    if any(token in text for token in ["中专", "高中"]):
        return "high_school"
    return "other"


def extract_jd_features(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()

    salary_values = out["salary_text"].apply(parse_salary) if "salary_text" in out.columns else []
    if len(salary_values):
        out["salary_min_k"] = salary_values.apply(lambda x: x[0])
        out["salary_max_k"] = salary_values.apply(lambda x: x[1])
        out["salary_months"] = salary_values.apply(lambda x: x[2])
        out["salary_is_negotiable"] = salary_values.apply(lambda x: x[3])
    else:
        out["salary_min_k"] = pd.NA
        out["salary_max_k"] = pd.NA
        out["salary_months"] = pd.NA
        out["salary_is_negotiable"] = False

    exp_values = out["exp_text"].apply(parse_experience) if "exp_text" in out.columns else []
    if len(exp_values):
        out["exp_min_years"] = exp_values.apply(lambda x: x[0])
        out["exp_max_years"] = exp_values.apply(lambda x: x[1])
    else:
        out["exp_min_years"] = pd.NA
        out["exp_max_years"] = pd.NA

    if "edu_text" in out.columns:
        out["edu_level"] = out["edu_text"].apply(normalize_education)
    else:
        out["edu_level"] = "unknown"

    return out
