import pandas as pd

from datalab.cleaning import clean_dataframe
from datalab.jd_features import normalize_education, parse_experience, parse_salary


def test_parse_salary_range_and_months():
    min_k, max_k, months, negotiable = parse_salary("15-25K 14薪")
    assert min_k == 15
    assert max_k == 25
    assert months == 14
    assert negotiable is False


def test_parse_experience_and_education():
    min_years, max_years = parse_experience("3-5年经验")
    assert min_years == 3
    assert max_years == 5
    assert normalize_education("本科及以上") == "bachelor"


def test_deduplicate_by_url_with_fallback_fields():
    df = pd.DataFrame(
        [
            {"url": "u1", "title": "A", "company": "C1", "city": "SZ"},
            {"url": "u1", "title": "B", "company": "C2", "city": "SH"},
            {"url": None, "title": "X", "company": "C3", "city": "BJ"},
            {"url": None, "title": "X", "company": "C3", "city": "BJ"},
        ]
    )
    out = clean_dataframe(df)
    assert len(out) == 2


def test_jd_schema_conversion_core_fields():
    df = pd.DataFrame(
        [
            {
                "url": "u1",
                "title": "Data Engineer",
                "company": "ACME",
                "city": "SZ",
                "publish_date": "2025-10-01",
                "salary_text": "20-30K 13薪",
                "exp_text": "3-5年",
                "edu_text": "本科",
            }
        ]
    )
    schema = {
        "publish_date": "datetime",
        "salary_min_k": "float",
        "salary_max_k": "float",
        "salary_is_negotiable": "bool",
    }
    out = clean_dataframe(df, schema=schema)
    assert str(out["publish_date"].dtype).startswith("datetime64")
    assert out["salary_min_k"].iloc[0] == 20.0
    assert out["salary_max_k"].iloc[0] == 30.0
    assert bool(out["salary_is_negotiable"].iloc[0]) is False
