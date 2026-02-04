import pandas as pd
import pytest

from datalab.cleaning import clean_dataframe
from datalab.exceptions import DataValidationError


def test_missing_value_fill_numeric_and_categorical():
    df = pd.DataFrame(
        {
            "num": [1, None, 3],
            "city": ["SZ", None, "SZ"],
        }
    )
    out = clean_dataframe(df)
    assert out["num"].isna().sum() == 0
    assert out["num"].iloc[1] == 2
    assert out["city"].isna().sum() == 0
    assert out["city"].iloc[1] == "SZ"


def test_schema_validation_raises_on_bad_type():
    df = pd.DataFrame({"id": ["A", "2"]})
    with pytest.raises(DataValidationError):
        clean_dataframe(df, schema={"id": "int"})


def test_remove_duplicates():
    df = pd.DataFrame({"id": [1, 1, 2], "name": ["a", "a", "b"]})
    out = clean_dataframe(df)
    assert len(out) == 2


def test_clip_outlier_iqr():
    df = pd.DataFrame({"x": [1, 2, 3, 4, 1000]})
    out = clean_dataframe(df)
    assert out["x"].max() < 1000


def test_infer_datetime_and_fill():
    df = pd.DataFrame({"ts": ["2024-01-01", "2024-01-02", None]})
    out = clean_dataframe(df)
    assert str(out["ts"].dtype).startswith("datetime64")
    assert out["ts"].isna().sum() == 0
