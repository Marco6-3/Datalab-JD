import pandas as pd

from datalab.skill_tags import extract_skill_tags


def test_extract_skill_tags_rule_based_dictionary():
    df = pd.DataFrame(
        [
            {"title": "Senior Python Data Engineer", "salary_text": "", "exp_text": "", "edu_text": ""},
            {"title": "Spark SQL Platform Engineer", "salary_text": "", "exp_text": "", "edu_text": ""},
            {"title": "General Ops", "salary_text": "", "exp_text": "", "edu_text": ""},
        ]
    )
    dictionary = {
        "python": ["python"],
        "spark": ["spark"],
        "sql": ["sql"],
    }
    out = extract_skill_tags(df, skill_dictionary=dictionary)
    assert out.loc[0, "skill_tags"] == "python"
    assert out.loc[1, "skill_tags"] == "spark|sql"
    assert out.loc[2, "skill_tags"] == ""
