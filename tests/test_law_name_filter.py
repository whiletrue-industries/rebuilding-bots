from botnim.vector_store.vector_store_aurora import _normalize_law_name, _build_metadata_filter_sql


def test_normalize_collapses_maqaf_colon_and_whitespace():
    # model emits hyphen+colon; stored basic laws use maqaf+colon — both must normalize equal
    assert _normalize_law_name("חוק-יסוד: הממשלה") == "חוק-יסוד הממשלה"
    assert _normalize_law_name("חוק־יסוד: הכנסת") == "חוק-יסוד הכנסת"   # maqaf U+05BE → hyphen
    assert _normalize_law_name("חוק-יסוד הממשלה") == "חוק-יסוד הממשלה"   # already-normal stored value
    assert _normalize_law_name("חוק   חובת  המכרזים ") == "חוק חובת המכרזים"
    assert _normalize_law_name(None) is None


def test_build_filter_normalizes_law_name():
    sql, params = _build_metadata_filter_sql({"law_name": "חוק-יסוד: הממשלה"})
    assert "metadata->>'law_name'" in sql and ":law_norm" in sql
    assert params == {"law_norm": "חוק-יסוד הממשלה"}
    assert "@>" not in sql            # law_name uses normalized equality, not containment


def test_build_filter_keeps_containment_for_other_keys():
    sql, params = _build_metadata_filter_sql({"decision_number": "550"})
    assert "metadata @> CAST(:mfilter AS jsonb)" in sql
    assert params == {"mfilter": '{"decision_number": "550"}'}


def test_build_filter_empty():
    assert _build_metadata_filter_sql(None) == ("", {})
    assert _build_metadata_filter_sql({}) == ("", {})
