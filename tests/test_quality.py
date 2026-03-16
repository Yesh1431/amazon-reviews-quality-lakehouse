import pytest
from src.quality import validate_required_columns, assert_zero_nulls
from src.quarantine import route_to_quarantine


def test_required_columns_failure(spark):
    df = spark.createDataFrame([{"a": 1}])
    with pytest.raises(ValueError):
        validate_required_columns(df, ["a", "b"])


def test_zero_null_assertion(spark):
    df = spark.createDataFrame([
        {"review_id": None, "product_id": "p", "review_date": "2020-01-01", "category": "electronics", "star_rating": 5, "sentiment_label": "positive", "sentiment_score": 0.5}
    ])
    with pytest.raises(ValueError):
        assert_zero_nulls(df)


def test_quarantine_routing(spark):
    df = spark.createDataFrame([{"review_id": "r1", "review_text": None}])
    out = route_to_quarantine(df, "missing_critical_fields", "run-1", "file.json")
    row = out.collect()[0]
    assert row["rejection_reason"] == "missing_critical_fields"
    assert row["pipeline_run_id"] == "run-1"
