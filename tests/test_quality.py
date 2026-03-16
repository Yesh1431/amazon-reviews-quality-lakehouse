import pytest
from pyspark.sql import types as T
from src.quality import validate_required_columns, assert_zero_nulls
from src.quarantine import route_to_quarantine


def test_required_columns_failure(spark):
    df = spark.createDataFrame([{"a": 1}])
    with pytest.raises(ValueError):
        validate_required_columns(df, ["a", "b"])


def test_zero_null_assertion(spark):
    schema = T.StructType(
        [
            T.StructField("review_id", T.StringType(), True),
            T.StructField("product_id", T.StringType(), True),
            T.StructField("review_date", T.StringType(), True),
            T.StructField("category", T.StringType(), True),
            T.StructField("star_rating", T.IntegerType(), True),
            T.StructField("sentiment_label", T.StringType(), True),
            T.StructField("sentiment_score", T.DoubleType(), True),
        ]
    )
    df = spark.createDataFrame(
        [(None, "p", "2020-01-01", "electronics", 5, "positive", 0.5)],
        schema=schema,
    )
    with pytest.raises(ValueError):
        assert_zero_nulls(df)


def test_quarantine_routing(spark):
    schema = T.StructType(
        [
            T.StructField("review_id", T.StringType(), True),
            T.StructField("review_text", T.StringType(), True),
        ]
    )
    df = spark.createDataFrame([("r1", None)], schema=schema)
    out = route_to_quarantine(df, "missing_critical_fields", "run-1", "file.json")
    row = out.collect()[0]
    assert row["rejection_reason"] == "missing_critical_fields"
    assert row["pipeline_run_id"] == "run-1"
