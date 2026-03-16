from typing import Iterable, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


CRITICAL_COLUMNS = ["review_id", "product_id", "review_date", "category", "star_rating", "sentiment_label", "sentiment_score"]


def validate_required_columns(df: DataFrame, required_cols: Iterable[str]) -> None:
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")


def find_duplicate_review_ids(df: DataFrame) -> DataFrame:
    return df.groupBy("review_id").count().filter(F.col("count") > 1)


def validate_accepted_values(df: DataFrame) -> DataFrame:
    return df.filter(
        (~F.col("star_rating").cast("int").between(1, 5))
        | (~F.col("sentiment_label").isin(["positive", "neutral", "negative"]))
        | (F.col("helpful_votes") < 0)
    )


def detect_future_dates(df: DataFrame) -> DataFrame:
    return df.filter(F.col("review_date") > F.current_date())


def validate_boolean_columns(df: DataFrame, columns: Iterable[str]) -> DataFrame:
    invalid_condition = None
    for col_name in columns:
        if col_name in df.columns:
            c = ~F.col(col_name).isin(True, False)
            invalid_condition = c if invalid_condition is None else (invalid_condition | c)
    if invalid_condition is None:
        return df.limit(0)
    return df.filter(invalid_condition)


def null_critical_records(df: DataFrame, columns=None) -> DataFrame:
    cols = [c for c in (columns or CRITICAL_COLUMNS) if c in df.columns]
    if not cols:
        return df.limit(0)
    condition = None
    for col_name in cols:
        c = F.col(col_name).isNull()
        condition = c if condition is None else (condition | c)
    return df.filter(condition)


def assert_zero_nulls(df: DataFrame, columns=None) -> None:
    if null_critical_records(df, columns=columns).count() > 0:
        raise ValueError("Zero-null guarantee failed for gold output")


def row_count_reconciliation(source_df: DataFrame, valid_df: DataFrame, quarantine_df: DataFrame) -> Tuple[int, int, int]:
    src, valid, quarantined = source_df.count(), valid_df.count(), quarantine_df.count()
    if src != valid + quarantined:
        raise ValueError(f"Row count mismatch source={src}, valid={valid}, quarantine={quarantined}")
    return src, valid, quarantined


def quarantine_report(quarantine_df: DataFrame) -> List[tuple]:
    return [(r["rejection_reason"], r["count"]) for r in quarantine_df.groupBy("rejection_reason").count().collect()]
