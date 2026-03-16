from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def clean_and_normalize(df: DataFrame) -> DataFrame:
    """Standardize text/typing and derive review-level metrics."""
    return (
        df.withColumn("review_text", F.trim(F.regexp_replace(F.coalesce(F.col("review_text"), F.lit("")), r"\s+", " ")))
        .withColumn("review_title", F.trim(F.coalesce(F.col("review_title"), F.lit(""))))
        .withColumn("product_title", F.trim(F.coalesce(F.col("product_title"), F.lit("unknown"))))
        .withColumn("category", F.lower(F.trim(F.coalesce(F.col("category"), F.lit("unknown")))))
        .withColumn("review_date", F.to_date("review_date"))
        .withColumn("star_rating", F.col("star_rating").cast("int"))
        .withColumn("helpful_votes", F.coalesce(F.col("helpful_votes").cast("int"), F.lit(0)))
        .withColumn("verified_purchase", F.coalesce(F.col("verified").cast("boolean"), F.lit(False)) if "verified" in df.columns else F.lit(False))
        .withColumn("review_length", F.length(F.col("review_text")))
        .withColumn("title_length", F.length(F.col("review_title")))
        .withColumn("review_year", F.year(F.col("review_date")))
        .withColumn("review_month", F.month(F.col("review_date")))
        .withColumn("review_week", F.weekofyear(F.col("review_date")))
        .withColumn("helpful_vote_ratio", F.when(F.col("review_length") <= 0, F.lit(0.0)).otherwise(F.col("helpful_votes") / F.col("review_length")))
    )


def split_valid_invalid(df: DataFrame):
    critical_condition = (
        F.col("review_id").isNull()
        | F.col("product_id").isNull()
        | F.col("user_id").isNull()
        | (F.length(F.col("review_text")) == 0)
        | F.col("review_date").isNull()
    )
    invalid = df.filter(critical_condition)
    valid = df.filter(~critical_condition)
    return valid, invalid
