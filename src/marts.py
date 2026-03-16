from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_fact_reviews(df: DataFrame) -> DataFrame:
    return df.select(
        "review_id",
        "product_id",
        "user_id",
        "review_date",
        "category",
        "star_rating",
        "helpful_votes",
        "sentiment_label",
        "sentiment_score",
        "review_length",
        "pipeline_run_id",
    )


def build_dim_products(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "product_title", "category").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("star_rating"), 4).alias("avg_rating"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
    )


def build_daily_review_metrics(df: DataFrame) -> DataFrame:
    return df.groupBy("review_date", "category").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("star_rating"), 4).alias("avg_rating"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
        F.sum("helpful_votes").alias("total_helpful_votes"),
    )


def build_category_sentiment_summary(df: DataFrame) -> DataFrame:
    return df.groupBy("category").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("star_rating"), 4).alias("avg_rating"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
        F.sum(F.when(F.col("sentiment_label") == "positive", 1).otherwise(0)).alias("positive_review_count"),
        F.sum(F.when(F.col("sentiment_label") == "negative", 1).otherwise(0)).alias("negative_review_count"),
        F.sum(F.when(F.col("sentiment_label") == "neutral", 1).otherwise(0)).alias("neutral_review_count"),
    )


def build_product_review_summary(df: DataFrame) -> DataFrame:
    return df.groupBy("product_id", "product_title", "category").agg(
        F.count("*").alias("total_reviews"),
        F.round(F.avg("star_rating"), 4).alias("avg_rating"),
        F.round(F.avg("helpful_votes"), 4).alias("avg_helpful_votes"),
        F.round(F.avg("sentiment_score"), 4).alias("avg_sentiment_score"),
    )


def build_negative_theme_summary(df: DataFrame) -> DataFrame:
    return (
        df.filter(F.col("sentiment_label") == "negative")
        .groupBy("category", "keyword_theme")
        .agg(F.count("*").alias("negative_review_count"))
    )
