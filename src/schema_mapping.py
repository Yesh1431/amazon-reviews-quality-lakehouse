from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def map_to_canonical_schema(df: DataFrame, category: str) -> DataFrame:
    return (
        df.withColumn("review_id", F.sha2(F.concat_ws("||", "reviewerID", "asin", "unixReviewTime", "reviewText"), 256))
        .withColumnRenamed("asin", "product_id")
        .withColumnRenamed("reviewerID", "user_id")
        .withColumnRenamed("reviewText", "review_text")
        .withColumnRenamed("summary", "review_title")
        .withColumnRenamed("overall", "star_rating")
        .withColumn("helpful_votes", F.col("helpful")[0].cast("int"))
        .withColumn("review_date", F.to_date(F.col("reviewTime"), "MM dd, yyyy"))
        .withColumn("category", F.lit(category))
        .withColumn("product_title", F.coalesce(F.col("title"), F.lit("unknown")))
    )
