from pyspark.sql import SparkSession


def get_spark(app_name: str = "amazon-reviews-quality-lakehouse") -> SparkSession:
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
