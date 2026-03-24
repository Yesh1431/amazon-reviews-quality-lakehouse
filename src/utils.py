import os
from pyspark.sql import SparkSession


def get_spark(app_name: str = "amazon-reviews-quality-lakehouse") -> SparkSession:
    os.environ.setdefault("SPARK_USER", "spark")
    os.environ.setdefault("HADOOP_USER_NAME", "spark")
    return (
        SparkSession.builder.master("local[*]")
        .appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
