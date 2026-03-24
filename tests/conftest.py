import os
import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    os.environ.setdefault("SPARK_USER", "spark")
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("test-amazon-reviews-lakehouse")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    yield spark
    spark.stop()
