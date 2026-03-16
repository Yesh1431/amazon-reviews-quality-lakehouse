from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def ingest_raw_json(spark, input_path: str) -> DataFrame:
    return spark.read.json(input_path)


def write_bronze(df: DataFrame, bronze_path: str, run_id: str, source_file: str) -> DataFrame:
    bronze_df = (
        df.withColumn("ingestion_ts", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(run_id))
        .withColumn("source_file", F.lit(source_file))
    )
    bronze_df.write.mode("overwrite").parquet(bronze_path)
    return bronze_df
