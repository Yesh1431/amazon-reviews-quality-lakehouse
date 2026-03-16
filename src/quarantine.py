from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def route_to_quarantine(df: DataFrame, reason: str, run_id: str, source_file: str) -> DataFrame:
    return (
        df.withColumn("rejection_reason", F.lit(reason))
        .withColumn("pipeline_run_id", F.lit(run_id))
        .withColumn("source_file", F.lit(source_file))
        .withColumn("ingestion_ts", F.current_timestamp())
    )


def write_quarantine(df: DataFrame, output_path: str) -> None:
    df.write.mode("overwrite").parquet(output_path)
