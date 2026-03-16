import logging
from pathlib import Path
from pyspark.sql import DataFrame
from pyspark.sql import Window
from pyspark.sql import functions as F
from src.cleaning import clean_and_normalize, split_valid_invalid
from src.config import PipelineConfig
from src.enrichment import enrich_with_nlp
from src.ingestion import ingest_raw_json, write_bronze
from src.marts import (
    build_category_sentiment_summary,
    build_daily_review_metrics,
    build_dim_products,
    build_fact_reviews,
    build_negative_theme_summary,
    build_product_review_summary,
)
from src.quality import (
    assert_zero_nulls,
    detect_future_dates,
    null_critical_records,
    quarantine_report,
    row_count_reconciliation,
    validate_accepted_values,
    validate_boolean_columns,
    validate_required_columns,
)
from src.quarantine import route_to_quarantine, write_quarantine
from src.schema_mapping import map_to_canonical_schema

logger = logging.getLogger(__name__)


def _empty_like(df: DataFrame) -> DataFrame:
    return df.limit(0)


def _dedupe_by_review_id(df: DataFrame):
    """Keep latest record by review_date per review_id; return (deduped, duplicates)."""
    w = Window.partitionBy("review_id").orderBy(F.col("review_date").desc_nulls_last())
    ranked = df.withColumn("_rn", F.row_number().over(w))
    deduped = ranked.filter(F.col("_rn") == 1).drop("_rn")
    duplicates = ranked.filter(F.col("_rn") > 1).drop("_rn")
    return deduped, duplicates


def run_pipeline(spark, cfg: PipelineConfig) -> None:
    cfg.ensure_directories()
    source_file = cfg.input_path

    raw_df = ingest_raw_json(spark, cfg.input_path)
    bronze_df = write_bronze(raw_df, cfg.bronze_path, cfg.pipeline_run_id, source_file)

    mapped = map_to_canonical_schema(bronze_df, cfg.category)
    validate_required_columns(mapped, ["review_id", "product_id", "user_id", "review_text", "review_date", "star_rating"])

    cleaned = clean_and_normalize(mapped)
    deduped, duplicate_records = _dedupe_by_review_id(cleaned)
    valid_clean, invalid_clean = split_valid_invalid(deduped)

    enriched = enrich_with_nlp(valid_clean).withColumn("pipeline_run_id", F.lit(cfg.pipeline_run_id))

    bad_values = validate_accepted_values(enriched)
    boolean_violations = validate_boolean_columns(enriched, ["verified_purchase"])
    future_dates = detect_future_dates(enriched)
    null_criticals = null_critical_records(enriched)

    quarantine_candidates = [
        route_to_quarantine(invalid_clean, "missing_critical_fields", cfg.pipeline_run_id, source_file),
        route_to_quarantine(duplicate_records, "duplicate_review_id", cfg.pipeline_run_id, source_file),
        route_to_quarantine(bad_values, "accepted_values_violation", cfg.pipeline_run_id, source_file),
        route_to_quarantine(boolean_violations, "boolean_validation_failed", cfg.pipeline_run_id, source_file),
        route_to_quarantine(future_dates, "future_date", cfg.pipeline_run_id, source_file),
        route_to_quarantine(null_criticals, "null_critical_columns", cfg.pipeline_run_id, source_file),
    ]

    quarantine_df = _empty_like(quarantine_candidates[0])
    for candidate in quarantine_candidates:
        quarantine_df = quarantine_df.unionByName(candidate, allowMissingColumns=True)

    quarantine_df = quarantine_df.dropDuplicates(["review_id"])

    valid_ids = quarantine_df.select("review_id").distinct()
    curated = enriched.join(valid_ids, on="review_id", how="left_anti")

    row_count_reconciliation(cleaned, curated, quarantine_df)

    curated.write.mode("overwrite").parquet(cfg.silver_path)
    write_quarantine(quarantine_df, cfg.quarantine_path)

    fact_reviews = build_fact_reviews(curated)
    dim_products = build_dim_products(curated)
    daily_metrics = build_daily_review_metrics(curated)
    category_summary = build_category_sentiment_summary(curated)
    product_summary = build_product_review_summary(curated)
    negative_theme = build_negative_theme_summary(curated)

    critical_cols = ["review_id", "product_id", "review_date", "category", "star_rating", "sentiment_label", "sentiment_score"]
    assert_zero_nulls(curated, columns=critical_cols)
    assert_zero_nulls(fact_reviews, columns=critical_cols)

    fact_reviews.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "fact_reviews"))
    dim_products.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "dim_products"))
    daily_metrics.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "daily_review_metrics"))
    category_summary.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "category_sentiment_summary"))
    product_summary.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "product_review_summary"))
    negative_theme.write.mode("overwrite").parquet(str(Path(cfg.gold_path) / "negative_theme_summary"))

    logger.info("Pipeline complete. Quarantine report: %s", quarantine_report(quarantine_df))
