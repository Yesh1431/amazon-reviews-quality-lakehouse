import argparse
from src.config import PipelineConfig
from src.logging_config import configure_logging
from src.pipeline import run_pipeline
from src.utils import get_spark


def parse_args():
    parser = argparse.ArgumentParser(description="Amazon reviews quality lakehouse pipeline")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--bronze-path", required=True)
    parser.add_argument("--silver-path", required=True)
    parser.add_argument("--gold-path", required=True)
    parser.add_argument("--quarantine-path", required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    configure_logging()
    spark = get_spark()
    cfg = PipelineConfig(
        input_path=args.input_path,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        gold_path=args.gold_path,
        quarantine_path=args.quarantine_path,
    )
    try:
        run_pipeline(spark, cfg)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
