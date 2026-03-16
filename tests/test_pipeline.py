from src.pipeline import _dedupe_by_review_id


def test_dedupe_by_review_id_keeps_latest(spark):
    df = spark.createDataFrame(
        [
            {"review_id": "r1", "review_date": "2020-01-01", "review_text": "first"},
            {"review_id": "r1", "review_date": "2020-02-01", "review_text": "latest"},
            {"review_id": "r1", "review_date": "2020-03-01", "review_text": "newest"},
            {"review_id": "r2", "review_date": "2020-03-01", "review_text": "only"},
        ]
    )
    deduped, duplicates = _dedupe_by_review_id(df)
    assert deduped.count() == 2
    assert duplicates.count() == 2
    kept = {r["review_id"]: r["review_text"] for r in deduped.collect()}
    assert kept["r1"] == "newest"
