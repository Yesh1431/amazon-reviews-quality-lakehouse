from src.marts import build_daily_review_metrics


def test_gold_aggregation(spark):
    df = spark.createDataFrame([
        {"review_date": "2020-01-01", "category": "electronics", "star_rating": 5, "sentiment_score": 0.8, "helpful_votes": 2},
        {"review_date": "2020-01-01", "category": "electronics", "star_rating": 3, "sentiment_score": 0.0, "helpful_votes": 1},
    ])
    out = build_daily_review_metrics(df).collect()[0]
    assert out["total_reviews"] == 2
