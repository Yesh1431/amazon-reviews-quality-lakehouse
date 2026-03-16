from pyspark.sql import functions as F
from src.cleaning import clean_and_normalize


def test_clean_and_normalize(spark):
    rows = [
        {
            "review_id": "1", "product_id": "p", "user_id": "u",
            "review_text": "  hi  ", "review_title": " t ",
            "star_rating": 5, "helpful_votes": 1,
            "review_date": "2020-01-01", "category": " Electronics ",
            "product_title": "A", "verified": True,
        },
        {
            "review_id": "2", "product_id": "p", "user_id": "u",
            "review_text": "  hello  ", "review_title": " s ",
            "star_rating": 4, "helpful_votes": 0,
            "review_date": "2020-02-01", "category": " Electronics ",
            "product_title": "A", "verified": False,
        },
    ]
    df = spark.createDataFrame(rows)
    out = clean_and_normalize(df)
    assert out.count() == 2
    row = out.filter(F.col("review_id") == "1").first()
    assert row["review_text"] == "hi"
    assert row["review_title"] == "t"
    assert row["category"] == "electronics"
    assert row["verified_purchase"] is True
