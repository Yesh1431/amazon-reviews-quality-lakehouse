from src.cleaning import clean_and_normalize


def test_duplicate_removal(spark):
    rows = [
        {"review_id": "1", "product_id": "p", "user_id": "u", "review_text": "  hi  ", "review_title": " t ", "star_rating": 5, "helpful_votes": 1, "review_date": "2020-01-01", "category": " Electronics ", "product_title": "A"},
        {"review_id": "1", "product_id": "p", "user_id": "u", "review_text": "  hi  ", "review_title": " t ", "star_rating": 5, "helpful_votes": 1, "review_date": "2020-01-01", "category": " Electronics ", "product_title": "A"},
    ]
    df = spark.createDataFrame(rows)
    out = clean_and_normalize(df)
    assert out.count() == 1


def test_missing_verified_defaults_false(spark):
    df = spark.createDataFrame([
        {"review_id": "2", "product_id": "p2", "user_id": "u2", "review_text": "works", "review_title": "ok", "star_rating": 4, "helpful_votes": 0, "review_date": "2020-01-02", "category": "electronics", "product_title": "B"}
    ])
    row = clean_and_normalize(df).collect()[0]
    assert row["verified_purchase"] is False


def test_verified_casted_when_present(spark):
    df = spark.createDataFrame([
        {"review_id": "3", "product_id": "p3", "user_id": "u3", "review_text": "nice", "review_title": "good", "star_rating": 5, "helpful_votes": 1, "review_date": "2020-01-03", "category": "electronics", "product_title": "C", "verified": "true"}
    ])
    row = clean_and_normalize(df).collect()[0]
    assert row["verified_purchase"] is True
