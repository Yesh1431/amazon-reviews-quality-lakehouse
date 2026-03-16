from src.cleaning import clean_and_normalize


def test_duplicate_removal(spark):
    rows = [
        {"review_id": "1", "product_id": "p", "user_id": "u", "review_text": "  hi  ", "review_title": " t ", "star_rating": 5, "helpful_votes": 1, "review_date": "2020-01-01", "category": " Electronics ", "product_title": "A"},
        {"review_id": "1", "product_id": "p", "user_id": "u", "review_text": "  hi  ", "review_title": " t ", "star_rating": 5, "helpful_votes": 1, "review_date": "2020-01-01", "category": " Electronics ", "product_title": "A"},
    ]
    df = spark.createDataFrame(rows)
    out = clean_and_normalize(df)
    assert out.count() == 1
