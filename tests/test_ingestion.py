from src.schema_mapping import map_to_canonical_schema


def test_schema_mapping(spark):
    df = spark.createDataFrame([
        {
            "reviewerID": "u1",
            "asin": "p1",
            "reviewText": "Great quality",
            "summary": "Great",
            "overall": 5.0,
            "helpful": [2, 3],
            "reviewTime": "01 01, 2020",
            "unixReviewTime": 1577836800,
            "title": "Item 1",
        }
    ])
    out = map_to_canonical_schema(df, "electronics")
    assert set(["review_id", "product_id", "user_id", "review_text", "review_title", "star_rating", "helpful_votes", "review_date", "category", "product_title"]).issubset(set(out.columns))
