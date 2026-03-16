from src.enrichment import enrich_with_nlp


def test_sentiment_enrichment(spark):
    df = spark.createDataFrame([{"review_text": "I love this product, excellent quality!"}])
    out = enrich_with_nlp(df).collect()[0]
    assert out["sentiment_label"] in {"positive", "neutral", "negative"}
    assert out["keyword_theme"] is not None
