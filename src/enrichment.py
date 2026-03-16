from typing import Dict, List
from textblob import TextBlob
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, DoubleType


KEYWORDS: Dict[str, List[str]] = {
    "delivery": ["delivery", "ship", "shipping", "arrived"],
    "packaging": ["package", "packaging", "box", "sealed"],
    "product_quality": ["quality", "build", "material"],
    "durability": ["durable", "durability", "last", "broke"],
    "price": ["price", "cost", "value", "expensive", "cheap"],
    "customer_service": ["support", "service", "refund", "seller"],
    "usability": ["easy", "use", "setup", "install"],
    "defects": ["defect", "broken", "fault", "issue"],
}


def _polarity(text: str) -> float:
    if not text:
        return 0.0
    return float(TextBlob(text).sentiment.polarity)


def _label(score: float) -> str:
    if score > 0.1:
        return "positive"
    if score < -0.1:
        return "negative"
    return "neutral"


def _theme(text: str) -> str:
    txt = (text or "").lower()
    for theme, words in KEYWORDS.items():
        if any(w in txt for w in words):
            return theme
    return "other"


def enrich_with_nlp(df: DataFrame) -> DataFrame:
    polarity_udf = F.udf(_polarity, DoubleType())
    label_udf = F.udf(_label, StringType())
    theme_udf = F.udf(_theme, StringType())

    return (
        df.withColumn("sentiment_score", polarity_udf(F.col("review_text")))
        .withColumn("sentiment_label", label_udf(F.col("sentiment_score")))
        .withColumn("keyword_theme", theme_udf(F.col("review_text")))
    )
