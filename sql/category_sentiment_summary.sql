SELECT category, total_reviews, avg_rating, avg_sentiment_score,
       positive_review_count, negative_review_count, neutral_review_count
FROM category_sentiment_summary
ORDER BY avg_sentiment_score DESC;
