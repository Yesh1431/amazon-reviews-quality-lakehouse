SELECT review_date, category, total_reviews, avg_rating, avg_sentiment_score, total_helpful_votes
FROM daily_review_metrics
ORDER BY review_date, category;
