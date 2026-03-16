SELECT product_id, product_title, category, total_reviews, avg_rating, avg_helpful_votes, avg_sentiment_score
FROM product_review_summary
ORDER BY total_reviews DESC;
