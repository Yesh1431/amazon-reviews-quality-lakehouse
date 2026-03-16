-- Best sentiment categories
SELECT category, avg_sentiment_score FROM category_sentiment_summary ORDER BY avg_sentiment_score DESC;

-- Highest rated products
SELECT product_id, product_title, avg_rating FROM product_review_summary ORDER BY avg_rating DESC LIMIT 20;

-- Most reviewed products
SELECT product_id, product_title, total_reviews FROM product_review_summary ORDER BY total_reviews DESC LIMIT 20;

-- Ratings trend over time
SELECT review_date, avg_rating FROM daily_review_metrics ORDER BY review_date;

-- Dominant negative themes
SELECT category, keyword_theme, SUM(negative_review_count) AS negatives
FROM negative_theme_summary GROUP BY category, keyword_theme ORDER BY negatives DESC;

-- Most helpful-vote categories
SELECT category, SUM(total_helpful_votes) AS helpful_votes
FROM daily_review_metrics GROUP BY category ORDER BY helpful_votes DESC;
