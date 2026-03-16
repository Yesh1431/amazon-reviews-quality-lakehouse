# Data Dictionary

## Canonical review schema
- review_id: Deterministic hash key.
- product_id: Amazon ASIN.
- user_id: Reviewer identifier.
- review_text: Free-form review body.
- review_title: Review summary/title.
- star_rating: Integer rating [1-5].
- helpful_votes: Non-negative helpful vote count.
- review_date: Parsed review date.
- category: Normalized category value.
- product_title: Product title from source or fallback.
