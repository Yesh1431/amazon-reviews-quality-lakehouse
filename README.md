# amazon-reviews-quality-lakehouse

Production-style, local-first **Medallion data lakehouse** for Amazon Electronics reviews.

## Business Problem
E-commerce teams need trusted review intelligence for product quality, customer satisfaction, and operational themes (delivery, packaging, defects). Raw review feeds are noisy and cannot be queried safely for decision-making.

## Solution Overview
This repository builds a strict batch pipeline that:
- Ingests UCSD Amazon Electronics reviews (`Electronics.json.gz`).
- Normalizes source data into a canonical schema.
- Enforces data quality and quarantine routing.
- Enriches reviews with sentiment and keyword themes.
- Publishes analytics-ready gold marts with zero-null critical fields.

## Architecture Diagram
```text
Raw JSON (.gz)
   -> Bronze (raw + metadata)
   -> Silver (clean + normalized + deduped)
   -> NLP Enrichment (sentiment/theme)
   -> Gold marts (facts/dims/aggregates)
   -> SQL analytics
             \-> Quarantine (invalid records + reasons)
```

## Canonical Schema Mapping
| Source | Canonical |
|---|---|
| reviewerID | user_id |
| asin | product_id |
| reviewText | review_text |
| summary | review_title |
| overall | star_rating |
| helpful[0] | helpful_votes |
| reviewTime | review_date |
| derived hash | review_id |
| literal | category |
| title/fallback | product_title |

## Data Quality Philosophy
Critical checks include:
- schema/required columns
- duplicate `review_id`
- accepted values (`star_rating` 1–5)
- non-negative helpful votes
- sentiment label validity
- future-date detection
- row-count reconciliation
- final zero-null assertion on curated outputs

Pipeline fails on critical violations.

## Quarantine Strategy
Records failing quality rules are routed to `data/quarantine/` with:
- `rejection_reason`
- `pipeline_run_id`
- `source_file`
- `ingestion_ts`

## Tech Stack
- Python 3.11
- PySpark
- SQL
- TextBlob NLP
- pytest
- Docker + docker-compose
- GitHub Actions CI

## Project Structure
See folders:
- `src/` pipeline modules
- `sql/` analytics queries
- `tests/` automated tests
- `docs/` architecture + dictionary + deployment

## Local Run
1. Install deps:
   ```bash
   make install
   ```
2. (Optional) Download full dataset:
   ```bash
   curl -L -o data/raw/Electronics.json.gz https://datarepo.eng.ucsd.edu/mcauley_group/data/amazon_v2/categoryFiles/Electronics.json.gz
   ```
   A starter sample file is already included at `data/raw/sample_electronics.json` for quick local runs.
3. Run pipeline:
   ```bash
   make run
   ```

## CLI
```bash
python -m src.cli \
  --input-path data/raw \
  --bronze-path data/bronze \
  --silver-path data/silver \
  --gold-path data/gold \
  --quarantine-path data/quarantine
```

## Testing
```bash
make test
```

## Notebook
Use `notebooks/walkthrough.ipynb` to inspect the sample input and generated silver/gold outputs after running the pipeline.

## Sample Gold Outputs
- `gold/fact_reviews`
- `gold/dim_products`
- `gold/daily_review_metrics`
- `gold/category_sentiment_summary`
- `gold/product_review_summary`
- `gold/negative_theme_summary`

## SQL Questions Answered
- Best sentiment categories
- Highest rated products
- Most reviewed products
- Rating trends over time
- Dominant negative themes
- Categories with most helpful votes

## Deployment
- Local via `make run`
- Containerized via `docker compose up`
- CI via `.github/workflows/ci.yml`

## Future Improvements
- Delta Lake + merge semantics
- Great Expectations integration
- Incremental partitioned loads
- Airflow orchestration
- Dashboard layer (Superset/Power BI)
