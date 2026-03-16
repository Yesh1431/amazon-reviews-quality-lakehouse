# Architecture

The project follows a Medallion architecture:

1. **Bronze**: Raw source with pipeline metadata.
2. **Silver**: Canonical schema, cleaning, dedupe, validation, NLP enrichment.
3. **Gold**: Analytics marts for BI/reporting.
4. **Quarantine**: Invalid records with rejection metadata.
