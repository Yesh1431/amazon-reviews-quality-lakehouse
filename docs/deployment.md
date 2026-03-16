# Deployment

## Local
- `make install`
- Download Electronics dataset to `data/raw/`
- `make run`

## Docker
- `docker compose build`
- `docker compose up`

## CI
GitHub Actions executes `pytest` on push.
