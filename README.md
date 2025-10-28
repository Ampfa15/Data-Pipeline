# Simple Data Pipeline (CSV -> Parquet + DuckDB)

This project extracts a CSV, applies a small transform with pandas, and loads the result into Parquet and a DuckDB warehouse. It includes pytest tests and a CLI.

## Setup

```bash
cd data-pipeline
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Run

```bash
python -m src.main --csv sample_data/sales.csv --out outputs
```

Outputs written to `outputs/`:
- `dataset.parquet`
- `warehouse.duckdb` (table `analytics.dataset`)

## Test

```bash
pytest -q
```

## Code
- `src/etl.py`: `extract`, `transform`, `load`
- `src/main.py`: CLI wrapper
- `sample_data/sales.csv`: sample dataset

## Why this is useful
- Shows a clean, minimal ETL that recruiters can run quickly
- Demonstrates pandas transforms and column hygiene
- Produces analytics-friendly Parquet + DuckDB for BI or queries
