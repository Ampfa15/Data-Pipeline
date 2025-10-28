from __future__ import annotations

import pandas as pd
import duckdb
from pathlib import Path


def extract(csv_path: Path) -> pd.DataFrame:
    df = pd.read_csv(csv_path)
    return df


def transform(df: pd.DataFrame) -> pd.DataFrame:
    # Simple clean: drop empty rows, standardize column names, compute totals if present
    df = df.dropna(how="all").copy()
    df.columns = [c.strip().lower().replace(" ", "_") for c in df.columns]
    # Example: if quantity and price columns exist, add total
    if set(["quantity", "price"]).issubset(df.columns):
        df["total"] = df["quantity"].fillna(0) * df["price"].fillna(0)
    return df


def load(df: pd.DataFrame, out_dir: Path) -> dict:
    out_dir.mkdir(parents=True, exist_ok=True)
    parquet_path = out_dir / "dataset.parquet"
    duckdb_path = out_dir / "warehouse.duckdb"

    # Write Parquet
    df.to_parquet(parquet_path, index=False)

    # Load to DuckDB
    con = duckdb.connect(str(duckdb_path))
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS analytics;")
        con.execute("CREATE TABLE IF NOT EXISTS analytics.dataset AS SELECT * FROM read_parquet(?) WITH NO DATA;", [str(parquet_path)])
        con.execute("DELETE FROM analytics.dataset;")
        con.execute("INSERT INTO analytics.dataset SELECT * FROM read_parquet(?);", [str(parquet_path)])
        rows = con.execute("SELECT COUNT(*) FROM analytics.dataset;").fetchone()[0]
    finally:
        con.close()

    return {
        "parquet": str(parquet_path),
        "duckdb": str(duckdb_path),
        "rows": int(rows),
    }


