from pathlib import Path
import pandas as pd

from src.etl import extract, transform, load


def test_end_to_end(tmp_path: Path):
    csv = Path(__file__).parents[1] / "sample_data" / "sales.csv"
    df = extract(csv)
    tdf = transform(df)
    result = load(tdf, tmp_path)
    assert result["rows"] == len(tdf)
    # Verify Parquet exists
    assert Path(result["parquet"]).exists()
    # Verify DuckDB exists
    assert Path(result["duckdb"]).exists()


