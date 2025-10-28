from __future__ import annotations

import click
from pathlib import Path
from .etl import extract, transform, load


@click.command()
@click.option("--csv", "csv_path", type=click.Path(exists=True, dir_okay=False, path_type=Path), required=True, help="Path to input CSV")
@click.option("--out", "out_dir", type=click.Path(file_okay=False, path_type=Path), default=Path("outputs"))
def cli(csv_path: Path, out_dir: Path):
    df = extract(csv_path)
    df = transform(df)
    result = load(df, out_dir)
    click.echo(f"Wrote Parquet: {result['parquet']}")
    click.echo(f"DuckDB: {result['duckdb']}")
    click.echo(f"Rows: {result['rows']}")


if __name__ == "__main__":
    cli()


