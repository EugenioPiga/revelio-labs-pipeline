"""Peek at the first parquet shard of revelio_patents_inventor_matches."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("/labs/khanna/linkedin_202507/revelio_patents_inventor_matches")
ROW_LIMIT = 5


def main() -> None:
    parquet_files = sorted(DATA_DIR.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {DATA_DIR}")

    first_file = parquet_files[0]
    df = pd.read_parquet(first_file)

    print(f"Loaded {len(df):,} rows and {len(df.columns)} columns from {first_file}.")
    preview = df.head(ROW_LIMIT)
    print(f"\nFirst {ROW_LIMIT} rows (per column):")
    for column in preview.columns:
        print(f"\nColumn: {column}")
        print(preview[[column]].to_string(index=False, header=False))

    non_null_counts = df.notna().sum().sort_values(ascending=False)
    print("\nNon-missing counts per column:")
    for column, count in non_null_counts.items():
        print(f"{column}: {count:,}")


if __name__ == "__main__":
    main()
