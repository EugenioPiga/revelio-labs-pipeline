"""Load only the first academic company reference parquet to validate access."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("/labs/khanna/linkedin_202507/academic_company_ref")


def load_first_parquet(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Return the DataFrame from the first parquet file found in the directory."""
    first_file = next(iter(sorted(data_dir.glob("*.parquet"))), None)
    if first_file is None:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")
    return pd.read_parquet(first_file)


def summarize_missing_values(df: pd.DataFrame) -> pd.DataFrame:
    """Return missing counts and percentages for every column."""
    total_rows = len(df)
    missing_counts = df.isna().sum()
    summary = pd.DataFrame(
        {
            "missing": missing_counts,
            "missing_pct": (missing_counts / total_rows * 100).round(2),
        }
    )
    return summary.sort_values(by="missing", ascending=False)


def print_first_rows_by_column(df: pd.DataFrame, num_rows: int = 5) -> None:
    """Print the first `num_rows` values for each column separately."""
    for column in df.columns:
        print(f"\nColumn: {column}")
        print(df[column].head(num_rows).to_string(index=False))


def main() -> None:
    df = load_first_parquet()
    summary = summarize_missing_values(df)

    print(
        f"Loaded first parquet file with {len(df):,} rows and "
        f"{df.shape[1]} columns from {DATA_DIR}"
    )
    print("\nFirst five rows by column:")
    print_first_rows_by_column(df)
    print("\nMissing values by column (first file only):")
    print(summary.to_string())


if __name__ == "__main__":
    main()
