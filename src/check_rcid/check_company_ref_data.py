"""Aggregate company reference parquet files and report missing counts."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("/labs/khanna/linkedin_202507/academic_company_ref")


def compile_company_reference_frames(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Load and concatenate every parquet file in the directory."""
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    frames = [pd.read_parquet(file) for file in parquet_files]
    return pd.concat(frames, ignore_index=True)


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


def main() -> None:
    df = compile_company_reference_frames()
    summary = summarize_missing_values(df)

    print(f"Compiled {len(df):,} rows from {DATA_DIR}")
    print("\nMissing values by column:")
    print(summary.to_string())


if __name__ == "__main__":
    main()
