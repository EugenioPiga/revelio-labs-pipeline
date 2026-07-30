"""Check whether user_id ↔ pv_inventor_id mapping is one-to-one."""

from pathlib import Path

import pandas as pd

DATA_DIR = Path("/labs/khanna/linkedin_202507/revelio_patents_inventor_matches")
SAMPLE_LIMIT = 5
COLUMNS = [
    "user_id",
    "pv_inventor_id",
    "user_fullname_cleaned",
    "inventor_fullname_cleaned",
]


def load_pairs(data_dir: Path = DATA_DIR) -> pd.DataFrame:
    """Read every parquet shard and keep the (user_id, pv_inventor_id) pairs."""
    parquet_files = sorted(data_dir.glob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No parquet files found in {data_dir}")

    frames = [pd.read_parquet(path, columns=COLUMNS) for path in parquet_files]
    df = pd.concat(frames, ignore_index=True)
    df = df.dropna(subset=["user_id", "pv_inventor_id"])
    df["user_id"] = df["user_id"].astype(str).str.strip()
    df["pv_inventor_id"] = df["pv_inventor_id"].astype(str).str.strip()
    unique_pairs = df.drop_duplicates(subset=["user_id", "pv_inventor_id"])
    print(
        f"Loaded {len(df):,} rows from {len(parquet_files)} file(s) "
        f"and found {len(unique_pairs):,} unique pairs."
    )
    return unique_pairs


def show_duplicates(pairs: pd.DataFrame, key_col: str, value_col: str) -> None:
    """Print identifiers that map to more than one counterpart."""
    counts = pairs.groupby(key_col)[value_col].nunique()
    duplicate_keys = counts[counts > 1].index
    duplicate_count = len(duplicate_keys)

    if duplicate_count == 0:
        print(f"{key_col}: 0 entries map to multiple {value_col}.")
        return

    print(f"{key_col}: {duplicate_count:,} entries map to multiple {value_col}.")

    sample_keys = list(duplicate_keys[:SAMPLE_LIMIT])
    sample = (
        pairs[pairs[key_col].isin(sample_keys)]
        .sort_values([key_col, value_col])
        .reset_index(drop=True)
    )
    columns_to_show = [
        col
        for col in [key_col, value_col, "user_fullname_cleaned", "inventor_fullname_cleaned"]
        if col in sample.columns
    ]
    print(f"Example {key_col} duplicates:")
    print(sample[columns_to_show].to_string(index=False))


def main() -> None:
    pairs = load_pairs()
    show_duplicates(pairs, key_col="user_id", value_col="pv_inventor_id")
    show_duplicates(pairs, key_col="pv_inventor_id", value_col="user_id")


if __name__ == "__main__":
    main()
