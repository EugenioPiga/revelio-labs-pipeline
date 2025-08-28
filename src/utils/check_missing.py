#!/usr/bin/env python
import os
import sys
import dask.dataframe as dd
import random

def check_missing_onefile(dataset_dir, column):
    parquet_files = [f for f in os.listdir(dataset_dir) if f.endswith(".parquet")]
    if not parquet_files:
        print(f"[ERROR] No parquet files found in {dataset_dir}")
        return

    fname = random.choice(parquet_files)
    fpath = os.path.join(dataset_dir, fname)
    print(f"\n[INFO] Checking random file: {fpath}")

    # Load file
    df = dd.read_parquet(fpath, engine="pyarrow")

    # Figure out identifier column
    id_col = None
    if "rcid" in df.columns:
        id_col = "rcid"
    elif "user_id" in df.columns:
        id_col = "user_id"

    # Build subset for preview
    cols_to_show = []
    if id_col:
        cols_to_show.append(id_col)
    if column in df.columns:
        cols_to_show.append(column)

    # Head with only relevant columns
    try:
        preview = df[cols_to_show].head(5)
        print("\n=== Dataset Head (first 5 rows) ===")
        print(preview)
    except Exception as e:
        print(f"[WARN] Could not preview head: {e}")

    # Missingness
    n_total = df.shape[0].compute()
    n_missing = df[column].isna().sum().compute()
    print("\n=== Missingness Report ===")
    print(f"Column : {column}")
    print(f"Rows   : {n_total:,}")
    print(f"Missing: {n_missing:,} ({100.0*n_missing/n_total:.4f}%)")

    # Type check (smoking gun for mixed types)
    try:
        sample_types = df[column].dropna().head(1000, compute=True).apply(type).value_counts()
        print("\n=== Type Check (first 1000 non-null values) ===")
        print(sample_types)
    except Exception as e:
        print(f"[WARN] Could not perform type check: {e}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python check_missing.py <dataset_dir> <column>")
        sys.exit(1)

    dataset_dir = sys.argv[1]
    column = sys.argv[2]
    check_missing_onefile(dataset_dir, column)
