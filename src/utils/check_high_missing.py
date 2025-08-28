#!/usr/bin/env python
import os
import sys
import dask.dataframe as dd
import random

def check_high_missing(dataset_dir, threshold=0.8, preview_n=10000):
    parquet_files = [f for f in os.listdir(dataset_dir) if f.endswith(".parquet")]
    if not parquet_files:
        print(f"[ERROR] No parquet files found in {dataset_dir}")
        return

    fname = random.choice(parquet_files)
    fpath = os.path.join(dataset_dir, fname)
    print(f"\n[INFO] Checking random file: {fpath}")

    # Load
    df = dd.read_parquet(fpath, engine="pyarrow")

    # Compute missing stats
    n_total = df.shape[0].compute()
    missing_counts = df.isna().sum().compute()
    missing_frac = missing_counts / n_total

    # Filter to variables with > threshold missing
    high_missing = missing_frac[missing_frac > threshold].sort_values(ascending=False)

    if high_missing.empty:
        print(f"[INFO] No columns with more than {threshold*100:.0f}% missing")
        return

    print("\n=== High Missingness Columns ===")
    for col, frac in high_missing.items():
        print(f"{col:30} {frac*100:6.2f}% missing")

        # Preview first preview_n rows for each column
        try:
            preview = df[col].head(preview_n, compute=True)
            print(f"--- Preview (first {preview_n} rows of {col}) ---")
            print(preview)
        except Exception as e:
            print(f"[WARN] Could not preview {col}: {e}")
        print("")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python check_high_missing.py <dataset_dir>")
        sys.exit(1)

    dataset_dir = sys.argv[1]
    check_high_missing(dataset_dir)
