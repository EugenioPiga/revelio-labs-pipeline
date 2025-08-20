#!/usr/bin/env python
# patent_inventor_sizes.py
#
# Computes number of inventors per patent (patent size).
# Includes sanity checks with progress prints.

import dask.dataframe as dd
import os

INPUT_PATH  = "revelio_patents_inventor_matches/*.parquet"
OUTPUT_PATH = "output/patent_inventor_sizes.parquet"

def main():
    print("=== Loading inventor-level patent dataset ===")
    df = dd.read_parquet(INPUT_PATH)
    print("Columns:", df.columns)
    print("Number of partitions:", df.npartitions)

    # --- Step 1: Original row count
    print("Computing original row count...")
    n_rows_original = df.shape[0].compute()
    print(f"Original dataset rows (inventor-level): {n_rows_original:,}")

    # --- Step 2: Aggregation
    print("Aggregating inventor counts per patent_id...")
    patent_sizes = (
        df.groupby("patent_id")
          .size()
          .to_frame("inventor_count")
          .reset_index()
    )

    # --- Step 3: Row count check
    print("Checking row count match...")
    n_rows_agg = patent_sizes["inventor_count"].sum().compute()
    print(f"Sum of inventor counts after aggregation: {n_rows_agg:,}")
    if n_rows_original == n_rows_agg:
        print("✅ Row count matches exactly.")
    else:
        print("❌ Row count mismatch! Check your aggregation logic.")

    # --- Step 4: Unique patent IDs
    print("Checking unique patent IDs...")
    n_unique_before = df["patent_id"].nunique().compute()
    n_unique_after  = patent_sizes["patent_id"].nunique().compute()
    print(f"Unique patents before: {n_unique_before:,}")
    print(f"Unique patents after : {n_unique_after:,}")
    if n_unique_before == n_unique_after:
        print("✅ Unique patent counts match.")
    else:
        print("❌ Patent IDs missing after aggregation!")

    # --- Step 5: Distribution of inventor counts
    print("Computing inventor count distribution...")
    stats = patent_sizes["inventor_count"].describe().compute()
    print(stats)

    # --- Step 6: Save result
    print(f"Saving inventor counts to {OUTPUT_PATH} ...")
    patent_sizes.to_parquet(OUTPUT_PATH, overwrite=True)
    print("✅ Done. File written.")

if __name__ == "__main__":
    main()
