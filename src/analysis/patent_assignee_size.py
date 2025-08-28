#!/usr/bin/env python
# patent_assignee_analysis.py
#
# 1. Computes number of assignees per patent.
# 2. Computes number of patents per assignee.
# Includes sanity checks with progress prints.

import dask.dataframe as dd
import os

INPUT_PATH = "revelio_patents_assignee_matches/*.parquet"   # <-- adjust if needed
OUTPUT_PATENT_VIEW = "output/patent_assignee_sizes.parquet"
OUTPUT_ASSIGNEE_VIEW = "output/assignee_patent_sizes.parquet"

def main():
    print("=== Loading assignee-level patent dataset ===")
    df = dd.read_parquet(INPUT_PATH)
    print("Columns:", df.columns)
    print("Number of partitions:", df.npartitions)

    # --- Step 1: Original row count
    print("Computing original row count...")
    n_rows_original = df.shape[0].compute()
    print(f"Original dataset rows (assignee-level): {n_rows_original:,}")

    # ------------------------------
    # Analysis A: Assignees per Patent
    # ------------------------------
    print("\n=== Analysis A: Assignees per Patent ===")
    patent_sizes = (
        df.groupby("patent_id")
          .size()
          .to_frame("assignee_count")
          .reset_index()
    )

    # Check row count
    n_rows_agg = patent_sizes["assignee_count"].sum().compute()
    print(f"Sum of assignee counts after aggregation: {n_rows_agg:,}")
    if n_rows_original == n_rows_agg:
        print("✅ Row count matches exactly.")
    else:
        print("❌ Row count mismatch!")

    # Unique patent IDs check
    n_unique_before = df["patent_id"].nunique().compute()
    n_unique_after  = patent_sizes["patent_id"].nunique().compute()
    print(f"Unique patents before: {n_unique_before:,} | after: {n_unique_after:,}")

    # Distribution
    stats_patent = patent_sizes["assignee_count"].describe().compute()
    print(stats_patent)

    # Save
    print(f"Saving assignee counts per patent to {OUTPUT_PATENT_VIEW} ...")
    patent_sizes.to_parquet(OUTPUT_PATENT_VIEW, overwrite=True)
    print("✅ Patent view written.")

    # ------------------------------
    # Analysis B: Patents per Assignee
    # ------------------------------
    print("\n=== Analysis B: Patents per Assignee ===")
    assignee_sizes = (
        df.groupby("assignee_id")
          .size()
          .to_frame("patent_count")
          .reset_index()
    )

    # Check row count
    n_rows_agg2 = assignee_sizes["patent_count"].sum().compute()
    print(f"Sum of patent counts after aggregation: {n_rows_agg2:,}")
    if n_rows_original == n_rows_agg2:
        print("✅ Row count matches exactly.")
    else:
        print("❌ Row count mismatch!")

    # Unique assignee IDs
    n_unique_assignees = df["assignee_id"].nunique().compute()
    n_unique_after2    = assignee_sizes["assignee_id"].nunique().compute()
    print(f"Unique assignees before: {n_unique_assignees:,} | after: {n_unique_after2:,}")

    # Distribution
    stats_assignee = assignee_sizes["patent_count"].describe().compute()
    print(stats_assignee)

    # Save
    print(f"Saving patent counts per assignee to {OUTPUT_ASSIGNEE_VIEW} ...")
    assignee_sizes.to_parquet(OUTPUT_ASSIGNEE_VIEW, overwrite=True)
    print("✅ Assignee view written.")

    print("\n=== All analyses complete ===")

if __name__ == "__main__":
    main()
