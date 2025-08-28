#!/usr/bin/env python
# patent_assignee_full_analysis.py
#
# Computes:
# 1. Distribution of patents across firms (rcid, company, primary_name, ultimate_parent_rcid)
# 2. Distribution of assignees per patent (rcid, company, primary_name)

import dask.dataframe as dd
import os

INPUT_PATH = "revelio_patents_assignee_matches/*.parquet"  # patent–assignee links
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)


def save_and_report(df, group_col, value_name, output_file):
    """Groupby, summarize, and save parquet output."""
    print(f"\n=== Grouping by {group_col} ===")
    grouped = (
        df.groupby(group_col)
          .size()
          .to_frame(value_name)
          .reset_index()
    )

    # Stats
    try:
        n_entities = grouped[group_col].nunique().compute()
        stats = grouped[value_name].describe().compute()
        print(f"Unique {group_col}: {n_entities:,}")
        print(stats)
    except Exception as e:
        print(f"⚠️ Could not compute stats for {group_col}: {e}")

    # Save
    outpath = os.path.join(OUTPUT_DIR, output_file)
    print(f"Saving results to {outpath} ...")
    grouped.to_parquet(outpath, overwrite=True)
    print("✅ Done.")
    return grouped


def main():
    print("=== Loading patent–assignee dataset ===")
    df = dd.read_parquet(INPUT_PATH)
    print("Columns:", list(df.columns))
    print("Number of partitions:", df.npartitions)

    n_rows = df.shape[0].compute()
    print(f"Rows (patent–assignee links): {n_rows:,}")

    # ------------------------------------------------------
    # 1. Patents per firm
    # ------------------------------------------------------
    print("\n=== Analysis 1: Patents per firm ===")

    if "rcid" in df.columns:
        save_and_report(df, "rcid", "patent_count", "patents_per_rcid.parquet")

    if "company" in df.columns:
        save_and_report(df, "company", "patent_count", "patents_per_company.parquet")

    if "primary_name" in df.columns:
        save_and_report(df, "primary_name", "patent_count", "patents_per_primary_name.parquet")

    if "ultimate_parent_rcid" in df.columns:
        save_and_report(df, "ultimate_parent_rcid", "patent_count",
                        "patents_per_parent_rcid.parquet")

    # ------------------------------------------------------
    # 2. Assignees per patent
    # ------------------------------------------------------
    print("\n=== Analysis 2: Assignees per patent ===")

    # RCID-level
    if "rcid" in df.columns:
        save_and_report(df[["patent_id", "rcid"]].drop_duplicates(),
                        "patent_id", "assignee_count_rcid",
                        "assignees_per_patent_rcid.parquet")

    # Company-level
    if "company" in df.columns:
        save_and_report(df[["patent_id", "company"]].drop_duplicates(),
                        "patent_id", "assignee_count_company",
                        "assignees_per_patent_company.parquet")

    # Primary_name-level
    if "primary_name" in df.columns:
        save_and_report(df[["patent_id", "primary_name"]].drop_duplicates(),
                        "patent_id", "assignee_count_primary_name",
                        "assignees_per_patent_primary_name.parquet")

    print("\n=== All analyses complete ===")


if __name__ == "__main__":
    main()
