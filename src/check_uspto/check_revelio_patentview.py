#!/usr/bin/env python3
"""Sanity-check overlaps between Revelio matches and PatentsView merges."""

import os

import pandas as pd

MERGED_DATA = os.path.expanduser(
    "~/code/revelio-labs-pipeline/data/revelio_patents_inventor_matches_with_USPTO.parquet"
)


def load_merged_frame(path: str) -> pd.DataFrame:
    """Load the premerged coverage dataset."""
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"Merged coverage file not found: {path}. "
            "Run merge_revelio_patentview.py first."
        )
    cols = [
        "patent_id",
        "inventor_id",
        "inventor_sequence",
        "patent_date",
        "pv_inventor_id",
        "pv_patent_date",
        "filing_date",
        "user_id",
        "_merge",
    ]
    return pd.read_parquet(path, columns=cols)


def analyze_matches(df: pd.DataFrame) -> None:
    """Print match statistics for rows joined on both datasets."""
    both_df = df[df["_merge"] == "both"].copy()
    total_both = len(both_df)
    print(f"[INFO] Rows present in both datasets: {total_both:,}")
    if total_both == 0:
        print("[WARN] No overlapping rows to analyze.")
        return
    inventors_match = (both_df["inventor_id"] == both_df["pv_inventor_id"]).sum()
    both_df["patent_date_dt"] = pd.to_datetime(both_df["patent_date"], errors="coerce")
    both_df["pv_patent_date_dt"] = pd.to_datetime(both_df["pv_patent_date"], errors="coerce")
    patent_dates_match = (both_df["patent_date_dt"] == both_df["pv_patent_date_dt"]).sum()
    print(f"[STATS] inventor_id == pv_inventor_id: {inventors_match:,}")
    print(f"[STATS] patent_date == pv_patent_date: {patent_dates_match:,}")


def main():
    merged_df = load_merged_frame(MERGED_DATA)
    analyze_matches(merged_df)


if __name__ == "__main__":
    main()
