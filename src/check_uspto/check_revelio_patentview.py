#!/usr/bin/env python3
"""Sanity-check overlaps between Revelio matches and PatentsView merges."""

import os

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
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


def report_both_fractions(df: pd.DataFrame) -> None:
    """Report row- and patent-level overlap fractions by patent year."""
    total_rows = len(df)
    if total_rows == 0:
        print("[WARN] No rows available to check overlaps.")
        return
    both_df = df[df["_merge"] == "both"].copy()
    both_count = len(both_df)
    overall_row_fraction = both_count / total_rows
    print(
        "[INFO] Rows present in both datasets (_merge == 'both'): "
        f"{both_count:,} ({overall_row_fraction:.2%} of all rows)"
    )
    df = df.assign(
        patent_year=pd.to_datetime(df["patent_date"], errors="coerce").dt.year
    )
    valid_df = df[df["patent_year"].notna()].copy()
    if valid_df.empty:
        print("[WARN] No valid patent dates available to plot yearly fractions.")
        return
    valid_df["patent_year"] = valid_df["patent_year"].astype(int)
    #
    # Row-level view (patent_id + inventor sequence granularity)
    yearly_totals = valid_df.groupby("patent_year").size()
    yearly_both = (
        valid_df[valid_df["_merge"] == "both"].groupby("patent_year").size()
    )
    yearly_both = yearly_both.reindex(yearly_totals.index, fill_value=0)
    row_fraction_by_year = (yearly_both / yearly_totals).sort_index()
    print("[INFO] Row-level overlap fraction by patent year:")
    for year, frac in row_fraction_by_year.items():
        print(f"  {year}: {frac:.2%}")
    #
    # Patent-level view (does a patent have at least one overlapping inventor)
    def has_both(series: pd.Series) -> bool:
        return (series == "both").any()
    #
    patent_groups = (
        valid_df.groupby("patent_id")
        .agg(
            patent_year=("patent_year", "first"),
            has_both=("_merge", has_both),
        )
        .dropna(subset=["patent_year"])
    )
    total_patents = len(patent_groups)
    if total_patents == 0:
        print("[WARN] No patents with valid patent years after grouping.")
        return
    patents_with_both = patent_groups["has_both"].sum()
    patent_fraction = patents_with_both / total_patents
    print(
        "[INFO] Patents with >=1 overlapping inventor: "
        f"{patents_with_both:,} ({patent_fraction:.2%} of patents with valid patent_year)"
    )
    #
    yearly_patent_totals = patent_groups.groupby("patent_year").size()
    yearly_patent_with_both = (
        patent_groups[patent_groups["has_both"]]
        .groupby("patent_year")
        .size()
    )
    yearly_patent_with_both = yearly_patent_with_both.reindex(
        yearly_patent_totals.index, fill_value=0
    )
    patent_fraction_by_year = (
        yearly_patent_with_both / yearly_patent_totals
    ).sort_index()
    print("[INFO] Patent-level overlap fraction by patent year:")
    for year, frac in patent_fraction_by_year.items():
        print(f"  {year}: {frac:.2%}")
    #
    plt.figure(figsize=(10, 5))
    ax = row_fraction_by_year.plot(marker="o", label="Row fraction (_merge == 'both')")
    patent_fraction_by_year.plot(
        marker="s", label="Patent fraction (>=1 row with _merge == 'both')", ax=ax
    )
    ax.set_title("Overlap fractions by patent year")
    ax.set_ylabel("Share of records with _merge == 'both'")
    ax.set_xlabel("Patent year")
    ax.yaxis.set_major_formatter(mtick.PercentFormatter(1.0))
    ax.set_ylim(bottom=0)
    ax.legend()
    plt.tight_layout()
    plt.show()


def main():
    merged_df = load_merged_frame(MERGED_DATA)
    analyze_matches(merged_df)
    report_both_fractions(merged_df)


if __name__ == "__main__":
    main()
