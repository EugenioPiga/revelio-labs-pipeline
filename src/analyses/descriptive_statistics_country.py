#!/usr/bin/env python3
"""
descriptive_statistics_country.py

Compute descriptive statistics on inventors by country definitions:
- Work country (dominant job country by tenure)
- First education country
- Last education country

Usage:
    python descriptive_statistics_country.py \
        --file /labs/khanna/linkedin_202507/processed/inventor_position_with_education \
        --start_year 2010
"""

import argparse
import os
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt


def save_barplot(series: pd.Series, title: str, xlabel: str, ylabel: str, filename: str, topn=20):
    """Save barplot for grouped values."""
    plt.figure(figsize=(10, 6))
    series = series.sort_values(ascending=False).head(topn)
    plt.bar(series.index.astype(str), series.values,
            color="steelblue", edgecolor="black")
    plt.xticks(rotation=90)
    plt.title(title)
    plt.xlabel(xlabel)
    plt.ylabel(ylabel)
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"Saved barplot: {filename}")


def main(file_path: str, start_year: int):
    print(f"Loading data from {file_path} ...")
    df = dd.read_parquet(os.path.join(file_path, "*.parquet"))

    # Parse dates
    df["startdate"] = dd.to_datetime(df["startdate"], errors="coerce")
    df["enddate"] = dd.to_datetime(df["enddate"], errors="coerce")

    cutoff = pd.Timestamp(f"{start_year}-01-01")
    df_cut = df[df["startdate"] >= cutoff]

    outdir = "output"
    os.makedirs(outdir, exist_ok=True)

    # Compute tenure at position level
    df_cut = df_cut.assign(tenure_days=(df_cut["enddate"] - df_cut["startdate"]).dt.days.fillna(0))

    # Move to pandas before ranking / dropping duplicates
    dominant_job = (
        df_cut.groupby(["user_id", "country"])["tenure_days"]
        .sum()
        .reset_index()
        .compute()  # <-- ensure pandas here
        .sort_values(["user_id", "tenure_days"], ascending=[True, False])
    )

    # Select dominant country per inventor
    dominant_job_country = (
        dominant_job.drop_duplicates("user_id")
        .set_index("user_id")["country"]
    )

    # Aggregate inventor-level table
    agg = (
        df_cut.groupby("user_id")
        .agg({
            "num_patents": "sum",
            "first_university_country": "first",
            "last_university_country": "first",
        })
        .compute()
    )

    # Join dominant job country
    agg = agg.join(dominant_job_country, on="user_id")
    agg = agg.rename(columns={"country": "dominant_job_country"})

    # --- 1. Patents by dominant job country
    job_counts = agg.groupby("dominant_job_country")["num_patents"].sum()
    save_barplot(
        job_counts,
        "Patents by Dominant Job Country",
        "Job Country",
        "Total Patents",
        os.path.join(outdir, "patents_by_job_country.png"),
    )

    # --- 2. Patents by first education country
    edu_first_counts = agg.groupby("first_university_country")["num_patents"].sum()
    save_barplot(
        edu_first_counts,
        "Patents by First Education Country",
        "First Education Country",
        "Total Patents",
        os.path.join(outdir, "patents_by_first_edu_country.png"),
    )

    # --- 3. Patents by last education country
    edu_last_counts = agg.groupby("last_university_country")["num_patents"].sum()
    save_barplot(
        edu_last_counts,
        "Patents by Last Education Country",
        "Last Education Country",
        "Total Patents",
        os.path.join(outdir, "patents_by_last_edu_country.png"),
    )

    print("\n=== Done! Results saved in", outdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file",
        type=str,
        default="/labs/khanna/linkedin_202507/processed/inventor_position_with_education",
        help="Path to Parquet file or directory",
    )
    parser.add_argument("--start_year", type=int, default=2010,
                        help="Cutoff year for analysis")
    args = parser.parse_args()
    main(args.file, args.start_year)

