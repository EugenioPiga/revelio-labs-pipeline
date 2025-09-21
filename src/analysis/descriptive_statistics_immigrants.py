#!/usr/bin/env python3
"""
descriptive_statistics_immigrants.py

Compute immigrant-focused descriptive statistics on
inventor_position_with_education.

Usage:
    python descriptive_statistics_immigrants.py \
        --file /labs/khanna/linkedin_202507/processed/inventor_position_with_education \
        --start_year 2010
"""

import argparse
import os
import dask.dataframe as dd
import pandas as pd
import matplotlib.pyplot as plt


def winsorize(series: pd.Series, lower=0.01, upper=0.99):
    """Clip a series at given percentiles."""
    lo, hi = series.quantile([lower, upper])
    return series.clip(lower=lo, upper=hi)


def save_hist(series: pd.Series, title: str, filename: str):
    """Save histogram with mean line."""
    plt.figure(figsize=(8, 6))
    series = series.dropna()
    plt.hist(series, bins=50, color="steelblue", edgecolor="black", alpha=0.7)
    mean_val = series.mean()
    plt.axvline(mean_val, color="red", linestyle="--", linewidth=2,
                label=f"Mean = {mean_val:.2f}")
    plt.title(title)
    plt.xlabel("Value")
    plt.ylabel("Frequency")
    plt.legend()
    plt.tight_layout()
    plt.savefig(filename)
    plt.close()
    print(f"Saved histogram: {filename}")


def save_barplot(series: pd.Series, title: str, xlabel: str, ylabel: str, filename: str):
    """Save barplot for grouped values."""
    plt.figure(figsize=(10, 6))
    series = series.sort_values(ascending=False)
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

    # --- Immigrant flag (education country != job country)
    immigrant_flag = (
        (df_cut["university_country"].notnull()) &
        (df_cut["country"].notnull()) &
        (df_cut["university_country"] != df_cut["country"])
    ).astype(int)
    df_cut = df_cut.assign(is_immigrant=immigrant_flag)

    # --- Tenure per position
    tenure = (df_cut["enddate"] - df_cut["startdate"]).dt.days.fillna(0)
    df_cut = df_cut.assign(tenure_days=tenure)

    # --- Aggregate to inventor level (one row per inventor)
    agg = (
        df_cut.groupby("user_id")
        .agg({
            "is_immigrant": "max",      # immigrant if ever
            "num_patents": "sum",
            "university_country": "first",  # first education = origin
        })
        .compute()
        .reset_index()
        .drop_duplicates("user_id")
        .set_index("user_id")
    )

    # --- Dominant job country by total tenure
    dominant_job = (
        df_cut.groupby(["user_id", "country"])["tenure_days"]
        .sum()
        .reset_index()
        .compute()
        .sort_values(["user_id", "tenure_days"], ascending=[True, False])
    )

    dominant_job_country = (
        dominant_job.drop_duplicates("user_id")
        .set_index("user_id")["country"]
    )
    dominant_job_country = dominant_job_country[~dominant_job_country.index.duplicated(keep="first")]

    # Join
    agg = agg.join(dominant_job_country.rename("job_country"))

    # --- 1. Immigrant share
    immigrant_share = agg["is_immigrant"].value_counts(normalize=True)
    save_barplot(
        immigrant_share,
        "Share of Immigrant vs. Non-Immigrant Inventors",
        "Immigrant (0=No, 1=Yes)",
        "Share",
        os.path.join(outdir, "immigrant_share.png"),
    )

    # --- 2. Patents distribution
    save_hist(
        winsorize(agg["num_patents"]),
        "Distribution of Patents per Inventor (Winsorized)",
        os.path.join(outdir, "patents_hist.png"),
    )

    # --- 3. Mean patents by immigrant status
    mean_patents = agg.groupby("is_immigrant")["num_patents"].mean()
    save_barplot(
        mean_patents,
        "Mean Patents by Immigrant Status",
        "Immigrant (0=No, 1=Yes)",
        "Mean Patents",
        os.path.join(outdir, "patents_by_immigrant.png"),
    )

    # --- 4. Education country distribution (immigrants only, top 20)
    edu_counts = agg.loc[agg["is_immigrant"] == 1, "university_country"].value_counts().head(20)
    save_barplot(
        edu_counts,
        "Top 20 Education Countries of Immigrant Inventors",
        "Education Country",
        "Count",
        os.path.join(outdir, "immigrants_by_edu_country.png"),
    )

    # --- 5. Job country distribution (immigrants only, top 20)
    job_counts = agg.loc[agg["is_immigrant"] == 1, "job_country"].value_counts().head(20)
    save_barplot(
        job_counts,
        "Top 20 Work Countries of Immigrant Inventors",
        "Job Country",
        "Count",
        os.path.join(outdir, "immigrants_by_job_country.png"),
    )

    # --- 6. Flows (education → job, top 20)
    flows = (
        agg.loc[agg["is_immigrant"] == 1]
        .groupby(["university_country", "job_country"])
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
        .head(20)
    )

    plt.figure(figsize=(10, 6))
    plt.barh(
        flows.apply(lambda x: f"{x['university_country']} → {x['job_country']}", axis=1),
        flows["count"],
        color="steelblue",
    )
    plt.xlabel("Number of Immigrant Inventors")
    plt.title("Top 20 Education → Job Country Flows")
    plt.tight_layout()
    plt.savefig(os.path.join(outdir, "immigrant_flows.png"))
    plt.close()
    print("Saved barplot: immigrant_flows.png")

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

