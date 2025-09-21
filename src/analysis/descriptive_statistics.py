#!/usr/bin/env python3
"""
descriptive_statistics.py
Compute descriptive statistics on inventor positions, firms, cities, and patents.
Applies winsorization (1st–99th percentile) for distributions to
reduce outlier influence in histograms.

Usage:
    python descriptive_statistics.py --file /labs/khanna/linkedin_202507/processed/inventor_position_file --start_year 2010
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
    """Save barplot for mean patents by group."""
    plt.figure(figsize=(10, 6))
    series = series.sort_index()
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
    df = dd.read_parquet(file_path)

    # Parse dates
    df["startdate"] = dd.to_datetime(df["startdate"], errors="coerce")
    df["enddate"] = dd.to_datetime(df["enddate"], errors="coerce")
    df["first_patent_date"] = dd.to_datetime(df["first_patent_date"], errors="coerce")
    df["last_patent_date"] = dd.to_datetime(df["last_patent_date"], errors="coerce")

    cutoff = pd.Timestamp(f"{start_year}-01-01")
    df_cut = df[df["startdate"] >= cutoff]

    # Save in cluster output for now
    outdir = "output"
    os.makedirs(outdir, exist_ok=True)

    # 1. Positions per inventor
    positions = df_cut.groupby("user_id")["position_id"].nunique().compute()
    save_hist(winsorize(positions), "Positions per Inventor", os.path.join(outdir, "positions_hist.png"))

    # 2. Cities per inventor
    cities = df_cut.groupby("user_id")["city"].nunique().compute()
    save_hist(winsorize(cities), "Cities per Inventor", os.path.join(outdir, "cities_hist.png"))

    # 3. Firms per inventor
    firms = df_cut.groupby("user_id")["rcid"].nunique().compute()
    save_hist(winsorize(firms), "Firms per Inventor", os.path.join(outdir, "firms_hist.png"))

    # 4. Duration in sample
    tenure = (
        df_cut.groupby("user_id")["enddate"].max()
        - df_cut.groupby("user_id")["startdate"].min()
    ).compute().dt.days
    save_hist(winsorize(tenure), "Duration in Sample (days)", os.path.join(outdir, "tenure_hist.png"))

    # 5. Duration since first invention
    inv_duration = (
        df.groupby("user_id")["last_patent_date"].max()
        - df.groupby("user_id")["first_patent_date"].min()
    ).compute().dt.days
    save_hist(winsorize(inv_duration), "Duration since First Invention (days)",
              os.path.join(outdir, "invention_duration_hist.png"))

    # 6. Mean patents by positions, cities, firms
    patents = df_cut.groupby("user_id")["num_patents"].sum().compute()
    summary = pd.DataFrame({
        "positions": positions,
        "cities": cities,
        "firms": firms,
        "patents": patents
    })

    # --- Raw barplots ---
    save_barplot(summary.groupby("positions")["patents"].mean(),
                 "Mean Patents by Number of Positions (Raw)", "Positions", "Mean Patents",
                 os.path.join(outdir, "patents_by_positions.png"))

    save_barplot(summary.groupby("cities")["patents"].mean(),
                 "Mean Patents by Number of Cities (Raw)", "Cities", "Mean Patents",
                 os.path.join(outdir, "patents_by_cities.png"))

    save_barplot(summary.groupby("firms")["patents"].mean(),
                 "Mean Patents by Number of Firms (Raw)", "Firms", "Mean Patents",
                 os.path.join(outdir, "patents_by_firms.png"))

    # --- Winsorized barplots ---
    summary_w = summary.copy()
    summary_w["positions"] = winsorize(summary_w["positions"])
    summary_w["cities"]    = winsorize(summary_w["cities"])
    summary_w["firms"]     = winsorize(summary_w["firms"])
    summary_w["patents"]   = winsorize(summary_w["patents"])

    save_barplot(summary_w.groupby("positions")["patents"].mean(),
                 "Mean Patents by Number of Positions (Winsorized)", "Positions", "Mean Patents",
                 os.path.join(outdir, "patents_by_positions_winsorized.png"))

    save_barplot(summary_w.groupby("cities")["patents"].mean(),
                 "Mean Patents by Number of Cities (Winsorized)", "Cities", "Mean Patents",
                 os.path.join(outdir, "patents_by_cities_winsorized.png"))

    save_barplot(summary_w.groupby("firms")["patents"].mean(),
                 "Mean Patents by Number of Firms (Winsorized)", "Firms", "Mean Patents",
                 os.path.join(outdir, "patents_by_firms_winsorized.png"))

    print("\n=== Done! Results saved in", outdir)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file",
        type=str,
        default="/labs/khanna/linkedin_202507/processed/inventor_position_file",
        help="Path to Parquet file or directory",
    )
    parser.add_argument("--start_year", type=int, default=2010,
                        help="Cutoff year for analysis")
    args = parser.parse_args()
    main(args.file, args.start_year)
