#!/usr/bin/env python3
"""Create additional interim figures from completed tenure and five-year-period tables.

Reads only:
  tables/outcomes_by_tenure.csv
  tables/five_year_period_summary.csv

It does not read the large Parquet panel and is safe to run while the main Spark
descriptive job continues.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd


SAMPLE_LABELS = {
    "all_inventors": "All inventors",
    "founders": "Founders",
}

STATUS_LABELS = {
    "immigrant": "Immigrants",
    "nonimmigrant": "Nonimmigrants",
}

BASE_NOTE = (
    "Current U.S. person-years; last observed education in the U.S. | "
    "Immigrant = any education abroad or first observed job abroad. | "
    "Interim results from job 1448694."
)

CITATION_NOTE = (
    BASE_NOTE
    + " Citation outcomes for recent years may be mechanically incomplete."
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--run-dir",
        required=True,
        help="Run directory containing tables/, e.g. .../run_1448694",
    )
    parser.add_argument(
        "--out-dir",
        default=None,
        help="Output folder. Default: <run-dir>/figures_interim",
    )
    return parser.parse_args()


def read_table(run_dir: Path, filename: str) -> pd.DataFrame:
    path = run_dir / "tables" / filename
    if not path.exists():
        raise FileNotFoundError(f"Missing completed table: {path}")
    return pd.read_csv(path)


def finish_figure(
    fig: plt.Figure,
    out_path: Path,
    footnote: str = BASE_NOTE,
) -> None:
    fig.text(0.01, 0.01, footnote, ha="left", va="bottom", fontsize=8)
    fig.tight_layout(rect=(0, 0.055, 1, 1))
    fig.savefig(out_path, dpi=240, bbox_inches="tight")
    plt.close(fig)
    print(f"[WROTE] {out_path}")


def status_subset(df: pd.DataFrame, sample: str) -> pd.DataFrame:
    return df[
        (df["sample"] == sample)
        & df["immigration_status"].isin(["immigrant", "nonimmigrant"])
    ].copy()


def plot_tenure_metric(
    tenure: pd.DataFrame,
    out_dir: Path,
    sample: str,
    metric: str,
    title: str,
    ylabel: str,
    filename: str,
    *,
    multiplier: float = 1.0,
    footnote: str = BASE_NOTE,
) -> None:
    d = status_subset(tenure, sample)
    if d.empty or metric not in d.columns:
        return

    d["tenure_bin_start"] = pd.to_numeric(d["tenure_bin_start"], errors="coerce")
    d[metric] = pd.to_numeric(d[metric], errors="coerce") * multiplier
    d = d.dropna(subset=["tenure_bin_start", metric])

    fig, ax = plt.subplots(figsize=(9.5, 5.8))
    for status, z in d.groupby("immigration_status"):
        z = z.sort_values("tenure_bin_start")
        ax.plot(
            z["tenure_bin_start"],
            z[metric],
            marker="o",
            markersize=4,
            label=STATUS_LABELS.get(status, status),
        )

    ax.set_title(f"{title}: {SAMPLE_LABELS.get(sample, sample)}")
    ax.set_xlabel("Years since current-firm entry, grouped in tenure bins")
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    finish_figure(fig, out_dir / filename, footnote)


def plot_period_metric(
    period: pd.DataFrame,
    out_dir: Path,
    sample: str,
    metric: str,
    title: str,
    ylabel: str,
    filename: str,
    *,
    footnote: str = BASE_NOTE,
) -> None:
    d = status_subset(period, sample)
    if d.empty or metric not in d.columns:
        return

    d["period_start"] = pd.to_numeric(d["period_start"], errors="coerce")
    d[metric] = pd.to_numeric(d[metric], errors="coerce")
    d = d.dropna(subset=["period_start", metric])

    # Prefer the source period labels. Fall back to period_start when unavailable.
    if "period" in d.columns:
        label_map = (
            d[["period_start", "period"]]
            .drop_duplicates()
            .set_index("period_start")["period"]
            .to_dict()
        )
    else:
        label_map = {}

    fig, ax = plt.subplots(figsize=(9.5, 5.8))
    for status, z in d.groupby("immigration_status"):
        z = z.sort_values("period_start")
        ax.plot(
            z["period_start"],
            z[metric],
            marker="o",
            markersize=5,
            label=STATUS_LABELS.get(status, status),
        )

    starts = sorted(d["period_start"].dropna().unique())
    ax.set_xticks(starts)
    ax.set_xticklabels([str(label_map.get(x, int(x))) for x in starts])
    ax.set_title(f"{title}: {SAMPLE_LABELS.get(sample, sample)}")
    ax.set_xlabel("Five-year period")
    ax.set_ylabel(ylabel)
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    finish_figure(fig, out_dir / filename, footnote)


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).expanduser().resolve()
    out_dir = (
        Path(args.out_dir).expanduser().resolve()
        if args.out_dir
        else run_dir / "figures_interim"
    )
    out_dir.mkdir(parents=True, exist_ok=True)

    tenure = read_table(run_dir, "outcomes_by_tenure.csv")
    period = read_table(run_dir, "five_year_period_summary.csv")

    for sample in ("all_inventors", "founders"):
        plot_tenure_metric(
            tenure,
            out_dir,
            sample,
            "mean_patents",
            "Mean annual patents by firm tenure",
            "Mean patents per inventor-year",
            f"07_mean_patents_by_tenure__{sample}.png",
        )
        plot_tenure_metric(
            tenure,
            out_dir,
            sample,
            "share_top10_patenting",
            "Share of top-10% patenting inventors by firm tenure",
            "Share of inventor-years associated with top-10% inventors (%)",
            f"08_top10_patenting_share_by_tenure__{sample}.png",
            multiplier=100.0,
        )
        plot_period_metric(
            period,
            out_dir,
            sample,
            "mean_patents",
            "Mean annual patents by five-year period",
            "Mean patents per inventor-year",
            f"09_mean_patents_by_five_year_period__{sample}.png",
        )
        plot_tenure_metric(
            tenure,
            out_dir,
            sample,
            "mean_us_citations",
            "Mean U.S. citations by firm tenure",
            "Mean U.S. citations per inventor-year",
            f"10_mean_us_citations_by_tenure__{sample}.png",
            footnote=CITATION_NOTE,
        )
        plot_period_metric(
            period,
            out_dir,
            sample,
            "mean_us_citations",
            "Mean U.S. citations by five-year period",
            "Mean U.S. citations per inventor-year",
            f"11_mean_us_citations_by_five_year_period__{sample}.png",
            footnote=CITATION_NOTE,
        )

    print(f"[DONE] Additional interim figures: {out_dir}")


if __name__ == "__main__":
    main()
