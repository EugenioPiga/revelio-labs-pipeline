#!/usr/bin/env python3
"""Create presentation-ready interim figures from completed immigrant descriptives CSVs.

This script reads only the small CSV tables already completed by the main Spark job.
It does not read the large Parquet panel and is safe to run while the Spark job continues.
"""

from __future__ import annotations

import argparse
from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd


FOOTNOTE = (
    "Current U.S. person-years; last observed education in the U.S.  |  "
    "Immigrant = any education abroad or first observed job abroad.  |  "
    "Interim results from job 1448694."
)

SAMPLE_LABELS = {
    "all_inventors": "All inventors",
    "founders": "Founders",
}

STATUS_LABELS = {
    "immigrant": "Immigrants",
    "nonimmigrant": "Nonimmigrants",
    "all": "All",
}

COMPONENT_LABELS = {
    "both_education_and_first_job_abroad": "Education and first job abroad",
    "education_abroad_only": "Education abroad only",
    "first_job_abroad_only": "First job abroad only",
    "neither_observed_signal": "Neither observed signal",
}


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


def read_table(run_dir: Path, name: str) -> pd.DataFrame:
    path = run_dir / "tables" / name
    if not path.exists():
        raise FileNotFoundError(f"Missing completed table: {path}")
    return pd.read_csv(path)


def finish_figure(fig: plt.Figure, out_path: Path) -> None:
    fig.text(0.01, 0.01, FOOTNOTE, ha="left", va="bottom", fontsize=8)
    fig.tight_layout(rect=(0, 0.045, 1, 1))
    fig.savefig(out_path, dpi=240, bbox_inches="tight")
    plt.close(fig)
    print(f"[WROTE] {out_path}")


def figure_immigrant_share(yearly: pd.DataFrame, out_dir: Path) -> None:
    d = yearly[yearly["immigration_status"].isin(["immigrant", "all"])].copy()
    piv = d.pivot_table(
        index=["sample", "year"],
        columns="immigration_status",
        values="people",
        aggfunc="first",
    ).reset_index()
    piv = piv.dropna(subset=["immigrant", "all"])
    piv["immigrant_share"] = 100 * piv["immigrant"] / piv["all"]

    fig, ax = plt.subplots(figsize=(9.5, 5.8))
    for sample, z in piv.groupby("sample"):
        z = z.sort_values("year")
        ax.plot(z["year"], z["immigrant_share"], marker="o", markersize=3,
                label=SAMPLE_LABELS.get(sample, sample))
    ax.set_title("Immigrant share among U.S.-based inventors")
    ax.set_xlabel("Year")
    ax.set_ylabel("Immigrant share (%)")
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    finish_figure(fig, out_dir / "01_immigrant_share_over_time.png")


def figure_patent_trends(yearly: pd.DataFrame, out_dir: Path, sample: str) -> None:
    d = yearly[
        (yearly["sample"] == sample)
        & yearly["immigration_status"].isin(["immigrant", "nonimmigrant"])
    ].copy()
    if d.empty:
        return

    fig, ax = plt.subplots(figsize=(9.5, 5.8))
    for status, z in d.groupby("immigration_status"):
        z = z.sort_values("year")
        ax.plot(
            z["year"],
            z["mean_patents_per_person_year"],
            marker="o",
            markersize=3,
            label=STATUS_LABELS.get(status, status),
        )
    ax.set_title(f"Mean annual patents: {SAMPLE_LABELS.get(sample, sample)}")
    ax.set_xlabel("Year")
    ax.set_ylabel("Mean patents per inventor-year")
    ax.grid(alpha=0.25)
    ax.legend(frameon=False)
    finish_figure(fig, out_dir / f"02_mean_patents_over_time__{sample}.png")


def figure_component_overlap(component: pd.DataFrame, out_dir: Path) -> None:
    d = component.copy()
    d["component_label"] = d["immigration_component"].map(COMPONENT_LABELS).fillna(
        d["immigration_component"]
    )
    order = [
        "Education and first job abroad",
        "Education abroad only",
        "First job abroad only",
        "Neither observed signal",
    ]
    piv = d.pivot_table(
        index="component_label", columns="sample", values="share", aggfunc="first"
    ).reindex(order)
    piv = piv * 100

    fig, ax = plt.subplots(figsize=(10, 5.8))
    x = np.arange(len(piv.index))
    samples = [s for s in ["all_inventors", "founders"] if s in piv.columns]
    width = 0.36 if len(samples) > 1 else 0.55
    offsets = np.linspace(-(len(samples) - 1) * width / 2, (len(samples) - 1) * width / 2, len(samples))
    for offset, sample in zip(offsets, samples):
        vals = piv[sample].fillna(0).to_numpy()
        bars = ax.bar(x + offset, vals, width=width, label=SAMPLE_LABELS.get(sample, sample))
        ax.bar_label(bars, labels=[f"{v:.1f}%" for v in vals], padding=2, fontsize=8)
    ax.set_xticks(x)
    ax.set_xticklabels(piv.index, rotation=18, ha="right")
    ax.set_ylabel("Share of people in sample (%)")
    ax.set_title("How the broader immigrant definition is constructed")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.25)
    finish_figure(fig, out_dir / "03_immigration_definition_components.png")


def figure_top_origins(origin: pd.DataFrame, out_dir: Path, top_n: int = 12) -> None:
    base = origin[origin["sample"] == "all_inventors"].nlargest(top_n, "people")
    countries = base["country_of_origin_clean"].tolist()
    d = origin[origin["country_of_origin_clean"].isin(countries)].copy()
    piv = d.pivot_table(
        index="country_of_origin_clean",
        columns="sample",
        values="share_within_sample_immigrants",
        aggfunc="first",
    ).reindex(countries)
    piv = piv.iloc[::-1] * 100

    fig, ax = plt.subplots(figsize=(9.5, 7.2))
    y = np.arange(len(piv.index))
    samples = [s for s in ["all_inventors", "founders"] if s in piv.columns]
    height = 0.36 if len(samples) > 1 else 0.55
    offsets = np.linspace(-(len(samples) - 1) * height / 2, (len(samples) - 1) * height / 2, len(samples))
    for offset, sample in zip(offsets, samples):
        ax.barh(y + offset, piv[sample].fillna(0), height=height,
                label=SAMPLE_LABELS.get(sample, sample))
    ax.set_yticks(y)
    ax.set_yticklabels(piv.index)
    ax.set_xlabel("Share of immigrants in sample (%)")
    ax.set_title("Largest immigrant origin groups")
    ax.legend(frameon=False)
    ax.grid(axis="x", alpha=0.25)
    finish_figure(fig, out_dir / "04_top_origin_countries_all_vs_founders.png")


def figure_origin_composition_over_time(origin_year: pd.DataFrame, out_dir: Path, sample: str) -> None:
    d = origin_year[origin_year["sample"] == sample].copy()
    if d.empty:
        return
    d["share"] = d["people"] / d.groupby("year")["people"].transform("sum")
    totals = (
        d[d["origin_grouped"] != "Other"]
        .groupby("origin_grouped")["people"]
        .sum()
        .sort_values(ascending=False)
    )
    keep = totals.head(8).index.tolist()
    d["origin_plot"] = np.where(d["origin_grouped"].isin(keep), d["origin_grouped"], "Other")
    d2 = d.groupby(["year", "origin_plot"], as_index=False)["share"].sum()
    piv = d2.pivot(index="year", columns="origin_plot", values="share").fillna(0)
    ordered_cols = [c for c in keep if c in piv.columns]
    if "Other" in piv.columns:
        ordered_cols.append("Other")
    piv = piv[ordered_cols] * 100

    fig, ax = plt.subplots(figsize=(10, 6.2))
    piv.plot.area(ax=ax, linewidth=0)
    ax.set_title(f"Immigrant origin composition over time: {SAMPLE_LABELS.get(sample, sample)}")
    ax.set_xlabel("Year")
    ax.set_ylabel("Share of immigrants (%)")
    ax.set_ylim(0, 100)
    ax.legend(title="Origin", frameon=False, bbox_to_anchor=(1.02, 1), loc="upper left")
    finish_figure(fig, out_dir / f"05_origin_composition_over_time__{sample}.png")


def figure_origin_patents(outcomes: pd.DataFrame, out_dir: Path, top_n: int = 12) -> None:
    base = outcomes[outcomes["sample"] == "all_inventors"].nlargest(top_n, "people")
    origins = base["origin_grouped"].tolist()
    d = outcomes[outcomes["origin_grouped"].isin(origins)].copy()
    piv = d.pivot_table(
        index="origin_grouped", columns="sample", values="mean_patents", aggfunc="first"
    ).reindex(origins)
    piv = piv.iloc[::-1]

    fig, ax = plt.subplots(figsize=(9.5, 7.2))
    y = np.arange(len(piv.index))
    samples = [s for s in ["all_inventors", "founders"] if s in piv.columns]
    height = 0.36 if len(samples) > 1 else 0.55
    offsets = np.linspace(-(len(samples) - 1) * height / 2, (len(samples) - 1) * height / 2, len(samples))
    for offset, sample in zip(offsets, samples):
        ax.barh(y + offset, piv[sample].fillna(0), height=height,
                label=SAMPLE_LABELS.get(sample, sample))
    ax.set_yticks(y)
    ax.set_yticklabels(piv.index)
    ax.set_xlabel("Mean patents per inventor-year")
    ax.set_title("Patent output by immigrant origin")
    ax.legend(frameon=False)
    ax.grid(axis="x", alpha=0.25)
    finish_figure(fig, out_dir / "06_mean_patents_by_origin_all_vs_founders.png")


def main() -> None:
    args = parse_args()
    run_dir = Path(args.run_dir).expanduser().resolve()
    out_dir = Path(args.out_dir).expanduser().resolve() if args.out_dir else run_dir / "figures_interim"
    out_dir.mkdir(parents=True, exist_ok=True)

    yearly = read_table(run_dir, "yearly_counts_and_outcomes.csv")
    component = read_table(run_dir, "immigration_component_overlap.csv")
    origin = read_table(run_dir, "origin_country_distribution.csv")
    origin_year = read_table(run_dir, "origin_country_by_year.csv")
    outcomes = read_table(run_dir, "outcomes_by_origin.csv")

    figure_immigrant_share(yearly, out_dir)
    figure_patent_trends(yearly, out_dir, "all_inventors")
    figure_patent_trends(yearly, out_dir, "founders")
    figure_component_overlap(component, out_dir)
    figure_top_origins(origin, out_dir)
    figure_origin_composition_over_time(origin_year, out_dir, "all_inventors")
    figure_origin_composition_over_time(origin_year, out_dir, "founders")
    figure_origin_patents(outcomes, out_dir)

    print(f"[DONE] Interim figures: {out_dir}")


if __name__ == "__main__":
    main()
