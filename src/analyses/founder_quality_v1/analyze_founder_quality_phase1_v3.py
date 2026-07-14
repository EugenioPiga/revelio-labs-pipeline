#!/usr/bin/env python3
"""
Phase-1 founder-quality analysis on validated V3 founder outputs.

Reads only:
  - output/founder_firm_outcomes_v3/founder_firm_outcomes_v3
  - validation_v3/<run>/candidate_link_horizon_eligibility
  - validation_v3/<run>/candidate_phase1_sample_markers

Writes only:
  - output/founder_quality_v1/phase1_v3/run_*

Design:
  * One earliest observed founder link per user-firm.
  * Main sample: core founder title + complete observed y0_5 horizon.
  * Founder entry is first observed founder-spell entry, not legal incorporation.
  * Formal immigration variable: immig_first_deg_or_job_nonUS only.
  * No absent firm-year is recoded to zero.
  * Multi-founder firms receive equal total weight across founder links;
    firm-clustered standard errors are used in link-level regressions.
  * Serial-founder evidence is explicitly descriptive/predictive, not causal.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import numpy as np
import pandas as pd
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

PROJECT = Path("/home/epiga/revelio_labs")
V3_FINAL = PROJECT / "output" / "founder_firm_outcomes_v3" / "founder_firm_outcomes_v3"
PRIMARY_IMMIGRATION = "immig_first_deg_or_job_nonUS"
USER_FIRM_KEY = ["user_id", "founded_rcid"]


def _json_default(value: Any) -> str:
    return str(value)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=_json_default) + "\n")


def require_columns(df: DataFrame, cols: Iterable[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def hash_file(path: Path) -> str | None:
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.exists() else None


def make_link_id(df: DataFrame) -> DataFrame:
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
        for c in ["user_id", "founded_rcid", "founder_position_id", "first_founder_entry_year"]
    ]
    return df.withColumn("analysis_link_id", F.sha2(F.concat_ws("||", *parts), 256))


def scalar_metrics(df: DataFrame, expressions: list[F.Column]) -> dict[str, Any]:
    row = df.agg(*expressions).first()
    return {} if row is None else row.asDict(recursive=True)


def write_spark_csv(df: DataFrame, path: Path) -> None:
    df.coalesce(1).write.mode("errorifexists").option("header", True).csv(str(path))


def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    valid = x.notna() & w.notna() & np.isfinite(x) & np.isfinite(w) & (w > 0)
    if int(valid.sum()) == 0:
        return float("nan")
    return float(np.average(x.loc[valid].astype(float), weights=w.loc[valid].astype(float)))


def normal_two_sided_p(z: np.ndarray) -> np.ndarray:
    z = np.asarray(z, dtype=float)
    return np.array([math.erfc(abs(float(v)) / math.sqrt(2.0)) if np.isfinite(v) else np.nan for v in z])


def collapse_rare_categories(s: pd.Series, min_n: int = 60, max_levels: int = 80) -> pd.Series:
    x = s.astype("string").fillna("__MISSING__")
    counts = x.value_counts(dropna=False)
    keep = counts[counts >= min_n].index.tolist()[:max_levels]
    return x.where(x.isin(keep), "__OTHER__")


def build_design(
    df: pd.DataFrame,
    numeric: list[str],
    categorical: list[str],
) -> tuple[pd.DataFrame, list[str]]:
    pieces: list[pd.DataFrame] = [pd.DataFrame({"const": np.ones(len(df), dtype=float)}, index=df.index)]
    numeric_used: list[str] = []
    for c in numeric:
        if c not in df.columns:
            continue
        x = pd.to_numeric(df[c], errors="coerce")
        if x.notna().sum() == 0:
            continue
        fill = float(x.median()) if x.notna().any() else 0.0
        pieces.append(pd.DataFrame({c: x.fillna(fill).astype(float)}, index=df.index))
        if x.isna().any():
            pieces.append(pd.DataFrame({f"missing_{c}": x.isna().astype(float)}, index=df.index))
        numeric_used.append(c)

    categorical_used: list[str] = []
    for c in categorical:
        if c not in df.columns:
            continue
        collapsed = collapse_rare_categories(df[c])
        dummies = pd.get_dummies(collapsed, prefix=c, dtype=float)
        if dummies.shape[1] > 1:
            dummies = dummies.iloc[:, 1:]
        if dummies.shape[1] > 0:
            pieces.append(dummies)
            categorical_used.append(c)

    X = pd.concat(pieces, axis=1)
    X = X.loc[:, X.nunique(dropna=False) > 1]
    if "const" not in X.columns:
        X.insert(0, "const", 1.0)
    return X.astype(float), numeric_used + categorical_used


def cluster_wls(
    y: pd.Series,
    X: pd.DataFrame,
    weights: pd.Series,
    clusters: pd.Series,
    model_name: str,
    sample_name: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    valid = (
        y.notna()
        & np.isfinite(y)
        & weights.notna()
        & np.isfinite(weights)
        & (weights > 0)
        & clusters.notna()
    )
    Xv = X.loc[valid].copy()
    yv = y.loc[valid].astype(float).to_numpy()
    wv = weights.loc[valid].astype(float).to_numpy()
    cv = clusters.loc[valid].astype(str).to_numpy()

    # Drop all-zero columns only after filtering.
    keep = Xv.columns[(Xv.abs().sum(axis=0) > 0).to_numpy()]
    Xv = Xv.loc[:, keep]
    x = Xv.to_numpy(dtype=float)
    n, p = x.shape
    unique_clusters = np.unique(cv)
    g = len(unique_clusters)
    if n <= p or g < 2:
        raise RuntimeError(f"Insufficient observations for {model_name}/{sample_name}: n={n}, p={p}, clusters={g}")

    xtwx = x.T @ (wv[:, None] * x)
    bread = np.linalg.pinv(xtwx, rcond=1e-10)
    beta = bread @ (x.T @ (wv * yv))
    residual = yv - x @ beta

    meat = np.zeros((p, p), dtype=float)
    for cl in unique_clusters:
        idx = cv == cl
        score = x[idx].T @ (wv[idx] * residual[idx])
        meat += np.outer(score, score)

    correction = (g / max(g - 1, 1)) * ((n - 1) / max(n - p, 1))
    cov = correction * (bread @ meat @ bread)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    pval = normal_two_sided_p(z)

    out = pd.DataFrame(
        {
            "sample": sample_name,
            "model": model_name,
            "term": list(Xv.columns),
            "estimate": beta,
            "cluster_se": se,
            "z_approx": z,
            "p_value_normal_approx": pval,
            "n_observations": n,
            "n_firm_clusters": g,
            "n_parameters": p,
            "weighted": True,
        }
    )
    info = {
        "sample": sample_name,
        "model": model_name,
        "n_observations": int(n),
        "n_firm_clusters": int(g),
        "n_parameters": int(p),
        "matrix_rank": int(np.linalg.matrix_rank(x)),
        "condition_number": float(np.linalg.cond(xtwx)),
    }
    return out, info


def simple_wls(
    y: pd.Series,
    x_main: pd.Series,
    controls: pd.DataFrame,
    weights: pd.Series,
    model_name: str,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    pieces = [pd.DataFrame({"const": 1.0, "first_outcome": x_main}, index=y.index)]
    if controls.shape[1] > 0:
        pieces.append(controls)
    X = pd.concat(pieces, axis=1)
    return cluster_wls(y, X, weights, pd.Series(np.arange(len(y)), index=y.index), model_name, "serial_founder_level")


def save_figure(fig: plt.Figure, figures_dir: Path, stem: str) -> None:
    fig.tight_layout()
    fig.savefig(figures_dir / f"{stem}.png", dpi=220, bbox_inches="tight")
    fig.savefig(figures_dir / f"{stem}.pdf", bbox_inches="tight")
    plt.close(fig)


def output_value(df: pd.DataFrame, field: str) -> pd.Series:
    return pd.to_numeric(df[field], errors="coerce") if field in df.columns else pd.Series(np.nan, index=df.index)


def make_descriptive_table(primary: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    traits: list[tuple[str, pd.Series]] = [
        ("formal_immigrant", output_value(primary, PRIMARY_IMMIGRATION).fillna(-1).astype(int).astype(str)),
        ("prehistory_observed", output_value(primary, "prior_years_observed").fillna(0).gt(0).astype(int).astype(str)),
        ("prior_patenter", output_value(primary, "prior_patents").fillna(0).gt(0).astype(int).astype(str)),
        ("prior_cited", output_value(primary, "prior_us_citations").fillna(0).gt(0).astype(int).astype(str)),
    ]
    if "founder_stem_at_founder" in primary.columns:
        traits.append(("stem_at_founder_entry", output_value(primary, "founder_stem_at_founder").fillna(-1).astype(int).astype(str)))

    for label, values in traits:
        temp = primary.copy()
        temp["_group"] = values.replace("-1", "missing")
        for group, sub in temp.groupby("_group", dropna=False):
            rows.append(
                {
                    "trait": label,
                    "group": str(group),
                    "n_links": int(len(sub)),
                    "weighted_mean_patents_y0_5": weighted_mean(sub["firm_patents_y0_5"], sub["team_weight"]),
                    "weighted_mean_citations_y0_5": weighted_mean(sub["firm_us_citations_y0_5"], sub["team_weight"]),
                    "weighted_mean_log1p_patents_y0_5": weighted_mean(sub["log1p_patents_y0_5"], sub["team_weight"]),
                    "weighted_mean_log1p_citations_y0_5": weighted_mean(sub["log1p_citations_y0_5"], sub["team_weight"]),
                }
            )
    return pd.DataFrame(rows)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-run", required=True)
    parser.add_argument("--out-root", default=str(PROJECT / "output" / "founder_quality_v1" / "phase1_v3"))
    parser.add_argument("--shuffle-partitions", type=int, default=320)
    args = parser.parse_args()

    validation_run = Path(args.validation_run).resolve()
    eligibility_path = validation_run / "candidate_link_horizon_eligibility"
    marker_path = validation_run / "candidate_phase1_sample_markers"
    if not (validation_run / "VALIDATION_REPORT.md").exists():
        raise FileNotFoundError(f"Validation report absent: {validation_run / 'VALIDATION_REPORT.md'}")
    for p in [V3_FINAL, eligibility_path, marker_path]:
        if not p.exists():
            raise FileNotFoundError(f"Required input absent: {p}")

    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    run_dir = Path(args.out_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    figures_dir = run_dir / "figures"
    tables_dir = run_dir / "tables"
    figures_dir.mkdir()
    tables_dir.mkdir()

    spark = (
        SparkSession.builder
        .appName("founder_quality_phase1_v3")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] run_dir={run_dir}", flush=True)
    print(f"[INFO] validation_run={validation_run}", flush=True)

    final = spark.read.parquet(str(V3_FINAL))
    eligibility = spark.read.parquet(str(eligibility_path))
    markers = spark.read.parquet(str(marker_path))

    required_final = [
        "user_id", "founded_rcid", "founder_position_id", "first_founder_entry_date",
        "first_founder_entry_year", "core_founder_title_flag", PRIMARY_IMMIGRATION,
        "firm_patents_y0_3", "firm_patents_y0_5", "firm_patents_y0_10",
        "firm_us_citations_y0_3", "firm_us_citations_y0_5", "firm_us_citations_y0_10",
        "prior_patents", "prior_us_citations", "prior_years_observed",
    ]
    required_eligibility = [
        "analysis_link_id", "eligible_patents_y0_3", "eligible_patents_y0_5", "eligible_patents_y0_10",
        "eligible_citations_y0_3", "eligible_citations_y0_5", "eligible_citations_y0_10",
    ]
    require_columns(final, required_final, "V3 final founder output")
    require_columns(eligibility, required_eligibility, "Validated horizon-eligibility panel")
    require_columns(markers, ["analysis_link_id", "proposed_primary_phase1_sample_y0_5"], "Validated Phase-1 sample-marker panel")

    optional = [
        "broad_founder_title_flag", "cofounder_title_flag", "founding_partner_title_flag",
        "founding_employee_title_flag", "founder_country", "founder_metro_area", "founder_naics3",
        "founder_naics6", "founder_stem_at_founder", "founder_seniority", "founder_mapped_role_v3",
        "prior_unique_current_rcids", "prior_unique_parent_rcids", "prior_unique_onet_codes",
        "prior_unique_naics6", "prior_max_seniority", "prior_avg_seniority",
        "prior_latest_parent_rcid", "prior_latest_naics3", "prior_latest_onet_code",
    ]
    final_keep = list(dict.fromkeys(required_final + optional + ["founder_company_name"]))
    final_keep = [c for c in final_keep if c in final.columns]

    # First observed founder link per user-firm. This preserves the V3 timing convention.
    w = Window.partitionBy(*USER_FIRM_KEY).orderBy(
        F.col("first_founder_entry_date").asc_nulls_last(),
        F.col("founder_position_id").asc_nulls_last(),
    )
    links = (
        final.select(*final_keep)
        .withColumn("_rn_user_firm", F.row_number().over(w))
        .filter(F.col("_rn_user_firm") == 1)
        .drop("_rn_user_firm")
    )
    links = make_link_id(links).persist(StorageLevel.MEMORY_AND_DISK)
    dedup_links = links.count()

    elig_keep = [
        "analysis_link_id", "eligible_patents_y0_3", "eligible_patents_y0_5", "eligible_patents_y0_10",
        "eligible_citations_y0_3", "eligible_citations_y0_5", "eligible_citations_y0_10",
        "complete_calendar_y0_3", "complete_calendar_y0_5", "complete_calendar_y0_10",
    ]
    elig_keep = [c for c in elig_keep if c in eligibility.columns]
    marker_keep = (
        markers.select(
            "analysis_link_id",
            F.col("proposed_primary_phase1_sample_y0_5").alias("validated_proposed_primary_y0_5"),
        )
        .dropDuplicates(["analysis_link_id"])
    )
    analysis = (
        links.join(eligibility.select(*elig_keep), on="analysis_link_id", how="inner")
        .join(marker_keep, on="analysis_link_id", how="left")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    joined_links = analysis.count()

    # Sample flow uses exact V3 flags; no absent calendar year enters an eligible horizon.
    sample_flow = scalar_metrics(
        analysis,
        [
            F.count("*").alias("deduplicated_user_firm_links"),
            F.sum((F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1).cast("int")).alias("core_founder_links"),
            F.sum(((F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1) & (F.col("eligible_patents_y0_3") == 1)).cast("int")).alias("core_complete_y0_3"),
            F.sum(((F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1) & (F.col("eligible_patents_y0_5") == 1)).cast("int")).alias("core_complete_y0_5"),
            F.sum(((F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1) & (F.col("eligible_patents_y0_10") == 1)).cast("int")).alias("core_complete_y0_10"),
            F.sum(F.col(PRIMARY_IMMIGRATION).isNull().cast("int")).alias("missing_formal_immigration"),
            F.sum(F.col("prior_years_observed").isNull().cast("int")).alias("missing_prior_years_observed"),
            F.sum(
                F.when(
                    ((F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1)
                    & (F.col("eligible_patents_y0_5") == 1))
                    != (F.coalesce(F.col("validated_proposed_primary_y0_5").cast("int"), F.lit(0)) == 1),
                    F.lit(1),
                ).otherwise(F.lit(0))
            ).alias("recomputed_vs_validated_primary_marker_mismatch"),
        ],
    )
    sample_flow["final_founder_link_rows_before_user_firm_deduplication"] = final.count()
    sample_flow["deduplicated_user_firm_links_before_validation_join"] = dedup_links
    sample_flow["deduplicated_user_firm_links_after_validation_join"] = joined_links
    pd.DataFrame([sample_flow]).to_csv(tables_dir / "sample_flow.csv", index=False)

    primary_spark = (
        analysis
        .filter(F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1)
        .filter((F.col("eligible_patents_y0_5") == 1) & (F.col("eligible_citations_y0_5") == 1))
    )
    team_sizes = (
        primary_spark.groupBy("founded_rcid")
        .agg(F.countDistinct("user_id").alias("n_core_founders_firm"))
    )
    primary_spark = (
        primary_spark.join(team_sizes, on="founded_rcid", how="left")
        .withColumn("team_weight", F.lit(1.0) / F.col("n_core_founders_firm"))
        .withColumn("log1p_patents_y0_5", F.log1p(F.col("firm_patents_y0_5").cast("double")))
        .withColumn("log1p_citations_y0_5", F.log1p(F.col("firm_us_citations_y0_5").cast("double")))
        .withColumn("has_preentry_panel_history", (F.coalesce(F.col("prior_years_observed"), F.lit(0)) > 0).cast("int"))
        .withColumn("log1p_prior_patents", F.log1p(F.greatest(F.coalesce(F.col("prior_patents").cast("double"), F.lit(0.0)), F.lit(0.0))))
        .withColumn("log1p_prior_citations", F.log1p(F.greatest(F.coalesce(F.col("prior_us_citations").cast("double"), F.lit(0.0)), F.lit(0.0))))
        .withColumn("log1p_prior_employers", F.log1p(F.greatest(F.coalesce(F.col("prior_unique_current_rcids").cast("double"), F.lit(0.0)), F.lit(0.0))))
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    primary_n = primary_spark.count()
    if primary_n == 0:
        raise RuntimeError("Primary core + complete y0_5 sample is empty.")

    # Write a limited analysis panel, not a copy of the raw data.
    panel_cols = [
        "analysis_link_id", "user_id", "founded_rcid", "founder_position_id",
        "first_founder_entry_date", "first_founder_entry_year", "core_founder_title_flag",
        PRIMARY_IMMIGRATION, "n_core_founders_firm", "team_weight",
        "firm_patents_y0_3", "firm_patents_y0_5", "firm_patents_y0_10",
        "firm_us_citations_y0_3", "firm_us_citations_y0_5", "firm_us_citations_y0_10",
        "prior_patents", "prior_us_citations", "prior_years_observed", "prior_unique_current_rcids",
        "has_preentry_panel_history", "log1p_prior_patents", "log1p_prior_citations", "log1p_prior_employers",
        "log1p_patents_y0_5", "log1p_citations_y0_5", "founder_naics3", "founder_country",
        "founder_stem_at_founder", "founder_seniority",
    ]
    panel_cols = [c for c in panel_cols if c in primary_spark.columns]
    primary_spark.select(*panel_cols).write.mode("errorifexists").parquet(str(run_dir / "analysis_panel_primary_core_y0_5"))

    # A one-row-per-firm robustness panel based on the earliest core founder-entry year at each firm.
    firm_first = primary_spark.groupBy("founded_rcid").agg(F.min("first_founder_entry_year").alias("firm_first_core_entry_year"))
    team_first = (
        primary_spark.join(firm_first, on="founded_rcid", how="inner")
        .filter(F.col("first_founder_entry_year") == F.col("firm_first_core_entry_year"))
    )
    team_level = (
        team_first.groupBy("founded_rcid", "firm_first_core_entry_year")
        .agg(
            F.countDistinct("user_id").alias("n_core_founders_at_first_event"),
            F.avg("log1p_prior_patents").alias("mean_log1p_prior_patents"),
            F.avg("log1p_prior_citations").alias("mean_log1p_prior_citations"),
            F.avg(F.col(PRIMARY_IMMIGRATION).cast("double")).alias("share_formal_immigrant"),
            F.first("firm_patents_y0_5", ignorenulls=True).alias("firm_patents_y0_5"),
            F.first("firm_us_citations_y0_5", ignorenulls=True).alias("firm_us_citations_y0_5"),
        )
        .withColumn("log1p_patents_y0_5", F.log1p(F.col("firm_patents_y0_5").cast("double")))
        .withColumn("log1p_citations_y0_5", F.log1p(F.col("firm_us_citations_y0_5").cast("double")))
    )
    team_level.write.mode("errorifexists").parquet(str(run_dir / "team_level_earliest_core_event_y0_5"))
    team_level_summary = scalar_metrics(
        team_level,
        [
            F.count("*").alias("team_level_firms"),
            F.avg("n_core_founders_at_first_event").alias("mean_core_founders_at_first_event"),
            F.max("n_core_founders_at_first_event").alias("max_core_founders_at_first_event"),
        ],
    )
    pd.DataFrame([team_level_summary]).to_csv(tables_dir / "team_level_summary.csv", index=False)

    # The validated primary analysis panel is small enough for transparent local descriptive/regression work.
    pandas_cols = list(dict.fromkeys(panel_cols + [
        "firm_patents_y0_5", "firm_us_citations_y0_5", "log1p_patents_y0_5", "log1p_citations_y0_5",
        "n_core_founders_firm", "team_weight", "has_preentry_panel_history", "log1p_prior_patents",
        "log1p_prior_citations", "log1p_prior_employers", "prior_max_seniority", "prior_avg_seniority",
        "founder_mapped_role_v3", "prior_unique_parent_rcids", "prior_unique_onet_codes", "prior_unique_naics6",
    ]))
    pandas_cols = [c for c in pandas_cols if c in primary_spark.columns]
    primary = primary_spark.select(*pandas_cols).toPandas()
    if len(primary) != primary_n:
        raise RuntimeError(f"Pandas row mismatch: expected {primary_n}, obtained {len(primary)}")

    # Numeric cleanup for transparent model construction.
    for c in [
        "firm_patents_y0_5", "firm_us_citations_y0_5", "team_weight", "n_core_founders_firm",
        "prior_patents", "prior_us_citations", "prior_years_observed", "prior_unique_current_rcids",
        "prior_max_seniority", "prior_avg_seniority", PRIMARY_IMMIGRATION,
        "founder_stem_at_founder", "founder_seniority", "first_founder_entry_year",
        "log1p_prior_patents", "log1p_prior_citations", "log1p_prior_employers",
        "log1p_patents_y0_5", "log1p_citations_y0_5",
    ]:
        if c in primary.columns:
            primary[c] = pd.to_numeric(primary[c], errors="coerce")

    primary["team_weight"] = primary["team_weight"].fillna(1.0)
    primary["formal_immigrant"] = primary.get(PRIMARY_IMMIGRATION, pd.Series(np.nan, index=primary.index)).fillna(0.0)
    primary["first_founder_entry_year"] = primary["first_founder_entry_year"].astype("Int64")
    primary["primary_team_size_le_10"] = (primary["n_core_founders_firm"].fillna(np.inf) <= 10).astype(int)
    primary["prehistory_observed"] = primary["has_preentry_panel_history"].fillna(0).astype(int)

    desc = make_descriptive_table(primary)
    desc.to_csv(tables_dir / "descriptive_by_founder_trait.csv", index=False)

    # Regression specification: association only. Outcome is log(1 + fixed-horizon count).
    numeric = [
        "formal_immigrant", "log1p_prior_patents", "log1p_prior_citations",
        "prehistory_observed", "prior_years_observed", "log1p_prior_employers",
        "founder_stem_at_founder", "founder_seniority", "n_core_founders_firm",
    ]
    categorical = ["first_founder_entry_year", "founder_naics3"]
    regression_rows: list[pd.DataFrame] = []
    model_info: list[dict[str, Any]] = []
    variants = {
        "primary_core_complete_y0_5": primary.copy(),
        "robust_team_size_le_10": primary.loc[primary["primary_team_size_le_10"] == 1].copy(),
    }
    for sample_name, sub in variants.items():
        if len(sub) < 100:
            continue
        X, used = build_design(sub, numeric=numeric, categorical=categorical)
        for outcome, model_name in [
            ("log1p_patents_y0_5", "log1p_firm_patents_y0_5"),
            ("log1p_citations_y0_5", "log1p_firm_us_citations_y0_5"),
        ]:
            try:
                tab, info = cluster_wls(
                    sub[outcome], X, sub["team_weight"], sub["founded_rcid"], model_name, sample_name
                )
                tab["controls_used"] = ";".join(used)
                regression_rows.append(tab)
                model_info.append(info)
            except Exception as exc:
                model_info.append({"sample": sample_name, "model": model_name, "error": repr(exc)})

    regression_table = pd.concat(regression_rows, ignore_index=True) if regression_rows else pd.DataFrame()
    regression_table.to_csv(tables_dir / "link_level_weighted_regressions.csv", index=False)
    pd.DataFrame(model_info).to_csv(tables_dir / "link_level_regression_diagnostics.csv", index=False)

    # Serial-founder analysis: first observed eligible core venture vs later eligible core ventures.
    serial_base = primary.sort_values(["user_id", "first_founder_entry_date", "founded_rcid"], kind="mergesort").copy()
    serial_base["venture_sequence"] = serial_base.groupby("user_id").cumcount() + 1
    serial_sizes = serial_base.groupby("user_id")["founded_rcid"].nunique().rename("n_eligible_core_ventures")
    serial_base = serial_base.join(serial_sizes, on="user_id")
    serial = serial_base.loc[serial_base["n_eligible_core_ventures"] >= 2].copy()

    serial_summary_rows: list[dict[str, Any]] = []
    serial_regression_rows: list[pd.DataFrame] = []
    if len(serial) > 0:
        for outcome in ["log1p_patents_y0_5", "log1p_citations_y0_5"]:
            grand_mean = float(serial[outcome].mean())
            founder_means = serial.groupby("user_id")[outcome].agg(["mean", "count"])
            between_ss = float((founder_means["count"] * (founder_means["mean"] - grand_mean) ** 2).sum())
            merged_means = serial[["user_id", outcome]].merge(founder_means[["mean"]], left_on="user_id", right_index=True)
            within_ss = float(((merged_means[outcome] - merged_means["mean"]) ** 2).sum())
            total_ss = between_ss + within_ss
            serial_summary_rows.append(
                {
                    "outcome": outcome,
                    "serial_founder_link_rows": int(len(serial)),
                    "serial_founders": int(serial["user_id"].nunique()),
                    "between_founder_ss": between_ss,
                    "within_founder_ss": within_ss,
                    "between_share_total_ss": between_ss / total_ss if total_ss > 0 else np.nan,
                    "within_share_total_ss": within_ss / total_ss if total_ss > 0 else np.nan,
                }
            )

        first = serial.loc[serial["venture_sequence"] == 1].copy()
        later = serial.loc[serial["venture_sequence"] >= 2].copy()
        later_agg = later.groupby("user_id").agg(
            n_later_ventures=("founded_rcid", "nunique"),
            later_mean_log1p_patents_y0_5=("log1p_patents_y0_5", "mean"),
            later_mean_log1p_citations_y0_5=("log1p_citations_y0_5", "mean"),
            later_mean_entry_year=("first_founder_entry_year", "mean"),
        )
        serial_founder = first.merge(later_agg, left_on="user_id", right_index=True, how="inner")
        serial_founder.to_csv(tables_dir / "serial_founder_first_vs_later_panel.csv", index=False)

        controls = pd.DataFrame({
            "first_entry_year": pd.to_numeric(serial_founder["first_founder_entry_year"], errors="coerce").fillna(serial_founder["first_founder_entry_year"].median()),
            "later_mean_entry_year": pd.to_numeric(serial_founder["later_mean_entry_year"], errors="coerce").fillna(serial_founder["later_mean_entry_year"].median()),
        }, index=serial_founder.index)
        for first_y, later_y, name in [
            ("log1p_patents_y0_5", "later_mean_log1p_patents_y0_5", "serial_persistence_patents"),
            ("log1p_citations_y0_5", "later_mean_log1p_citations_y0_5", "serial_persistence_citations"),
        ]:
            try:
                tab, info = simple_wls(
                    serial_founder[later_y], serial_founder[first_y], controls,
                    serial_founder["n_later_ventures"].astype(float), name,
                )
                tab["interpretation"] = "Descriptive predictive continuity; not a causal founder effect."
                serial_regression_rows.append(tab)
                model_info.append(info)
            except Exception as exc:
                model_info.append({"sample": "serial_founder_level", "model": name, "error": repr(exc)})
    else:
        serial_founder = pd.DataFrame()

    pd.DataFrame(serial_summary_rows).to_csv(tables_dir / "serial_founder_variance_decomposition.csv", index=False)
    (pd.concat(serial_regression_rows, ignore_index=True) if serial_regression_rows else pd.DataFrame()).to_csv(
        tables_dir / "serial_founder_persistence_regressions.csv", index=False
    )

    # Figure 1: sample coverage by fixed horizon, taken from validated primary panel counts.
    coverage = pd.DataFrame(
        {
            "horizon": ["0-3 years", "0-5 years", "0-10 years"],
            "core_complete_links": [
                sample_flow.get("core_complete_y0_3", np.nan),
                sample_flow.get("core_complete_y0_5", np.nan),
                sample_flow.get("core_complete_y0_10", np.nan),
            ],
        }
    )
    coverage.to_csv(tables_dir / "core_horizon_coverage.csv", index=False)
    fig, ax = plt.subplots(figsize=(7.0, 4.4))
    ax.bar(coverage["horizon"], coverage["core_complete_links"])
    ax.set_ylabel("Core founder-user-firm links with complete coverage")
    ax.set_title("Validated fixed-horizon coverage")
    save_figure(fig, figures_dir, "figure_1_validated_horizon_coverage")

    # Figure 2: outcome profiles by pre-entry patenting status.
    plot = primary.copy()
    plot["pre_entry_patent_status"] = np.where(
        plot["prehistory_observed"].eq(0), "No observed pre-entry panel history",
        np.where(plot["prior_patents"].fillna(0).gt(0), "Prior patenter", "Observed history, no prior patents"),
    )
    grouped = []
    for group, sub in plot.groupby("pre_entry_patent_status", sort=False):
        grouped.append({
            "group": group,
            "mean_log1p_patents": weighted_mean(sub["log1p_patents_y0_5"], sub["team_weight"]),
            "mean_log1p_citations": weighted_mean(sub["log1p_citations_y0_5"], sub["team_weight"]),
            "n": len(sub),
        })
    grouped_df = pd.DataFrame(grouped)
    grouped_df.to_csv(tables_dir / "outcomes_by_preentry_patenting_status.csv", index=False)
    fig, ax = plt.subplots(figsize=(8.2, 4.6))
    x = np.arange(len(grouped_df))
    width = 0.38
    ax.bar(x - width / 2, grouped_df["mean_log1p_patents"], width, label="log(1 + patents), y0-5")
    ax.bar(x + width / 2, grouped_df["mean_log1p_citations"], width, label="log(1 + US citations), y0-5")
    ax.set_xticks(x)
    ax.set_xticklabels(grouped_df["group"], rotation=15, ha="right")
    ax.set_ylabel("Team-weighted mean")
    ax.set_title("Firm outcomes by observed pre-entry patenting history")
    ax.legend()
    save_figure(fig, figures_dir, "figure_2_outcomes_by_preentry_patenting")

    # Figure 3: formal immigration descriptives.
    imm = primary.copy()
    imm["formal_immigrant_group"] = np.where(imm["formal_immigrant"].eq(1), "Formal immigrant indicator = 1", "Formal immigrant indicator = 0")
    imm_grouped = []
    for group, sub in imm.groupby("formal_immigrant_group", sort=False):
        imm_grouped.append({
            "group": group,
            "mean_log1p_patents": weighted_mean(sub["log1p_patents_y0_5"], sub["team_weight"]),
            "mean_log1p_citations": weighted_mean(sub["log1p_citations_y0_5"], sub["team_weight"]),
            "n": len(sub),
        })
    imm_grouped_df = pd.DataFrame(imm_grouped)
    imm_grouped_df.to_csv(tables_dir / "outcomes_by_formal_immigration.csv", index=False)
    fig, ax = plt.subplots(figsize=(6.8, 4.4))
    x = np.arange(len(imm_grouped_df))
    width = 0.38
    ax.bar(x - width / 2, imm_grouped_df["mean_log1p_patents"], width, label="log(1 + patents), y0-5")
    ax.bar(x + width / 2, imm_grouped_df["mean_log1p_citations"], width, label="log(1 + US citations), y0-5")
    ax.set_xticks(x)
    ax.set_xticklabels(imm_grouped_df["group"], rotation=10, ha="right")
    ax.set_ylabel("Team-weighted mean")
    ax.set_title("Firm outcomes by formal immigration indicator")
    ax.legend()
    save_figure(fig, figures_dir, "figure_3_outcomes_by_formal_immigration")

    # Figure 4: serial-founder predictive continuity, binned by first venture outcome.
    if 'serial_founder' in locals() and len(serial_founder) >= 20:
        fig, ax = plt.subplots(figsize=(6.5, 5.0))
        serial_plot = serial_founder.copy()
        try:
            serial_plot["first_outcome_bin"] = pd.qcut(serial_plot["log1p_patents_y0_5"], q=min(8, serial_plot["log1p_patents_y0_5"].nunique()), duplicates="drop")
            binned = serial_plot.groupby("first_outcome_bin", observed=False).agg(
                first_outcome=("log1p_patents_y0_5", "mean"),
                later_outcome=("later_mean_log1p_patents_y0_5", "mean"),
                n=("user_id", "count"),
            ).dropna()
            ax.scatter(binned["first_outcome"], binned["later_outcome"])
            ax.plot(binned["first_outcome"], binned["later_outcome"])
            ax.set_xlabel("First eligible core venture: log(1 + patents), y0-5")
            ax.set_ylabel("Mean later eligible core ventures: log(1 + patents), y0-5")
            ax.set_title("Serial-founder outcome persistence (descriptive)")
            save_figure(fig, figures_dir, "figure_4_serial_founder_persistence")
        except Exception:
            plt.close(fig)

    # A compact dictionary makes the intended uses and timing restrictions explicit.
    dictionary = pd.DataFrame([
        {"variable": "first_founder_entry_date", "role": "event time", "definition": "First observed selected founder-spell entry date, built as coalesce(startdate, enddate).", "timing_caveat": "Not legal incorporation or registry-verified birth."},
        {"variable": "firm_patents_y0_5", "role": "main outcome", "definition": "Firm patent count from entry year through entry year + 5.", "timing_caveat": "Used only when complete observed calendar coverage is validated."},
        {"variable": "firm_us_citations_y0_5", "role": "main outcome", "definition": "Firm US citation count from entry year through entry year + 5.", "timing_caveat": "Used only when complete observed calendar coverage is validated."},
        {"variable": "prior_*", "role": "pre-entry covariates", "definition": "Inventor-year aggregates strictly before first founder entry year.", "timing_caveat": "Avoid post-entry contamination but omit same-calendar-year information."},
        {"variable": PRIMARY_IMMIGRATION, "role": "formal immigration indicator", "definition": "Project formal immigration field retained without redefining it.", "timing_caveat": "Do not replace with education or work-history proxies; founder_immigrant_any excluded."},
        {"variable": "team_weight", "role": "link-level analytic weight", "definition": "1 divided by number of core eligible founders linked to the firm.", "timing_caveat": "Prevents multi-founder firms from mechanically receiving more total weight."},
        {"variable": "n_core_founders_firm", "role": "team diagnostic", "definition": "Distinct founders in the core eligible y0-5 link sample at a firm.", "timing_caveat": "Very large values motivate the <=10-founder robustness, not an undisclosed sample deletion."},
    ])
    dictionary.to_csv(tables_dir / "phase1_data_dictionary.csv", index=False)

    # Preserve a concise manifest and methods report.
    manifest = {
        "run_id": run_id,
        "validation_run": str(validation_run),
        "validation_report": str(validation_run / "VALIDATION_REPORT.md"),
        "input_final_founder_output": str(V3_FINAL),
        "script_sha256": hash_file(Path(__file__)),
        "primary_sample": "one earliest observed founder link per user-firm; core_founder_title_flag=1; complete observed y0-5 patent and citation horizon",
        "formal_immigration_field": PRIMARY_IMMIGRATION,
        "team_handling": "link-level weight=1/n_core_founders_firm; firm-clustered standard errors; team-level earliest-event panel saved separately",
        "serial_interpretation": "descriptive predictive persistence, not causal founder effects",
        "n_primary_links": int(primary_n),
    }
    write_json(run_dir / "manifest.json", manifest)

    report = [
        "# Founder-quality Phase-1 results - V3",
        "",
        "## Design locked by validation",
        "",
        "- The data source is the validated V3 merged founder output, exactly matching the 24 completed shards.",
        "- Primary unit: one earliest observed founder link per user-firm.",
        "- Primary sample: `core_founder_title_flag = 1` with complete observed 0-5-year patent and citation coverage.",
        "- Event date is first observed founder-spell entry, not legal incorporation.",
        "- Formal immigration is `immig_first_deg_or_job_nonUS`; `founder_immigrant_any` is excluded.",
        "- All link-level descriptive/regression estimates weight founders within a firm to sum to one and use firm-clustered standard errors.",
        "",
        "## Sample",
        "",
        f"- Primary core complete y0-5 founder-user-firm links: `{primary_n}`.",
        f"- Primary firms: `{int(primary['founded_rcid'].nunique())}`.",
        f"- Primary founders: `{int(primary['user_id'].nunique())}`.",
        f"- Share of primary links in firms with >10 core founders: `{float((primary['n_core_founders_firm'] > 10).mean()):.4f}`.",
        "",
        "## Outputs",
        "",
        "- `analysis_panel_primary_core_y0_5/` - limited reproducible analysis panel.",
        "- `team_level_earliest_core_event_y0_5/` - one-row-per-firm robustness panel.",
        "- `tables/` - sample flow, descriptive profiles, weighted link-level regressions, and serial-founder diagnostics.",
        "- `figures/` - PNG and PDF versions of the four primary figures when data support them.",
        "",
        "## Interpretation",
        "",
        "The Phase-1 associations are descriptive/predictive. The serial-founder analysis measures persistence across a person's ventures but does not identify a causal founder effect because founder, firm, and selection mechanisms remain jointly determined.",
        "",
    ]
    (run_dir / "PHASE1_REPORT.md").write_text("\n".join(report))

    # Cleanly release persisted data.
    for frame in [links, analysis, primary_spark]:
        try:
            frame.unpersist()
        except Exception:
            pass
    spark.stop()
    print(f"[INFO] wrote Phase-1 report: {run_dir / 'PHASE1_REPORT.md'}", flush=True)
    print("[INFO] Phase-1 analysis completed", flush=True)


if __name__ == "__main__":
    main()
