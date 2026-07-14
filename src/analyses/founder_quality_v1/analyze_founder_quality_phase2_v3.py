#!/usr/bin/env python3
"""
Founder-quality Phase 2, V3.

Purpose
-------
A timing- and coverage-safe extension of Phase 1. It reads the validated V3
founder output and firm-year panel, verifies stored y1--y5 outcomes directly,
and estimates descriptive/predictive founder-quality associations without
silently treating missing pre-entry history as zero.

Main analytical improvements relative to Phase 1
-------------------------------------------------
1. Primary post-entry outcome is years 1--5, not 0--5. The entry calendar year
   can contain firm activity before the observed founder-spell start date.
2. Historical-founder covariates are analysed in an observed-prehistory sample
   (prior_years_observed >= 1). A separate full trait sample is retained for
   entry-time variables such as the project immigration proxy and STEM role.
3. The script verifies stored y1--y5 outcomes against direct sums of the
   validated firm-year panel before writing results.
4. Link-level estimates give each firm total weight one across observed core
   founders, cluster by firm, and do not control for an ex-post all-time team
   count. A one-row-per-firm earliest-entry team analysis is reported separately.
5. Continuous predictors are standardized within each estimation sample;
   coefficients are therefore per one-standard-deviation change. Binary
   indicators remain in 0/1 units.
6. Both log1p-WLS and PPML are reported. All interpretations remain descriptive.
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
from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel

PROJECT = Path("/home/epiga/revelio_labs")
V3_FINAL = PROJECT / "output/founder_firm_outcomes_v3/founder_firm_outcomes_v3"
V3_FIRM_YEAR = PROJECT / "output/founder_firm_outcomes_v3/firm_year_outcomes_current_rcid_v3"
PRIMARY_IMMIGRATION = "immig_first_deg_or_job_nonUS"
USER_FIRM_KEY = ["user_id", "founded_rcid"]


def require_columns(df: DataFrame, cols: Iterable[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def make_link_id(df: DataFrame) -> DataFrame:
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("__NULL__")) for c in [
        "user_id", "founded_rcid", "founder_position_id", "first_founder_entry_year"
    ]]
    return df.withColumn("analysis_link_id", F.sha2(F.concat_ws("||", *parts), 256))


def scalar_metrics(df: DataFrame, expressions: list[F.Column]) -> dict[str, Any]:
    row = df.agg(*expressions).first()
    return {} if row is None else row.asDict(recursive=True)


def write_spark_csv(df: DataFrame, path: Path) -> None:
    df.coalesce(1).write.mode("errorifexists").option("header", True).csv(str(path))


def normal_two_sided_p(z: np.ndarray) -> np.ndarray:
    z = np.asarray(z, dtype=float)
    return np.array([math.erfc(abs(float(v)) / math.sqrt(2.0)) if np.isfinite(v) else np.nan for v in z])


def collapse_rare_categories(s: pd.Series, min_n: int = 60, max_levels: int = 80) -> pd.Series:
    x = s.astype("string").fillna("__MISSING__")
    counts = x.value_counts(dropna=False)
    keep = counts[counts >= min_n].index.tolist()[:max_levels]
    return x.where(x.isin(keep), "__OTHER__")


def prepare_design(
    df: pd.DataFrame,
    numeric: list[str],
    binary: list[str],
    categorical: list[str],
) -> tuple[pd.DataFrame, dict[str, dict[str, float]], list[str]]:
    """Build a transparent, centered design matrix.

    Numeric fields are z-scored within the estimation sample. Binary fields stay
    in 0/1 units. Missing numeric values receive median imputation plus a missing
    indicator; binary fields use 0 only after an explicit missing indicator is
    added. This avoids silently interpreting absent records as substantive zeros.
    """
    pieces = [pd.DataFrame({"const": np.ones(len(df), dtype=float)}, index=df.index)]
    scaling: dict[str, dict[str, float]] = {}
    used: list[str] = []

    for c in numeric:
        if c not in df.columns:
            continue
        x = pd.to_numeric(df[c], errors="coerce")
        if x.notna().sum() == 0:
            continue
        med = float(x.median())
        sd = float(x.std(ddof=0))
        if not np.isfinite(sd) or sd <= 0:
            continue
        z = (x.fillna(med).astype(float) - med) / sd
        name = f"z_{c}"
        pieces.append(pd.DataFrame({name: z}, index=df.index))
        if x.isna().any():
            pieces.append(pd.DataFrame({f"missing_{c}": x.isna().astype(float)}, index=df.index))
        scaling[c] = {"median": med, "sd": sd}
        used.append(c)

    for c in binary:
        if c not in df.columns:
            continue
        x = pd.to_numeric(df[c], errors="coerce")
        if x.notna().sum() == 0 or x.nunique(dropna=True) <= 1:
            continue
        pieces.append(pd.DataFrame({c: x.fillna(0.0).astype(float)}, index=df.index))
        if x.isna().any():
            pieces.append(pd.DataFrame({f"missing_{c}": x.isna().astype(float)}, index=df.index))
        used.append(c)

    for c in categorical:
        if c not in df.columns:
            continue
        cat = collapse_rare_categories(df[c])
        dummies = pd.get_dummies(cat, prefix=c, dtype=float)
        if dummies.shape[1] > 1:
            dummies = dummies.iloc[:, 1:]
        if dummies.shape[1] > 0:
            pieces.append(dummies)
            used.append(c)

    X = pd.concat(pieces, axis=1)
    X = X.loc[:, X.nunique(dropna=False) > 1]
    if "const" not in X.columns:
        X.insert(0, "const", 1.0)
    return X.astype(float), scaling, used


def _valid_rows(y: pd.Series, X: pd.DataFrame, weights: pd.Series, clusters: pd.Series) -> tuple[np.ndarray, np.ndarray, np.ndarray, np.ndarray, list[str]]:
    valid = (
        y.notna() & np.isfinite(y) & weights.notna() & np.isfinite(weights) & (weights > 0) & clusters.notna()
    )
    Xv = X.loc[valid].copy()
    keep = Xv.columns[(Xv.abs().sum(axis=0) > 0).to_numpy()]
    Xv = Xv.loc[:, keep]
    return (
        y.loc[valid].astype(float).to_numpy(),
        Xv.to_numpy(dtype=float),
        weights.loc[valid].astype(float).to_numpy(),
        clusters.loc[valid].astype(str).to_numpy(),
        list(Xv.columns),
    )


def clustered_covariance(x: np.ndarray, bread: np.ndarray, scores: np.ndarray, clusters: np.ndarray) -> np.ndarray:
    """CR1-style sandwich covariance. `scores` is n x p observation score matrix."""
    n, p = x.shape
    unique = np.unique(clusters)
    g = len(unique)
    meat = np.zeros((p, p), dtype=float)
    for cl in unique:
        s = scores[clusters == cl].sum(axis=0)
        meat += np.outer(s, s)
    correction = (g / max(g - 1, 1)) * ((n - 1) / max(n - p, 1)) if g > 1 else (n / max(n - p, 1))
    return correction * (bread @ meat @ bread)


def clustered_wls(y: pd.Series, X: pd.DataFrame, weights: pd.Series, clusters: pd.Series, model: str, sample: str) -> tuple[pd.DataFrame, dict[str, Any]]:
    yv, x, w, cl, terms = _valid_rows(y, X, weights, clusters)
    n, p = x.shape
    g = len(np.unique(cl))
    if n <= p or g < 2:
        raise RuntimeError(f"Insufficient rows for WLS {sample}/{model}: n={n}, p={p}, clusters={g}")
    xtwx = x.T @ (w[:, None] * x)
    bread = np.linalg.pinv(xtwx, rcond=1e-11)
    beta = bread @ (x.T @ (w * yv))
    resid = yv - x @ beta
    cov = clustered_covariance(x, bread, x * (w * resid)[:, None], cl)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    out = pd.DataFrame({
        "estimator": "WLS_log1p", "sample": sample, "model": model, "term": terms,
        "estimate": beta, "cluster_se": se, "z_approx": z,
        "p_value_normal_approx": normal_two_sided_p(z),
        "n_observations": n, "n_clusters": g,
    })
    info = {
        "estimator": "WLS_log1p", "sample": sample, "model": model,
        "n_observations": int(n), "n_clusters": int(g), "n_parameters": int(p),
        "matrix_rank": int(np.linalg.matrix_rank(x)), "condition_number": float(np.linalg.cond(xtwx)),
        "converged": True, "iterations": 1,
    }
    return out, info


def clustered_ppml(y: pd.Series, X: pd.DataFrame, weights: pd.Series, clusters: pd.Series, model: str, sample: str, max_iter: int = 150, tol: float = 1e-9) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Small-sample PPML IRLS with analytic weights and firm-clustered CR1 SEs."""
    yv, x, w, cl, terms = _valid_rows(y, X, weights, clusters)
    n, p = x.shape
    g = len(np.unique(cl))
    if np.any(yv < 0) or n <= p or g < 2:
        raise RuntimeError(f"Invalid rows for PPML {sample}/{model}: n={n}, p={p}, clusters={g}")

    beta = np.zeros(p, dtype=float)
    converged = False
    for it in range(1, max_iter + 1):
        eta = np.clip(x @ beta, -20.0, 20.0)
        mu = np.exp(eta)
        ww = w * mu
        xtwx = x.T @ (ww[:, None] * x)
        bread = np.linalg.pinv(xtwx, rcond=1e-11)
        score = x.T @ (w * (yv - mu))
        step = bread @ score
        # Conservative backtracking prevents unstable jumps with sparse outcomes.
        step_scale = 1.0
        old_obj = float(np.sum(w * (yv * eta - mu)))
        accepted = False
        for _ in range(20):
            candidate = beta + step_scale * step
            eta_c = np.clip(x @ candidate, -20.0, 20.0)
            mu_c = np.exp(eta_c)
            obj_c = float(np.sum(w * (yv * eta_c - mu_c)))
            if np.isfinite(obj_c) and obj_c >= old_obj - 1e-9:
                beta = candidate
                accepted = True
                break
            step_scale *= 0.5
        if not accepted:
            beta = beta + 0.1 * step
        if np.max(np.abs(step_scale * step)) < tol:
            converged = True
            break

    eta = np.clip(x @ beta, -20.0, 20.0)
    mu = np.exp(eta)
    xtwx = x.T @ ((w * mu)[:, None] * x)
    bread = np.linalg.pinv(xtwx, rcond=1e-11)
    cov = clustered_covariance(x, bread, x * (w * (yv - mu))[:, None], cl)
    se = np.sqrt(np.maximum(np.diag(cov), 0.0))
    z = np.divide(beta, se, out=np.full_like(beta, np.nan), where=se > 0)
    out = pd.DataFrame({
        "estimator": "PPML", "sample": sample, "model": model, "term": terms,
        "estimate": beta, "cluster_se": se, "z_approx": z,
        "p_value_normal_approx": normal_two_sided_p(z),
        "n_observations": n, "n_clusters": g,
    })
    info = {
        "estimator": "PPML", "sample": sample, "model": model,
        "n_observations": int(n), "n_clusters": int(g), "n_parameters": int(p),
        "matrix_rank": int(np.linalg.matrix_rank(x)), "condition_number": float(np.linalg.cond(xtwx)),
        "converged": bool(converged), "iterations": int(it),
    }
    return out, info


def save_figure(fig: plt.Figure, figures_dir: Path, stem: str) -> None:
    fig.tight_layout()
    fig.savefig(figures_dir / f"{stem}.png", dpi=220, bbox_inches="tight")
    fig.savefig(figures_dir / f"{stem}.pdf", bbox_inches="tight")
    plt.close(fig)


def plot_coefficients(tab: pd.DataFrame, figures_dir: Path, stem: str, estimator: str, title: str) -> None:
    wanted = [
        "formal_immigrant_proxy", "founder_stem_at_founder",
        "z_log1p_prior_patents", "z_log1p_prior_citations", "z_log1p_prior_distinct_rcids",
        "z_prior_years_observed", "z_founder_seniority",
    ]
    d = tab[(tab["estimator"] == estimator) & (tab["term"].isin(wanted))].copy()
    if d.empty:
        return
    d["lo"] = d["estimate"] - 1.96 * d["cluster_se"]
    d["hi"] = d["estimate"] + 1.96 * d["cluster_se"]
    d["label"] = d["model"] + " | " + d["term"]
    d = d.sort_values("estimate")
    fig, ax = plt.subplots(figsize=(9.2, max(4.5, 0.26 * len(d) + 1.5)))
    ax.errorbar(d["estimate"], np.arange(len(d)), xerr=[d["estimate"] - d["lo"], d["hi"] - d["estimate"]], fmt="o")
    ax.axvline(0, linewidth=1)
    ax.set_yticks(np.arange(len(d)))
    ax.set_yticklabels(d["label"])
    ax.set_xlabel("Estimate with 95% CI")
    ax.set_title(title)
    save_figure(fig, figures_dir, stem)


def weighted_mean(x: pd.Series, w: pd.Series) -> float:
    valid = x.notna() & w.notna() & np.isfinite(x) & np.isfinite(w) & (w > 0)
    return float(np.average(x[valid], weights=w[valid])) if valid.any() else float("nan")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--validation-run", required=True)
    parser.add_argument("--out-root", default=str(PROJECT / "output/founder_quality_v1/phase2_v3"))
    parser.add_argument("--shuffle-partitions", type=int, default=320)
    args = parser.parse_args()

    validation_run = Path(args.validation_run).resolve()
    eligibility_path = validation_run / "candidate_link_horizon_eligibility"
    if not (validation_run / "VALIDATION_REPORT.md").exists():
        raise FileNotFoundError("Validation report is required before Phase 2")
    if not eligibility_path.exists():
        raise FileNotFoundError(f"Missing eligibility panel: {eligibility_path}")

    run_dir = Path(args.out_root) / datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    run_dir.mkdir(parents=True, exist_ok=False)
    tables_dir = run_dir / "tables"; tables_dir.mkdir()
    figures_dir = run_dir / "figures"; figures_dir.mkdir()

    spark = (SparkSession.builder.appName("founder_quality_phase2_v3")
             .config("spark.sql.adaptive.enabled", "true")
             .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] run_dir={run_dir}", flush=True)
    print(f"[INFO] validation_run={validation_run}", flush=True)

    final = spark.read.parquet(str(V3_FINAL))
    firm_year = spark.read.parquet(str(V3_FIRM_YEAR))
    eligibility = spark.read.parquet(str(eligibility_path))
    require_columns(final, [
        "user_id", "founded_rcid", "founder_position_id", "first_founder_entry_date",
        "first_founder_entry_year", "founder_spell_startdate", "core_founder_title_flag",
        PRIMARY_IMMIGRATION, "firm_patents_y1_5", "firm_us_citations_y1_5",
        "prior_patents", "prior_us_citations", "prior_years_observed",
    ], "V3 founder output")
    require_columns(firm_year, ["firm_rcid", "year", "firm_patents", "firm_us_citations"], "V3 firm-year")
    require_columns(eligibility, ["analysis_link_id", "eligible_patents_y0_5", "eligible_citations_y0_5"], "validated eligibility")

    optional = [
        "founder_naics3", "founder_metro_area", "founder_country", "founder_mapped_role_v3",
        "founder_stem_at_founder", "founder_seniority", "prior_unique_current_rcids",
        "prior_unique_parent_rcids", "prior_unique_onet_codes", "prior_unique_naics6",
        "founder_company_name", "founder_spell_enddate",
    ]
    keep = [c for c in list(dict.fromkeys([
        "user_id", "founded_rcid", "founder_position_id", "first_founder_entry_date",
        "first_founder_entry_year", "founder_spell_startdate", "founder_spell_enddate",
        "core_founder_title_flag", PRIMARY_IMMIGRATION, "firm_patents_y1_5", "firm_us_citations_y1_5",
        "prior_patents", "prior_us_citations", "prior_years_observed",
    ] + optional)) if c in final.columns]

    # Exactly one earliest observed founder link per person-firm.
    w = Window.partitionBy(*USER_FIRM_KEY).orderBy(F.col("first_founder_entry_date").asc_nulls_last(), F.col("founder_position_id").asc_nulls_last())
    links = (final.select(*keep).withColumn("_rn", F.row_number().over(w)).filter(F.col("_rn") == 1).drop("_rn"))
    links = make_link_id(links).persist(StorageLevel.MEMORY_AND_DISK)
    dedup_n = links.count()

    elig = eligibility.select(
        "analysis_link_id", "eligible_patents_y0_5", "eligible_citations_y0_5",
        *[c for c in ["complete_calendar_y0_5"] if c in eligibility.columns]
    ).dropDuplicates(["analysis_link_id"])
    base = (links.join(elig, "analysis_link_id", "inner")
            .filter(F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1)
            .filter((F.col("eligible_patents_y0_5") == 1) & (F.col("eligible_citations_y0_5") == 1))
            .persist(StorageLevel.MEMORY_AND_DISK))
    base_n = base.count()

    # Direct y1--y5 verification. y0--y5 completeness is a necessary condition;
    # direct calendar checks below are the sufficient condition for the timing-safe outcome.
    keys = base.select("analysis_link_id", "user_id", "founded_rcid", "first_founder_entry_year")
    fy = firm_year.select(
        F.col("firm_rcid").alias("fy_firm_rcid"), F.col("year").cast("int").alias("fy_year"),
        F.col("firm_patents").cast("double").alias("fy_patents"),
        F.col("firm_us_citations").cast("double").alias("fy_citations"),
    )
    direct = (keys.join(fy, keys.founded_rcid == fy.fy_firm_rcid, "left")
              .filter((F.col("fy_year") >= F.col("first_founder_entry_year") + F.lit(1)) &
                      (F.col("fy_year") <= F.col("first_founder_entry_year") + F.lit(5)))
              .groupBy("analysis_link_id")
              .agg(
                  F.countDistinct("fy_year").alias("n_y1_5_calendar_years"),
                  F.sum(F.col("fy_patents").isNull().cast("int")).alias("n_y1_5_missing_patent_fields"),
                  F.sum(F.col("fy_citations").isNull().cast("int")).alias("n_y1_5_missing_citation_fields"),
                  F.sum("fy_patents").alias("direct_firm_patents_y1_5"),
                  F.sum("fy_citations").alias("direct_firm_us_citations_y1_5"),
              ))
    analysis = (base.join(direct, "analysis_link_id", "left")
                .withColumn("eligible_direct_y1_5", (
                    (F.col("n_y1_5_calendar_years") == 5) &
                    (F.coalesce(F.col("n_y1_5_missing_patent_fields"), F.lit(999)) == 0) &
                    (F.coalesce(F.col("n_y1_5_missing_citation_fields"), F.lit(999)) == 0)
                ).cast("int")))
    check = scalar_metrics(analysis, [
        F.count("*").alias("core_y0_5_complete_links"),
        F.sum((F.col("eligible_direct_y1_5") == 1).cast("int")).alias("direct_y1_5_complete_links"),
        F.sum(F.when((F.col("eligible_direct_y1_5") == 1) &
                     (F.abs(F.coalesce(F.col("firm_patents_y1_5").cast("double"), F.lit(-999999.0)) - F.col("direct_firm_patents_y1_5")) > F.lit(1e-8)), 1).otherwise(0)).alias("patent_y1_5_mismatches"),
        F.sum(F.when((F.col("eligible_direct_y1_5") == 1) &
                     (F.abs(F.coalesce(F.col("firm_us_citations_y1_5").cast("double"), F.lit(-999999.0)) - F.col("direct_firm_us_citations_y1_5")) > F.lit(1e-8)), 1).otherwise(0)).alias("citation_y1_5_mismatches"),
        F.sum(F.col("founder_spell_startdate").isNotNull().cast("int")).alias("links_with_observed_startdate"),
    ])
    if check["direct_y1_5_complete_links"] != check["core_y0_5_complete_links"]:
        raise RuntimeError(f"Direct y1--y5 completeness is not implied as expected: {check}")
    if check["patent_y1_5_mismatches"] != 0 or check["citation_y1_5_mismatches"] != 0:
        raise RuntimeError(f"Stored/direct y1--y5 mismatch: {check}")

    analysis = (analysis.filter(F.col("eligible_direct_y1_5") == 1)
                .withColumn("firm_patents_y1_5_direct", F.col("direct_firm_patents_y1_5"))
                .withColumn("firm_us_citations_y1_5_direct", F.col("direct_firm_us_citations_y1_5"))
                .withColumn("log1p_patents_y1_5", F.log1p(F.col("direct_firm_patents_y1_5")))
                .withColumn("log1p_citations_y1_5", F.log1p(F.col("direct_firm_us_citations_y1_5")))
                .withColumn("formal_immigrant_proxy", F.col(PRIMARY_IMMIGRATION).cast("double"))
                .withColumn("has_preentry_panel_history", (F.col("prior_years_observed") > 0).cast("int"))
                .withColumn("log1p_prior_patents", F.log1p(F.greatest(F.coalesce(F.col("prior_patents").cast("double"), F.lit(0.0)), F.lit(0.0))))
                .withColumn("log1p_prior_citations", F.log1p(F.greatest(F.coalesce(F.col("prior_us_citations").cast("double"), F.lit(0.0)), F.lit(0.0))))
                .withColumn("log1p_prior_distinct_rcids", F.log1p(F.greatest(F.coalesce(F.col("prior_unique_current_rcids").cast("double"), F.lit(0.0)), F.lit(0.0))))
                .persist(StorageLevel.MEMORY_AND_DISK))

    # Link weights are only for the duplicated-outcome problem in multi-founder firms;
    # the number itself is deliberately not an explanatory covariate.
    team_sizes = analysis.groupBy("founded_rcid").agg(F.countDistinct("user_id").alias("n_eligible_core_founders_firm"))
    analysis = (analysis.join(team_sizes, "founded_rcid", "left")
                .withColumn("team_weight", F.lit(1.0) / F.col("n_eligible_core_founders_firm"))
                .persist(StorageLevel.MEMORY_AND_DISK))

    strict = analysis.filter(F.col("founder_spell_startdate").isNotNull()).persist(StorageLevel.MEMORY_AND_DISK)
    observed_history = strict.filter(F.col("has_preentry_panel_history") == 1).persist(StorageLevel.MEMORY_AND_DISK)

    flow = {
        **check,
        "deduplicated_user_firm_links": int(dedup_n),
        "full_core_y1_5_links": int(analysis.count()),
        "strict_observed_startdate_links": int(strict.count()),
        "strict_observed_prehistory_links": int(observed_history.count()),
        "full_core_y1_5_firms": int(analysis.select("founded_rcid").distinct().count()),
        "strict_observed_prehistory_firms": int(observed_history.select("founded_rcid").distinct().count()),
    }
    pd.DataFrame([flow]).to_csv(tables_dir / "sample_flow_phase2.csv", index=False)

    # Limited, reproducible analysis panel.
    panel_cols = [
        "analysis_link_id", "user_id", "founded_rcid", "founder_position_id",
        "first_founder_entry_date", "first_founder_entry_year", "founder_spell_startdate",
        "formal_immigrant_proxy", "founder_stem_at_founder", "founder_seniority",
        "founder_naics3", "founder_metro_area", "founder_country", "founder_mapped_role_v3",
        "prior_patents", "prior_us_citations", "prior_years_observed", "prior_unique_current_rcids",
        "has_preentry_panel_history", "log1p_prior_patents", "log1p_prior_citations", "log1p_prior_distinct_rcids",
        "firm_patents_y1_5_direct", "firm_us_citations_y1_5_direct", "log1p_patents_y1_5", "log1p_citations_y1_5",
        "n_eligible_core_founders_firm", "team_weight",
    ]
    panel_cols = [c for c in panel_cols if c in analysis.columns]
    analysis.select(*panel_cols).write.mode("errorifexists").parquet(str(run_dir / "analysis_panel_core_y1_5"))

    # Pull manageable samples into pandas for transparent model estimation.
    pandas_cols = panel_cols
    full_pd = analysis.select(*pandas_cols).toPandas()
    strict_pd = strict.select(*pandas_cols).toPandas()
    hist_pd = observed_history.select(*pandas_cols).toPandas()
    for df in [full_pd, strict_pd, hist_pd]:
        for c in [
            "formal_immigrant_proxy", "founder_stem_at_founder", "founder_seniority",
            "first_founder_entry_year", "prior_patents", "prior_us_citations", "prior_years_observed",
            "prior_unique_current_rcids", "log1p_prior_patents", "log1p_prior_citations",
            "log1p_prior_distinct_rcids", "firm_patents_y1_5_direct", "firm_us_citations_y1_5_direct",
            "log1p_patents_y1_5", "log1p_citations_y1_5", "team_weight",
        ]:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce")
        if "team_weight" in df.columns:
            df["team_weight"] = df["team_weight"].fillna(1.0)

    # Descriptive profiles: full traits and observed-history comparison are deliberately separated.
    desc_rows: list[dict[str, Any]] = []
    for sample_name, df in [("full_core_y1_5", full_pd), ("strict_startdate", strict_pd), ("strict_observed_prehistory", hist_pd)]:
        for trait, label in [
            ("formal_immigrant_proxy", "project_immigration_proxy"),
            ("founder_stem_at_founder", "stem_role_at_entry"),
            ("has_preentry_panel_history", "preentry_panel_observed"),
        ]:
            if trait not in df.columns:
                continue
            for value, sub in df.assign(_g=df[trait].fillna(-1)).groupby("_g"):
                desc_rows.append({
                    "sample": sample_name, "trait": label, "group": "missing" if value == -1 else str(int(value)),
                    "n_links": int(len(sub)),
                    "weighted_mean_log1p_patents_y1_5": weighted_mean(sub["log1p_patents_y1_5"], sub["team_weight"]),
                    "weighted_mean_log1p_citations_y1_5": weighted_mean(sub["log1p_citations_y1_5"], sub["team_weight"]),
                    "weighted_mean_patents_y1_5": weighted_mean(sub["firm_patents_y1_5_direct"], sub["team_weight"]),
                    "weighted_mean_citations_y1_5": weighted_mean(sub["firm_us_citations_y1_5_direct"], sub["team_weight"]),
                })
    pd.DataFrame(desc_rows).to_csv(tables_dir / "descriptive_profiles_y1_5.csv", index=False)

    # Nested association models. No claim is causal.
    specs = [
        {
            "name": "A_full_traits_year_industry",
            "df": full_pd,
            "numeric": ["founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3"],
        },
        {
            "name": "B_full_traits_plus_location_role",
            "df": full_pd,
            "numeric": ["founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3", "founder_metro_area", "founder_mapped_role_v3"],
        },
        {
            "name": "C_strict_history_year_industry",
            "df": hist_pd,
            "numeric": ["log1p_prior_patents", "log1p_prior_citations", "log1p_prior_distinct_rcids", "prior_years_observed", "founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3"],
        },
        {
            "name": "D_strict_history_plus_location_role",
            "df": hist_pd,
            "numeric": ["log1p_prior_patents", "log1p_prior_citations", "log1p_prior_distinct_rcids", "prior_years_observed", "founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3", "founder_metro_area", "founder_mapped_role_v3"],
        },
        {
            "name": "E_strict_history_patents_only",
            "df": hist_pd,
            "numeric": ["log1p_prior_patents", "log1p_prior_distinct_rcids", "prior_years_observed", "founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3", "founder_metro_area", "founder_mapped_role_v3"],
        },
        {
            "name": "F_strict_history_citations_only",
            "df": hist_pd,
            "numeric": ["log1p_prior_citations", "log1p_prior_distinct_rcids", "prior_years_observed", "founder_seniority"],
            "binary": ["formal_immigrant_proxy", "founder_stem_at_founder"],
            "categorical": ["first_founder_entry_year", "founder_naics3", "founder_metro_area", "founder_mapped_role_v3"],
        },
    ]
    result_tabs: list[pd.DataFrame] = []
    diag_rows: list[dict[str, Any]] = []
    scales: list[dict[str, Any]] = []
    for spec in specs:
        df = spec["df"].copy()
        X, scaling, used = prepare_design(df, spec["numeric"], spec["binary"], spec["categorical"])
        for var, vals in scaling.items():
            scales.append({"model": spec["name"], "variable": var, **vals})
        for outcome_col, count_col, label in [
            ("log1p_patents_y1_5", "firm_patents_y1_5_direct", "patents_y1_5"),
            ("log1p_citations_y1_5", "firm_us_citations_y1_5_direct", "citations_y1_5"),
        ]:
            try:
                tab, info = clustered_wls(df[outcome_col], X, df["team_weight"], df["founded_rcid"], label, spec["name"])
                tab["controls"] = ";".join(used)
                result_tabs.append(tab); diag_rows.append(info)
            except Exception as exc:
                diag_rows.append({"estimator": "WLS_log1p", "sample": spec["name"], "model": label, "error": repr(exc)})
            try:
                tab, info = clustered_ppml(df[count_col], X, df["team_weight"], df["founded_rcid"], label, spec["name"])
                tab["controls"] = ";".join(used)
                result_tabs.append(tab); diag_rows.append(info)
            except Exception as exc:
                diag_rows.append({"estimator": "PPML", "sample": spec["name"], "model": label, "error": repr(exc)})
    reg = pd.concat(result_tabs, ignore_index=True) if result_tabs else pd.DataFrame()
    reg.to_csv(tables_dir / "phase2_link_level_regressions.csv", index=False)
    pd.DataFrame(diag_rows).to_csv(tables_dir / "phase2_regression_diagnostics.csv", index=False)
    pd.DataFrame(scales).to_csv(tables_dir / "phase2_standardization.csv", index=False)

    # Team-level counterpart: earliest core entry year at a firm, then aggregate contemporaneous founders.
    team_pd = strict_pd.copy()
    if not team_pd.empty:
        first_year = team_pd.groupby("founded_rcid")["first_founder_entry_year"].transform("min")
        team_pd = team_pd.loc[team_pd["first_founder_entry_year"] == first_year].copy()
        agg = {
            "first_founder_entry_year": "first", "firm_patents_y1_5_direct": "first", "firm_us_citations_y1_5_direct": "first",
            "log1p_patents_y1_5": "first", "log1p_citations_y1_5": "first", "formal_immigrant_proxy": "mean",
            "founder_stem_at_founder": "mean", "founder_seniority": "mean", "founder_naics3": "first",
            "founder_metro_area": "first", "founder_mapped_role_v3": "first", "log1p_prior_patents": "mean",
            "log1p_prior_citations": "mean", "log1p_prior_distinct_rcids": "mean", "prior_years_observed": "mean",
        }
        agg = {k: v for k, v in agg.items() if k in team_pd.columns}
        team = team_pd.groupby("founded_rcid", dropna=False).agg(agg).reset_index()
        team["n_founders_at_earliest_entry"] = team_pd.groupby("founded_rcid")["user_id"].nunique().reindex(team["founded_rcid"]).to_numpy()
        team["unit_weight"] = 1.0
        Xteam, team_scaling, used_team = prepare_design(
            team,
            ["log1p_prior_patents", "log1p_prior_citations", "log1p_prior_distinct_rcids", "prior_years_observed", "founder_seniority", "n_founders_at_earliest_entry"],
            ["formal_immigrant_proxy", "founder_stem_at_founder"],
            ["first_founder_entry_year", "founder_naics3", "founder_metro_area", "founder_mapped_role_v3"],
        )
        # Each firm is a unique cluster: this reduces to HC1-style sandwich inference.
        for outcome_col, count_col, label in [
            ("log1p_patents_y1_5", "firm_patents_y1_5_direct", "team_patents_y1_5"),
            ("log1p_citations_y1_5", "firm_us_citations_y1_5_direct", "team_citations_y1_5"),
        ]:
            try:
                tab, info = clustered_wls(team[outcome_col], Xteam, team["unit_weight"], team["founded_rcid"], label, "G_team_earliest_entry")
                tab["controls"] = ";".join(used_team); result_tabs.append(tab); diag_rows.append(info)
            except Exception as exc:
                diag_rows.append({"estimator": "WLS_log1p", "sample": "G_team_earliest_entry", "model": label, "error": repr(exc)})
            try:
                tab, info = clustered_ppml(team[count_col], Xteam, team["unit_weight"], team["founded_rcid"], label, "G_team_earliest_entry")
                tab["controls"] = ";".join(used_team); result_tabs.append(tab); diag_rows.append(info)
            except Exception as exc:
                diag_rows.append({"estimator": "PPML", "sample": "G_team_earliest_entry", "model": label, "error": repr(exc)})
        team.to_csv(tables_dir / "team_level_earliest_entry_panel.csv", index=False)
        pd.DataFrame([{"team_level_firms": int(len(team)), "mean_team_size_at_earliest_entry": float(team["n_founders_at_earliest_entry"].mean()), "max_team_size_at_earliest_entry": int(team["n_founders_at_earliest_entry"].max())}]).to_csv(tables_dir / "team_level_summary_phase2.csv", index=False)

    # Re-write regression outputs after appending team estimates.
    reg = pd.concat(result_tabs, ignore_index=True) if result_tabs else pd.DataFrame()
    reg.to_csv(tables_dir / "phase2_link_level_and_team_regressions.csv", index=False)
    pd.DataFrame(diag_rows).to_csv(tables_dir / "phase2_all_regression_diagnostics.csv", index=False)

    # Serial-founder persistence: strict-date, y1--y5 eligible ventures. Sequence is first OBSERVED eligible core venture.
    serial = strict_pd.sort_values(["user_id", "first_founder_entry_date", "founded_rcid"], kind="mergesort").copy()
    serial["venture_sequence_observed_eligible"] = serial.groupby("user_id").cumcount() + 1
    serial["n_observed_eligible_ventures"] = serial.groupby("user_id")["founded_rcid"].transform("nunique")
    serial = serial.loc[serial["n_observed_eligible_ventures"] >= 2].copy()
    serial_rows = []
    if not serial.empty:
        for outcome in ["log1p_patents_y1_5", "log1p_citations_y1_5"]:
            founder_means = serial.groupby("user_id")[outcome].agg(["mean", "count"])
            grand = float(serial[outcome].mean())
            between = float((founder_means["count"] * (founder_means["mean"] - grand) ** 2).sum())
            temp = serial[["user_id", outcome]].merge(founder_means[["mean"]], left_on="user_id", right_index=True)
            within = float(((temp[outcome] - temp["mean"]) ** 2).sum())
            serial_rows.append({"outcome": outcome, "serial_link_rows": int(len(serial)), "serial_founders": int(serial["user_id"].nunique()), "between_share_total_ss": between / (between + within) if (between + within) else np.nan})
        first = serial.loc[serial["venture_sequence_observed_eligible"] == 1].copy()
        later = serial.loc[serial["venture_sequence_observed_eligible"] >= 2].groupby("user_id").agg(
            later_mean_log1p_patents_y1_5=("log1p_patents_y1_5", "mean"),
            later_mean_log1p_citations_y1_5=("log1p_citations_y1_5", "mean"),
            later_mean_entry_year=("first_founder_entry_year", "mean"),
            n_later_ventures=("founded_rcid", "nunique"),
        )
        sf = first.merge(later, left_on="user_id", right_index=True, how="inner")
        serial_tabs=[]
        for initial, later_out, label in [
            ("log1p_patents_y1_5", "later_mean_log1p_patents_y1_5", "serial_y1_5_patents"),
            ("log1p_citations_y1_5", "later_mean_log1p_citations_y1_5", "serial_y1_5_citations"),
        ]:
            Xs, _, used_s = prepare_design(
                sf.assign(first_outcome=sf[initial]),
                numeric=["first_outcome"], binary=[], categorical=["first_founder_entry_year", "later_mean_entry_year"],
            )
            # Founder is the analysis row; distinct clusters are appropriate here.
            tab, info = clustered_wls(sf[later_out], Xs, pd.Series(1.0, index=sf.index), sf["user_id"], label, "H_serial_founders")
            tab["controls"] = ";".join(used_s)
            serial_tabs.append(tab); diag_rows.append(info)
        pd.concat(serial_tabs, ignore_index=True).to_csv(tables_dir / "serial_founder_persistence_y1_5.csv", index=False)
        sf.to_csv(tables_dir / "serial_founder_first_vs_later_y1_5.csv", index=False)
    pd.DataFrame(serial_rows).to_csv(tables_dir / "serial_founder_variance_y1_5.csv", index=False)

    # Figures.
    plot_coefficients(reg, figures_dir, "figure_1_wls_coefficients_y1_5", "WLS_log1p", "Founder associations, y1--y5: log1p WLS")
    plot_coefficients(reg, figures_dir, "figure_2_ppml_coefficients_y1_5", "PPML", "Founder associations, y1--y5: PPML")
    if not hist_pd.empty:
        fig, ax = plt.subplots(figsize=(7.4, 4.7))
        groups = hist_pd.assign(_g=(hist_pd["prior_patents"] > 0).astype(int)).groupby("_g")
        labels=[]; vals=[]
        for g, sub in groups:
            labels.append("Prior patents" if g == 1 else "No prior patents")
            vals.append(weighted_mean(sub["log1p_patents_y1_5"], sub["team_weight"]))
        ax.bar(labels, vals)
        ax.set_ylabel("Weighted mean log(1 + firm patents), years 1--5")
        ax.set_title("Post-entry patents by observed pre-entry patenting")
        save_figure(fig, figures_dir, "figure_3_prior_patenting_y1_5")
    if not serial.empty:
        fig, ax = plt.subplots(figsize=(6.8, 5.2))
        ax.scatter(sf["log1p_patents_y1_5"], sf["later_mean_log1p_patents_y1_5"], alpha=0.25, s=12)
        ax.set_xlabel("First observed eligible venture: log(1 + patents), years 1--5")
        ax.set_ylabel("Later observed eligible ventures: mean log(1 + patents), years 1--5")
        ax.set_title("Descriptive serial-founder persistence")
        save_figure(fig, figures_dir, "figure_4_serial_persistence_y1_5")

    # Concise report.
    report = [
        "# Founder-quality Phase-2 results — V3", "",
        "## Design", "",
        "- Main outcome is the direct firm-year sum in calendar years 1–5 after the first observed founder-spell entry year.",
        "- The entry calendar year is excluded because annual firm outcomes may contain pre-entry activity.",
        "- The event is an observed founder-spell entry, not a legal incorporation date.",
        "- `immig_first_deg_or_job_nonUS` is the project immigration proxy; it is not interpreted as registry-verified immigration status.",
        "- Founder-history models restrict to records with at least one observed pre-entry inventor-year. Missing history is not recoded as zero.",
        "- All estimates are descriptive/predictive, not causal founder effects.", "",
        "## Direct y1–y5 integrity", "",
        f"- Core y0–5-complete links inspected: `{flow['core_y0_5_complete_links']}`.",
        f"- Directly complete y1–5 links: `{flow['direct_y1_5_complete_links']}`.",
        f"- Stored/direct y1–5 patent mismatches: `{flow['patent_y1_5_mismatches']}`.",
        f"- Stored/direct y1–5 citation mismatches: `{flow['citation_y1_5_mismatches']}`.", "",
        "## Samples", "",
        f"- Full core y1–5 sample: `{flow['full_core_y1_5_links']}` links across `{flow['full_core_y1_5_firms']}` firms.",
        f"- Strict observed-start-date sample: `{flow['strict_observed_startdate_links']}` links.",
        f"- Strict observed-prehistory sample: `{flow['strict_observed_prehistory_links']}` links across `{flow['strict_observed_prehistory_firms']}` firms.", "",
        "## Reading the tables", "",
        "- `phase2_link_level_and_team_regressions.csv` contains WLS log1p and PPML estimates. Continuous coefficients are per within-sample standard deviation; binary indicators remain 0/1.",
        "- `phase2_all_regression_diagnostics.csv` reports rank, conditioning, and PPML convergence.",
        "- `team_level_earliest_entry_panel.csv` is a one-row-per-firm robustness analysis.",
        "- `serial_founder_persistence_y1_5.csv` is descriptive continuity across observed eligible ventures, not a causal founder effect.",
    ]
    (run_dir / "PHASE2_REPORT.md").write_text("\n".join(report) + "\n")
    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(), "validation_run": str(validation_run),
        "v3_final": str(V3_FINAL), "firm_year": str(V3_FIRM_YEAR), "flow": flow,
        "notes": ["Read-only V3 inputs", "No raw input overwritten", "y1--y5 direct verification passed before output"],
    }
    (run_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, default=str) + "\n")
    print(f"[INFO] wrote Phase-2 report: {run_dir / 'PHASE2_REPORT.md'}", flush=True)
    spark.stop()


if __name__ == "__main__":
    main()
