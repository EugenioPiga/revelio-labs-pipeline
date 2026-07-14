#!/usr/bin/env python3
"""
validate_founder_quality_v3.py

Read-only validation of the existing V3 founder outputs.  Writes only under:
  /home/epiga/revelio_labs/output/founder_quality_v1/validation_v3/run_*

This script does not rebuild, modify, or delete existing V3 founder outputs.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.storagelevel import StorageLevel


PROJECT = Path("/home/epiga/revelio_labs")
V3_ROOT = PROJECT / "output" / "founder_firm_outcomes_v3"
FINAL_PATH = str(V3_ROOT / "founder_firm_outcomes_v3")
SHARDS_PATH = str(V3_ROOT / "founder_firm_outcomes_shards")
FIRMYEAR_PATH = str(V3_ROOT / "firm_year_outcomes_current_rcid_v3")
CREATOR_PATH = PROJECT / "founder_restaurant_v3" / "founder_firm_outcomes_v3.py"

PRIMARY_IMMIGRATION = "immig_first_deg_or_job_nonUS"
LINK_KEY = ["user_id", "founded_rcid", "founder_position_id"]
USER_FIRM_KEY = ["user_id", "founded_rcid"]
HORIZONS = [3, 5, 10]


def json_default(value: Any) -> str:
    return str(value)


def write_json(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True, default=json_default) + "\n")


def require_columns(df: DataFrame, required: list[str], label: str) -> None:
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {missing}")


def write_csv(df: DataFrame, path: Path) -> None:
    # Each output is intentionally a new unique run directory; do not overwrite.
    df.coalesce(1).write.mode("errorifexists").option("header", True).csv(str(path))


def value_or_none(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "item"):
        return value.item()
    return value


def collect_metric_row(df: DataFrame) -> dict[str, Any]:
    row = df.first()
    if row is None:
        return {}
    return {k: value_or_none(v) for k, v in row.asDict(recursive=True).items()}


def safe_count_distinct(df: DataFrame, cols: list[str]) -> int:
    return df.select(*cols).dropDuplicates().count()


def missing_expr(column: str) -> F.Column:
    return F.col(column).isNull() | (F.trim(F.col(column).cast("string")) == F.lit(""))


def make_link_id(df: DataFrame) -> DataFrame:
    parts = [
        F.coalesce(F.col(c).cast("string"), F.lit("__NULL__"))
        for c in USER_FIRM_KEY + ["founder_position_id", "first_founder_entry_year"]
    ]
    return df.withColumn("analysis_link_id", F.sha2(F.concat_ws("||", *parts), 256))


def code_hash(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--out-root",
        default=str(PROJECT / "output" / "founder_quality_v1" / "validation_v3"),
    )
    parser.add_argument("--shuffle-partitions", type=int, default=320)
    args = parser.parse_args()

    run_id = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    run_dir = Path(args.out_root) / run_id
    run_dir.mkdir(parents=True, exist_ok=False)

    for path in [FINAL_PATH, SHARDS_PATH, FIRMYEAR_PATH]:
        if not Path(path).exists():
            raise FileNotFoundError(f"Required V3 input path does not exist: {path}")

    spark = (
        SparkSession.builder
        .appName("founder_quality_v3_validation")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] run_dir={run_dir}", flush=True)
    print(f"[INFO] final={FINAL_PATH}", flush=True)
    print(f"[INFO] shards={SHARDS_PATH}", flush=True)
    print(f"[INFO] firm_year={FIRMYEAR_PATH}", flush=True)

    merged = spark.read.parquet(FINAL_PATH)
    shards = spark.read.parquet(SHARDS_PATH)
    firm_year = spark.read.parquet(FIRMYEAR_PATH)

    merged_required = [
        "user_id", "founded_rcid", "founder_position_id",
        "first_founder_entry_date", "first_founder_entry_year",
        PRIMARY_IMMIGRATION,
        "firm_post_years_observed", "firm_first_post_year_observed",
        "firm_last_post_year_observed",
    ]
    for h in HORIZONS:
        merged_required += [
            f"firm_patents_y0_{h}",
            f"firm_us_citations_y0_{h}",
            f"firm_applications_y0_{h}",
        ]
    firm_year_required = [
        "firm_rcid", "year", "firm_patents", "firm_applications",
        "firm_first_inventor_patents", "firm_us_citations",
    ]

    require_columns(merged, merged_required, "Merged V3 founder output")
    require_columns(shards, merged_required, "V3 shard founder output")
    require_columns(firm_year, firm_year_required, "V3 firm-year output")

    write_json(
        run_dir / "schema_catalog.json",
        {
            "merged_founder_output": dict(merged.dtypes),
            "shard_founder_output": dict(shards.dtypes),
            "firm_year_output": dict(firm_year.dtypes),
        },
    )

    # Narrow founder panel once and persist; this is the only founder panel used below.
    founder_keep = list(dict.fromkeys(
        merged_required
        + [
            "core_founder_title_flag", "broad_founder_title_flag",
            "cofounder_title_flag", "founding_partner_title_flag",
            "founding_employee_title_flag", "founder_country",
            "founder_metro_area", "founder_naics3", "founder_naics6",
            "prior_patents", "prior_us_citations", "prior_years_observed",
            "prior_unique_current_rcids", "prior_unique_parent_rcids",
            "prior_unique_onet_codes", "prior_unique_naics6",
        ]
    ))
    founder_keep = [c for c in founder_keep if c in merged.columns]
    founders = merged.select(*founder_keep).persist(StorageLevel.MEMORY_AND_DISK)
    merged_rows = founders.count()

    shard_common_cols = [c for c in merged.columns if c in shards.columns]
    shard_common_cols = sorted(shard_common_cols)
    shard_rows = shards.count()
    merged_all_rows = merged.count()

    # Exact multiset comparison across the common schema, including duplicate rows.
    # This is deliberately strict and reads only the specific child datasets.
    shard_only_rows = (
        shards.select(*shard_common_cols)
        .exceptAll(merged.select(*shard_common_cols))
        .count()
    )
    merged_only_rows = (
        merged.select(*shard_common_cols)
        .exceptAll(shards.select(*shard_common_cols))
        .count()
    )

    key_stats = collect_metric_row(
        founders.agg(
            F.count("*").alias("founder_link_rows"),
            F.countDistinct("user_id").alias("unique_founders"),
            F.countDistinct("founded_rcid").alias("unique_founded_firms"),
            F.countDistinct(*USER_FIRM_KEY).alias("distinct_user_firm_pairs"),
            F.countDistinct(*LINK_KEY).alias("distinct_user_firm_position_pairs"),
            F.min("first_founder_entry_date").alias("min_founder_entry_date"),
            F.max("first_founder_entry_date").alias("max_founder_entry_date"),
            F.min("first_founder_entry_year").alias("min_founder_entry_year"),
            F.max("first_founder_entry_year").alias("max_founder_entry_year"),
        )
    )

    missing_fields = [
        "user_id", "founded_rcid", "founder_position_id",
        "first_founder_entry_date", "first_founder_entry_year",
        PRIMARY_IMMIGRATION,
        "prior_patents", "prior_us_citations", "prior_years_observed",
    ]
    missing_fields = [c for c in missing_fields if c in founders.columns]
    missing_profile = collect_metric_row(
        founders.agg(*[
            F.sum(F.when(missing_expr(c), F.lit(1)).otherwise(F.lit(0))).alias(f"missing_{c}")
            for c in missing_fields
        ])
    )

    duplicate_user_firm = (
        founders.groupBy(*USER_FIRM_KEY).count()
        .filter(F.col("count") > 1)
        .agg(
            F.count("*").alias("n_duplicate_user_firm_pairs"),
            F.sum("count").alias("rows_in_duplicate_user_firm_pairs"),
        )
    )
    duplicate_user_firm_position = (
        founders.groupBy(*LINK_KEY).count()
        .filter(F.col("count") > 1)
        .agg(
            F.count("*").alias("n_duplicate_user_firm_position_pairs"),
            F.sum("count").alias("rows_in_duplicate_user_firm_position_pairs"),
        )
    )

    team = (
        founders.filter(F.col("founded_rcid").isNotNull())
        .groupBy("founded_rcid")
        .agg(
            F.countDistinct("user_id").alias("n_distinct_founders"),
            F.count("*").alias("n_founder_link_rows"),
        )
    )
    team_distribution = (
        team.groupBy("n_distinct_founders")
        .agg(
            F.count("*").alias("n_firms"),
            F.sum("n_founder_link_rows").alias("n_founder_link_rows"),
        )
        .orderBy("n_distinct_founders")
    )
    write_csv(team_distribution, run_dir / "team_size_distribution")

    serial = (
        founders.filter(F.col("user_id").isNotNull() & F.col("founded_rcid").isNotNull())
        .groupBy("user_id")
        .agg(F.countDistinct("founded_rcid").alias("n_distinct_founded_firms"))
    )
    serial_distribution = (
        serial.groupBy("n_distinct_founded_firms")
        .agg(F.count("*").alias("n_founders"))
        .orderBy("n_distinct_founded_firms")
    )
    write_csv(serial_distribution, run_dir / "serial_founder_distribution")

    team_summary = collect_metric_row(
        team.agg(
            F.count("*").alias("n_firms_with_any_founder_link"),
            F.sum(F.when(F.col("n_distinct_founders") == 1, 1).otherwise(0)).alias("n_solo_founder_firms"),
            F.sum(F.when(F.col("n_distinct_founders") >= 2, 1).otherwise(0)).alias("n_multi_founder_firms"),
        )
    )
    serial_summary = collect_metric_row(
        serial.agg(
            F.count("*").alias("n_founders_with_any_firm"),
            F.sum(F.when(F.col("n_distinct_founded_firms") == 1, 1).otherwise(0)).alias("n_one_time_founders"),
            F.sum(F.when(F.col("n_distinct_founded_firms") >= 2, 1).otherwise(0)).alias("n_serial_founders"),
        )
    )

    # Distribution of founder-title flags and the formal immigration field.
    distribution_frames: list[DataFrame] = []
    distribution_cols = [
        "core_founder_title_flag", "broad_founder_title_flag",
        "cofounder_title_flag", "founding_partner_title_flag",
        "founding_employee_title_flag", PRIMARY_IMMIGRATION,
    ]
    for c in distribution_cols:
        if c in founders.columns:
            distribution_frames.append(
                founders.groupBy(
                    F.coalesce(F.col(c).cast("string"), F.lit("__MISSING__")).alias("value")
                )
                .agg(F.count("*").alias("n_rows"))
                .withColumn("variable", F.lit(c))
                .select("variable", "value", "n_rows")
            )
    if distribution_frames:
        dist = distribution_frames[0]
        for d in distribution_frames[1:]:
            dist = dist.unionByName(d)
        write_csv(dist.orderBy("variable", "value"), run_dir / "founder_flag_and_immigration_distribution")

    # Firm-year diagnostics: never impute absent firm-years as zeros.
    fy_global = collect_metric_row(
        firm_year.agg(
            F.count("*").alias("firm_year_rows"),
            F.countDistinct("firm_rcid").alias("unique_firms"),
            F.min("year").alias("min_year"),
            F.max("year").alias("max_year"),
            F.sum(F.when(F.col("firm_patents").isNull(), 1).otherwise(0)).alias("patent_null_rows"),
            F.sum(F.when(F.col("firm_patents") == 0, 1).otherwise(0)).alias("patent_zero_rows"),
            F.sum(F.when(F.col("firm_patents") < 0, 1).otherwise(0)).alias("patent_negative_rows"),
            F.sum(F.when(F.col("firm_us_citations").isNull(), 1).otherwise(0)).alias("citation_null_rows"),
            F.sum(F.when(F.col("firm_us_citations") == 0, 1).otherwise(0)).alias("citation_zero_rows"),
            F.sum(F.when(F.col("firm_us_citations") < 0, 1).otherwise(0)).alias("citation_negative_rows"),
        )
    )
    fy_duplicate_keys = collect_metric_row(
        firm_year.groupBy("firm_rcid", "year").count()
        .filter(F.col("count") > 1)
        .agg(
            F.count("*").alias("n_duplicate_firm_year_keys"),
            F.sum("count").alias("rows_in_duplicate_firm_year_keys"),
        )
    )

    # Use one earliest observed founder event per user-firm only for horizon validation.
    # We report duplicate links separately above; this does not silently discard them.
    w = Window.partitionBy(*USER_FIRM_KEY).orderBy(
        F.col("first_founder_entry_date").asc_nulls_last(),
        F.col("founder_position_id").asc_nulls_last(),
    )
    horizon_links = (
        founders
        .filter(
            F.col("user_id").isNotNull()
            & F.col("founded_rcid").isNotNull()
            & F.col("first_founder_entry_year").isNotNull()
        )
        .withColumn("_rn_user_firm", F.row_number().over(w))
        .filter(F.col("_rn_user_firm") == 1)
        .drop("_rn_user_firm")
    )
    horizon_links = make_link_id(horizon_links).persist(StorageLevel.MEMORY_AND_DISK)
    n_horizon_links = horizon_links.count()

    founder_firms = horizon_links.select(
        F.col("founded_rcid").alias("firm_rcid")
    ).dropDuplicates()

    fy_keep = [
        "firm_rcid", "year", "firm_patents", "firm_applications",
        "firm_first_inventor_patents", "firm_us_citations",
    ]
    fy_relevant = (
        firm_year.select(*fy_keep)
        .join(founder_firms, on="firm_rcid", how="left_semi")
        .persist(StorageLevel.MEMORY_AND_DISK)
    )
    fy_relevant_rows = fy_relevant.count()
    fy_relevant_summary = collect_metric_row(
        fy_relevant.agg(
            F.count("*").alias("relevant_firm_year_rows"),
            F.countDistinct("firm_rcid").alias("relevant_firms_with_any_firm_year_row"),
            F.min("year").alias("relevant_min_year"),
            F.max("year").alias("relevant_max_year"),
        )
    )

    max_year = fy_global.get("max_year")
    if max_year is None:
        raise RuntimeError("Firm-year output has no non-null year; cannot validate horizons.")
    max_year = int(max_year)

    # Equi-join first, then retain only years in the widest required post-event window.
    joined = (
        horizon_links.alias("l")
        .join(
            fy_relevant.alias("f"),
            F.col("l.founded_rcid") == F.col("f.firm_rcid"),
            how="left",
        )
        .filter(
            F.col("f.year").isNull()
            | (
                (F.col("f.year") >= F.col("l.first_founder_entry_year"))
                & (F.col("f.year") <= F.col("l.first_founder_entry_year") + F.lit(max(HORIZONS)))
            )
        )
    )

    group_cols = [
        F.col("l.analysis_link_id").alias("analysis_link_id"),
        F.col("l.user_id").alias("user_id"),
        F.col("l.founded_rcid").alias("founded_rcid"),
        F.col("l.founder_position_id").alias("founder_position_id"),
        F.col("l.first_founder_entry_date").alias("first_founder_entry_date"),
        F.col("l.first_founder_entry_year").alias("first_founder_entry_year"),
    ]
    for h in HORIZONS:
        group_cols.extend([
            F.col(f"l.firm_patents_y0_{h}").alias(f"stored_firm_patents_y0_{h}"),
            F.col(f"l.firm_us_citations_y0_{h}").alias(f"stored_firm_us_citations_y0_{h}"),
            F.col(f"l.firm_applications_y0_{h}").alias(f"stored_firm_applications_y0_{h}"),
        ])

    aggs: list[F.Column] = []
    for h in HORIZONS:
        condition = (
            (F.col("f.year") >= F.col("l.first_founder_entry_year"))
            & (F.col("f.year") <= F.col("l.first_founder_entry_year") + F.lit(h))
        )
        aggs.extend([
            F.countDistinct(F.when(condition, F.col("f.year"))).alias(f"observed_years_y0_{h}"),
            F.min(F.when(condition, F.col("f.year"))).alias(f"min_observed_year_y0_{h}"),
            F.max(F.when(condition, F.col("f.year"))).alias(f"max_observed_year_y0_{h}"),
            F.sum(F.when(condition, F.col("f.firm_patents"))).alias(f"direct_firm_patents_y0_{h}"),
            F.sum(F.when(condition, F.col("f.firm_us_citations"))).alias(f"direct_firm_us_citations_y0_{h}"),
            F.sum(F.when(condition, F.col("f.firm_applications"))).alias(f"direct_firm_applications_y0_{h}"),
            F.sum(
                F.when(condition & F.col("f.firm_patents").isNull(), 1).otherwise(0)
            ).alias(f"null_patent_rows_y0_{h}"),
            F.sum(
                F.when(condition & F.col("f.firm_us_citations").isNull(), 1).otherwise(0)
            ).alias(f"null_citation_rows_y0_{h}"),
        ])

    horizon_panel = joined.groupBy(*group_cols).agg(*aggs)
    for h in HORIZONS:
        complete_calendar = (
            (F.col(f"observed_years_y0_{h}") == F.lit(h + 1))
            & (F.col(f"min_observed_year_y0_{h}") == F.col("first_founder_entry_year"))
            & (F.col(f"max_observed_year_y0_{h}") == F.col("first_founder_entry_year") + F.lit(h))
        )
        mature = F.col("first_founder_entry_year") <= F.lit(max_year - h)
        horizon_panel = (
            horizon_panel
            .withColumn(f"mature_by_data_cutoff_y0_{h}", mature.cast("int"))
            .withColumn(f"complete_calendar_y0_{h}", complete_calendar.cast("int"))
            .withColumn(
                f"eligible_patents_y0_{h}",
                (
                    mature
                    & complete_calendar
                    & (F.coalesce(F.col(f"null_patent_rows_y0_{h}"), F.lit(0)) == 0)
                ).cast("int"),
            )
            .withColumn(
                f"eligible_citations_y0_{h}",
                (
                    mature
                    & complete_calendar
                    & (F.coalesce(F.col(f"null_citation_rows_y0_{h}"), F.lit(0)) == 0)
                ).cast("int"),
            )
            .withColumn(
                f"patent_sum_mismatch_y0_{h}",
                (
                    F.col(f"observed_years_y0_{h}") > 0
                )
                & (
                    F.abs(
                        F.coalesce(F.col(f"stored_firm_patents_y0_{h}").cast("double"), F.lit(0.0))
                        - F.coalesce(F.col(f"direct_firm_patents_y0_{h}").cast("double"), F.lit(0.0))
                    ) > F.lit(1e-9)
                )
            )
            .withColumn(
                f"citation_sum_mismatch_y0_{h}",
                (
                    F.col(f"observed_years_y0_{h}") > 0
                )
                & (
                    F.abs(
                        F.coalesce(F.col(f"stored_firm_us_citations_y0_{h}").cast("double"), F.lit(0.0))
                        - F.coalesce(F.col(f"direct_firm_us_citations_y0_{h}").cast("double"), F.lit(0.0))
                    ) > F.lit(1e-9)
                )
            )
        )

    horizon_panel.persist(StorageLevel.MEMORY_AND_DISK)
    # Save the precise eligibility panel to avoid rerunning this join in Phase 1.
    horizon_panel.write.mode("errorifexists").parquet(
        str(run_dir / "candidate_link_horizon_eligibility")
    )

    horizon_rows = []
    for h in HORIZONS:
        metrics = collect_metric_row(
            horizon_panel.agg(
                F.count("*").alias("deduplicated_user_firm_links"),
                F.sum(F.col(f"mature_by_data_cutoff_y0_{h}")).alias("mature_by_data_cutoff"),
                F.sum(F.col(f"complete_calendar_y0_{h}")).alias("complete_calendar"),
                F.sum(F.col(f"eligible_patents_y0_{h}")).alias("eligible_patent_horizon"),
                F.sum(F.col(f"eligible_citations_y0_{h}")).alias("eligible_citation_horizon"),
                F.sum(F.when(F.col(f"observed_years_y0_{h}") == 0, 1).otherwise(0)).alias("no_observed_firm_years"),
                F.sum(F.when(F.col(f"patent_sum_mismatch_y0_{h}"), 1).otherwise(0)).alias("stored_vs_direct_patent_mismatch"),
                F.sum(F.when(F.col(f"citation_sum_mismatch_y0_{h}"), 1).otherwise(0)).alias("stored_vs_direct_citation_mismatch"),
                F.sum(F.when(F.col(f"stored_firm_patents_y0_{h}").isNull(), 1).otherwise(0)).alias("stored_patent_outcome_missing"),
                F.sum(F.when(F.col(f"stored_firm_us_citations_y0_{h}").isNull(), 1).otherwise(0)).alias("stored_citation_outcome_missing"),
            )
        )
        metrics["horizon"] = f"y0_{h}"
        horizon_rows.append(metrics)

    horizon_summary = spark.createDataFrame(horizon_rows)
    write_csv(horizon_summary.orderBy("horizon"), run_dir / "horizon_eligibility_summary")

    # Conservative primary candidate for Phase 1: core title + complete 0-5 patent horizon.
    # This is a proposed sample marker, not a final paper specification.
    sample_marker = horizon_panel
    if "core_founder_title_flag" in founders.columns:
        sample_marker = (
            horizon_panel.alias("h")
            .join(
                founders.select(
                    "user_id", "founded_rcid", "founder_position_id",
                    F.col("core_founder_title_flag").alias("core_founder_title_flag"),
                ).dropDuplicates(LINK_KEY).alias("m"),
                on=(
                    (F.col("h.user_id") == F.col("m.user_id"))
                    & (F.col("h.founded_rcid") == F.col("m.founded_rcid"))
                    & F.col("h.founder_position_id").eqNullSafe(F.col("m.founder_position_id"))
                ),
                how="left",
            )
            .select("h.*", "m.core_founder_title_flag")
        )
    sample_marker = sample_marker.withColumn(
        "proposed_primary_phase1_sample_y0_5",
        (
            (F.coalesce(F.col("core_founder_title_flag").cast("int"), F.lit(0)) == 1)
            & (F.col("eligible_patents_y0_5") == 1)
        ).cast("int")
    )
    sample_marker.write.mode("errorifexists").parquet(
        str(run_dir / "candidate_phase1_sample_markers")
    )
    proposed_sample_summary = collect_metric_row(
        sample_marker.agg(
            F.count("*").alias("deduplicated_user_firm_links"),
            F.sum("proposed_primary_phase1_sample_y0_5").alias("proposed_core_complete_y0_5_links"),
        )
    )

    validation = {
        "run_id": run_id,
        "inputs": {
            "merged_founder_output": FINAL_PATH,
            "shard_founder_output": SHARDS_PATH,
            "firm_year_output": FIRMYEAR_PATH,
            "creator_script": str(CREATOR_PATH),
            "creator_script_sha256": code_hash(CREATOR_PATH),
        },
        "output_integrity": {
            "merged_rows_all_columns": merged_all_rows,
            "shard_rows_all_columns": shard_rows,
            "common_schema_columns_compared": len(shard_common_cols),
            "rows_in_shards_not_in_merged_exact_multiset": shard_only_rows,
            "rows_in_merged_not_in_shards_exact_multiset": merged_only_rows,
        },
        "founder_link_structure": {
            **key_stats,
            **missing_profile,
            "duplicate_user_firm": collect_metric_row(duplicate_user_firm),
            "duplicate_user_firm_position": collect_metric_row(duplicate_user_firm_position),
            "team_summary": team_summary,
            "serial_founder_summary": serial_summary,
        },
        "firm_year_structure": {
            **fy_global,
            "duplicate_firm_year_keys": fy_duplicate_keys,
            "relevant_firm_year": fy_relevant_summary,
        },
        "horizon_validation": {
            "max_firm_year": max_year,
            "deduplicated_user_firm_links_with_nonmissing_entry_year": n_horizon_links,
            "horizons": horizon_rows,
        },
        "proposed_sample_marker": proposed_sample_summary,
        "interpretation_rules": {
            "event_time": (
                "The creator code defines founder entry as coalesce(founder spell startdate, enddate). "
                "This is a first observed founder-spell entry date, not verified legal incorporation."
            ),
            "pre_founding_timing": (
                "Existing prior_* fields use annual inventor-year observations with year < first_founder_entry_year. "
                "They are conservative against post-event contamination but exclude all same-calendar-year information."
            ),
            "immigration": (
                f"Use {PRIMARY_IMMIGRATION} as the formal project immigration variable. "
                "Do not use founder_immigrant_any as a main measure or silently substitute foreign education/work proxies."
            ),
            "zero_rule": (
                "Missing firm-years are not recoded to zero. Main fixed-horizon outcomes require observed complete "
                "calendar-year coverage and nonmissing outcome fields."
            ),
        },
    }
    write_json(run_dir / "validation_metrics.json", validation)

    integrity_pass = (
        merged_all_rows == shard_rows
        and shard_only_rows == 0
        and merged_only_rows == 0
        and (fy_duplicate_keys.get("n_duplicate_firm_year_keys") or 0) == 0
    )

    report = [
        "# V3 founder-output validation",
        "",
        f"Run directory: `{run_dir}`",
        "",
        "## Status",
        "",
        f"- Merge-versus-shards integrity: **{'PASS' if integrity_pass else 'ATTENTION REQUIRED'}**.",
        f"- Merged rows: `{merged_all_rows}`; shard rows: `{shard_rows}`.",
        f"- Exact multiset residual rows: shards-only `{shard_only_rows}`, merged-only `{merged_only_rows}`.",
        f"- Firm-year duplicate keys: `{fy_duplicate_keys.get('n_duplicate_firm_year_keys')}`.",
        "",
        "## Verified construction rules",
        "",
        "- Founder event time is the creator script's `coalesce(startdate, enddate)` for the selected founder spell.",
        "- Treat `first_founder_entry_date` as a **first observed founder-spell entry date**, not legal incorporation or a registry-verified firm birth.",
        "- Existing `prior_*` measures use inventor-year rows strictly before `first_founder_entry_year`; this avoids post-event contamination at annual frequency but omits the entire entry year.",
        f"- The formal immigration analysis variable is `{PRIMARY_IMMIGRATION}`. `founder_immigrant_any` is excluded from the main analysis.",
        "- A missing firm-year is not a zero. The candidate main sample uses explicit complete-horizon eligibility flags.",
        "",
        "## Key diagnostics",
        "",
        f"- Founder link rows: `{key_stats.get('founder_link_rows')}`.",
        f"- Unique founders: `{key_stats.get('unique_founders')}`.",
        f"- Unique founded firms: `{key_stats.get('unique_founded_firms')}`.",
        f"- Serial founders (2+ distinct founded firms): `{serial_summary.get('n_serial_founders')}`.",
        f"- Multi-founder firms (2+ distinct founders): `{team_summary.get('n_multi_founder_firms')}`.",
        f"- Firm-year data range: `{fy_global.get('min_year')}` to `{fy_global.get('max_year')}`.",
        "",
        "## Horizon eligibility",
        "",
        "| Horizon | Mature by data cutoff | Complete observed calendar | Eligible patents | Eligible citations | Patent stored/direct mismatches |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for m in horizon_rows:
        report.append(
            f"| {m['horizon']} | {m.get('mature_by_data_cutoff')} | "
            f"{m.get('complete_calendar')} | {m.get('eligible_patent_horizon')} | "
            f"{m.get('eligible_citation_horizon')} | {m.get('stored_vs_direct_patent_mismatch')} |"
        )
    report += [
        "",
        "## Next analytical gate",
        "",
        "Use the validated `candidate_link_horizon_eligibility` panel and `candidate_phase1_sample_markers` only after reviewing the metrics above. "
        "The Phase-1 design will set the primary sample as core founder links with complete fixed-horizon coverage, keep broader founder titles as robustness, "
        "and use one founder-firm link per user-firm with firm-level clustering and an alternative team-level aggregation.",
        "",
    ]
    (run_dir / "VALIDATION_REPORT.md").write_text("\n".join(report))

    print(f"[INFO] wrote validation report: {run_dir / 'VALIDATION_REPORT.md'}", flush=True)
    print("[INFO] validation completed", flush=True)

    horizon_panel.unpersist()
    horizon_links.unpersist()
    fy_relevant.unpersist()
    founders.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
