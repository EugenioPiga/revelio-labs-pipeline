#!/usr/bin/env python3
"""
Detailed immigrant descriptives for the corrected V3 Revelio inventor panel.

Main analysis restrictions
--------------------------
1. U.S. person-years only:
   current_position_observed == 1 and current_country is the United States.
2. U.S.-educated sample:
   the person's last observed education country is the United States.
3. Canonical immigrant definition:
   immig_any_edu_or_first_job_nonUS == 1, meaning any observed education abroad
   OR the first observed raw job abroad.
4. Founder sample:
   ever has a raw position title containing founder, co-founder/cofounder, or
   founding partner. Owner-only titles are reported separately but are not
   included in the founder sample, matching the established inventor-founder
   definition.

The script produces detailed CSV tables, compact Parquet intermediates, figures,
a manifest, and a data dictionary for both:
- all inventors satisfying the analysis restrictions; and
- the founder subset.

Country of origin is the corrected LinkedIn-history proxy in the V3 panel. It is
not citizenship or birthplace.
"""

from __future__ import annotations

import argparse
import glob
import json
import math
import os
import re
import shutil
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.types import NumericType


US_KEYS = {
    "united states",
    "united states of america",
    "usa",
    "u s a",
    "us",
    "u s",
}

FOUNDER_PATTERN = r"(?i)(^|[^a-z])(co[\s-]?founder|founder|founding\s+partner)([^a-z]|$)"
OWNER_PATTERN = r"(?i)(^|[^a-z])(co[\s-]?owner|owner[\s-]?operator|owner|proprietor|franchisee)([^a-z]|$)"
OWNER_FALSE_POSITIVE_PATTERN = (
    r"(?i)(product|process|platform|application|system|data|service|technical|"
    r"solution|feature|risk|control|project|program|domain|content)\s+owner"
)


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--input-panel",
        default=(
            "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
            "inventor_year_merged_colleague_origin_any_education"
        ),
    )
    ap.add_argument(
        "--positions-dir",
        default=(
            "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
            "step2_inventors_matched_positions"
        ),
        help="Canonical raw-position dataset used to identify founders from all spells.",
    )
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--year-start", type=int, default=2000)
    ap.add_argument("--year-end", type=int, default=2024)
    ap.add_argument(
        "--citation-end-year",
        type=int,
        default=2019,
        help="Last year used for citation-mature summaries.",
    )
    ap.add_argument("--tenure-max", type=int, default=50)
    ap.add_argument("--tenure-bin-width", type=int, default=5)
    ap.add_argument("--min-cluster", type=int, default=10)
    ap.add_argument("--decomp-min-cluster", type=int, default=50)
    ap.add_argument("--top-origins", type=int, default=20)
    ap.add_argument("--top-categories", type=int, default=20)
    ap.add_argument("--founder-event-pre", type=int, default=5)
    ap.add_argument("--founder-event-post", type=int, default=10)
    ap.add_argument(
        "--threads",
        type=int,
        default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")),
    )
    ap.add_argument("--shuffle-partitions", type=int, default=3000)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument("--overwrite", action="store_true")
    ap.add_argument("--skip-plots", action="store_true")
    ap.add_argument(
        "--write-cluster-panel",
        action="store_true",
        help="Also retain the compact cluster-year metrics as partitioned Parquet.",
    )
    return ap.parse_args()


def msg(text: str) -> None:
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now}] {text}", flush=True)


def clean_key_expr(column: F.Column) -> F.Column:
    x = F.lower(F.trim(column.cast("string")))
    x = F.regexp_replace(x, r"&", " and ")
    x = F.regexp_replace(x, r"[^a-z0-9]+", " ")
    x = F.regexp_replace(x, r"\s+", " ")
    return F.when(
        column.isNull()
        | (F.trim(column.cast("string")) == "")
        | F.lower(F.trim(column.cast("string"))).isin("empty", "null", "none", "na"),
        F.lit(None).cast("string"),
    ).otherwise(F.trim(x))


def is_us(column: F.Column) -> F.Column:
    return clean_key_expr(column).isin(*sorted(US_KEYS))


def nonempty(column: F.Column) -> F.Column:
    return (
        column.isNotNull()
        & (F.trim(column.cast("string")) != "")
        & (~F.lower(F.trim(column.cast("string"))).isin("empty", "null", "none", "na"))
    )


def first_existing(columns: Iterable[str], candidates: Sequence[str]) -> Optional[str]:
    available = set(columns)
    return next((c for c in candidates if c in available), None)


def require_columns(df: DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label} is missing required columns: {missing}")


def assert_unique(df: DataFrame, keys: Sequence[str], label: str) -> None:
    bad = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(5)
    if bad.count() > 0:
        bad.show(truncate=False)
        raise RuntimeError(f"Duplicate keys in {label}: {keys}")


def safe_remove(path: str) -> None:
    if os.path.islink(path) or os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


def write_single_csv(df: DataFrame, output_file: str, order_by: Optional[Sequence[str]] = None) -> None:
    """Write a small/medium Spark table as one ordinary CSV file."""
    output_file = os.path.abspath(output_file)
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    tmp = output_file + "._spark_tmp"
    if os.path.exists(tmp):
        safe_remove(tmp)
    if os.path.exists(output_file):
        os.unlink(output_file)

    out = df
    if order_by:
        out = out.orderBy(*order_by)
    (
        out.coalesce(1)
        .write.mode("overwrite")
        .option("header", "true")
        .option("emptyValue", "")
        .csv(tmp)
    )
    parts = glob.glob(os.path.join(tmp, "part-*.csv"))
    if len(parts) != 1:
        raise RuntimeError(f"Expected one CSV part for {output_file}, found {len(parts)}")
    os.rename(parts[0], output_file)
    safe_remove(tmp)
    msg(f"Wrote {output_file}")


def add_overall_status(df: DataFrame, status_col: str = "immigration_status") -> DataFrame:
    return df.unionByName(df.withColumn(status_col, F.lit("all")))


def numeric_or_zero(df: DataFrame, column: str) -> F.Column:
    if column in df.columns:
        return F.coalesce(F.col(column).cast("double"), F.lit(0.0))
    return F.lit(0.0)


def build_founder_flags(spark: SparkSession, positions_dir: str) -> DataFrame:
    msg(f"Reading raw positions for founder identification: {positions_dir}")
    pos_raw = (
        spark.read.option("recursiveFileLookup", "true")
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(positions_dir)
    )
    require_columns(pos_raw, ["user_id"], "Positions input")

    title_candidates = [
        c
        for c in [
            "title_raw",
            "title_translated",
            "mapped_role_v3",
            "job_category_v2",
            "role_k150_v2",
        ]
        if c in pos_raw.columns
    ]
    if not title_candidates:
        raise RuntimeError("No title or role columns were found in the positions input.")

    date_col = first_existing(pos_raw.columns, ["startdate", "start_date", "position_startdate"])
    end_col = first_existing(pos_raw.columns, ["enddate", "end_date", "position_enddate"])
    position_id_col = first_existing(pos_raw.columns, ["position_id", "id", "position_number"])

    text_parts = [F.coalesce(F.col(c).cast("string"), F.lit("")) for c in title_candidates]
    title_text = F.trim(F.regexp_replace(F.concat_ws(" | ", *text_parts), r"\s+", " "))

    pos = pos_raw.select(
        F.col("user_id").cast("string").alias("user_id"),
        title_text.alias("founder_title_text"),
        (F.to_date(F.col(date_col)) if date_col else F.lit(None).cast("date")).alias("position_startdate"),
        (F.to_date(F.col(end_col)) if end_col else F.lit(None).cast("date")).alias("position_enddate"),
        (
            F.col(position_id_col).cast("string")
            if position_id_col
            else F.lit(None).cast("string")
        ).alias("position_id_key"),
    ).filter(F.col("user_id").isNotNull())

    founder_hit = F.col("founder_title_text").rlike(FOUNDER_PATTERN)
    owner_hit = F.col("founder_title_text").rlike(OWNER_PATTERN) & (~F.col("founder_title_text").rlike(OWNER_FALSE_POSITIVE_PATTERN))

    flags = pos.groupBy("user_id").agg(
        F.max(founder_hit.cast("int")).alias("ever_founder"),
        F.max(owner_hit.cast("int")).alias("ever_owner_title"),
        F.max((founder_hit & owner_hit).cast("int")).alias("ever_founder_owner_title"),
    )

    first_founder = (
        pos.filter(founder_hit)
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.col("position_startdate").asc_nulls_last(),
                    F.col("position_enddate").asc_nulls_last(),
                    F.col("position_id_key").asc_nulls_last(),
                    F.col("founder_title_text").asc(),
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select(
            "user_id",
            F.col("position_startdate").alias("founder_entry_date"),
            F.year("position_startdate").cast("int").alias("founder_entry_year"),
            F.col("founder_title_text").alias("founder_entry_title"),
        )
    )

    result = (
        flags.join(first_founder, "user_id", "left")
        .fillna({"ever_founder": 0, "ever_owner_title": 0, "ever_founder_owner_title": 0})
    )
    assert_unique(result, ["user_id"], "founder flags")
    return result


def choose_panel_columns(panel: DataFrame) -> Tuple[List[str], List[str]]:
    required = [
        "user_id",
        "year",
        "current_position_observed",
        "current_country",
        "last_university_country",
        "immig_any_edu_or_first_job_nonUS",
        "immig_education_any_nonUS",
        "immig_job_first_nonUS",
        "country_of_origin",
        "country_of_origin_key",
        "country_of_origin_source",
        "country_of_origin_nonUS",
        "n_patents",
        "n_us_citations",
    ]
    require_columns(panel, required, "Corrected inventor-year panel")

    optional_candidates = [
        "first_observed_education_country",
        "first_nonUS_education_country",
        "first_observed_job_country",
        "first_observed_education_startdate",
        "first_nonUS_education_startdate",
        "first_observed_job_startdate",
        "first_startdate_edu",
        "first_enddate_edu",
        "first_startdate_pos",
        "last_degree",
        "edu_last_degree",
        "first_degree",
        "edu_first_degree",
        "has_education",
        "n_educations",
        "current_state",
        "current_city",
        "current_metro_area",
        "current_rcid",
        "current_parent_rcid",
        "current_company_name",
        "current_parent_company_name",
        "current_title_raw",
        "current_title_translated",
        "current_mapped_role_v3",
        "current_onet_code",
        "current_onet_title",
        "current_onet_major",
        "current_naics2",
        "current_naics3",
        "current_naics4",
        "current_naics6",
        "current_naics_description",
        "current_salary",
        "current_total_compensation",
        "current_seniority",
        "career_modal_section",
        "career_modal_class",
        "modal_section_y",
        "modal_class_y",
        "au_sex_predicted",
        "au_ethnicity_predicted",
        "immig1_post",
        "immig1_ever",
        "immig2_post",
        "immig2_ever",
    ]

    optional = [c for c in optional_candidates if c in panel.columns]

    step6_metrics: List[str] = []
    for field in panel.schema.fields:
        name = field.name
        lname = name.lower()
        if (
            isinstance(field.dataType, NumericType)
            and any(token in lname for token in ["colleague", "collab", "citation"])
            and name not in {"n_us_citations", "year"}
        ):
            step6_metrics.append(name)

    # Keep this diagnostic bundle bounded and deterministic.
    step6_metrics = sorted(step6_metrics)[:120]
    selected = list(dict.fromkeys(required + optional + step6_metrics))
    return selected, step6_metrics


def add_derived_variables(df: DataFrame, args: argparse.Namespace) -> DataFrame:
    first_edu_date_col = first_existing(
        df.columns,
        ["first_startdate_edu", "first_observed_education_startdate"],
    )
    first_pos_date_col = first_existing(
        df.columns,
        ["first_startdate_pos", "first_observed_job_startdate"],
    )

    edu_year = F.year(F.to_date(F.col(first_edu_date_col))) if first_edu_date_col else F.lit(None).cast("int")
    pos_year = F.year(F.to_date(F.col(first_pos_date_col))) if first_pos_date_col else F.lit(None).cast("int")

    tenure_from_edu = F.col("year") - edu_year + F.lit(3)
    tenure_from_pos = F.col("year") - pos_year
    tenure = F.when(
        tenure_from_edu.isNull() | (tenure_from_edu > args.tenure_max),
        tenure_from_pos,
    ).otherwise(tenure_from_edu)
    tenure = F.when((tenure < 0) | (tenure > args.tenure_max), F.lit(None)).otherwise(tenure)

    period_start = (
        F.floor((F.col("year") - F.lit(args.year_start)) / F.lit(5)) * F.lit(5)
        + F.lit(args.year_start)
    ).cast("int")
    period_end = F.least(period_start + F.lit(4), F.lit(args.year_end)).cast("int")

    component = (
        F.when(
            (F.col("immig_education_any_nonUS") == 1)
            & (F.col("immig_job_first_nonUS") == 1),
            F.lit("both_education_and_first_job_abroad"),
        )
        .when(
            (F.col("immig_education_any_nonUS") == 1)
            & (F.col("immig_job_first_nonUS") == 0),
            F.lit("education_abroad_only"),
        )
        .when(
            (F.col("immig_education_any_nonUS") == 0)
            & (F.col("immig_job_first_nonUS") == 1),
            F.lit("first_job_abroad_only"),
        )
        .otherwise(F.lit("neither_observed_signal"))
    )

    result = (
        df.withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("year", F.col("year").cast("int"))
        .withColumn(
            "immigrant",
            F.coalesce(F.col("immig_any_edu_or_first_job_nonUS").cast("int"), F.lit(0)),
        )
        .withColumn(
            "immigration_status",
            F.when(F.col("immigrant") == 1, F.lit("immigrant")).otherwise(F.lit("nonimmigrant")),
        )
        .withColumn("immigration_component", component)
        .withColumn("country_of_origin_clean", F.trim(F.col("country_of_origin").cast("string")))
        .withColumn("country_of_origin_key_clean", clean_key_expr(F.col("country_of_origin_key")))
        .withColumn("n_patents", F.coalesce(F.col("n_patents").cast("double"), F.lit(0.0)))
        .withColumn("n_us_citations", F.coalesce(F.col("n_us_citations").cast("double"), F.lit(0.0)))
        .withColumn(
            "citations_per_patent",
            F.when(F.col("n_patents") > 0, F.col("n_us_citations") / F.col("n_patents")),
        )
        .withColumn("log1p_patents", F.log1p(F.col("n_patents")))
        .withColumn("log1p_citations", F.log1p(F.col("n_us_citations")))
        .withColumn("tenure", tenure.cast("int"))
        .withColumn(
            "tenure_bin_start",
            (
                F.floor(F.col("tenure") / F.lit(args.tenure_bin_width))
                * F.lit(args.tenure_bin_width)
            ).cast("int"),
        )
        .withColumn(
            "tenure_bin",
            F.when(
                F.col("tenure").isNotNull(),
                F.concat(
                    F.col("tenure_bin_start").cast("string"),
                    F.lit("-"),
                    (F.col("tenure_bin_start") + F.lit(args.tenure_bin_width - 1)).cast("string"),
                ),
            ),
        )
        .withColumn("period_start", period_start)
        .withColumn("period_end", period_end)
        .withColumn(
            "period",
            F.concat(F.col("period_start").cast("string"), F.lit("-"), F.col("period_end").cast("string")),
        )
        .withColumn("citation_mature_year", (F.col("year") <= args.citation_end_year).cast("int"))
    )

    parent_col = first_existing(result.columns, ["current_parent_rcid", "current_rcid"])
    metro_col = first_existing(result.columns, ["current_metro_area"])
    state_col = first_existing(result.columns, ["current_state"])

    result = result.withColumn(
        "cluster_parent",
        F.col(parent_col).cast("string") if parent_col else F.lit(None).cast("string"),
    )
    result = result.withColumn(
        "cluster_metro",
        clean_key_expr(F.col(metro_col)) if metro_col else F.lit(None).cast("string"),
    )
    result = result.withColumn(
        "cluster_state",
        clean_key_expr(F.col(state_col)) if state_col else F.lit(None).cast("string"),
    )
    result = result.withColumn(
        "cluster_metro_parent",
        F.when(
            nonempty(F.col("cluster_parent")) & nonempty(F.col("cluster_metro")),
            F.concat_ws("||", F.col("cluster_parent"), F.col("cluster_metro")),
        ),
    )
    return result


def stage_count(df: DataFrame, stage: str) -> DataFrame:
    return df.agg(
        F.lit(stage).alias("stage"),
        F.count(F.lit(1)).alias("person_years"),
        F.countDistinct("user_id").alias("people"),
    )


def create_sample_flow(panel_years: DataFrame, founder_flags: DataFrame) -> DataFrame:
    observed = panel_years.filter(F.col("current_position_observed") == 1)
    us_years = observed.filter(is_us(F.col("current_country")))
    last_edu_observed = us_years.filter(nonempty(F.col("last_university_country")))
    last_edu_us = last_edu_observed.filter(is_us(F.col("last_university_country")))
    with_founders = last_edu_us.join(founder_flags.select("user_id", "ever_founder"), "user_id", "left").fillna({"ever_founder": 0})

    return (
        stage_count(panel_years, "01_panel_rows_in_year_window")
        .unionByName(stage_count(observed, "02_current_position_observed"))
        .unionByName(stage_count(us_years, "03_current_US_person_years"))
        .unionByName(stage_count(last_edu_observed, "04_last_education_country_observed"))
        .unionByName(stage_count(last_edu_us, "05_last_education_US"))
        .unionByName(stage_count(with_founders.filter(F.col("ever_founder") == 1), "06_founders_in_final_sample"))
    )


def duplicate_samples(base: DataFrame) -> DataFrame:
    all_rows = base.withColumn("sample", F.lit("all_inventors"))
    founder_rows = base.filter(F.col("ever_founder") == 1).withColumn("sample", F.lit("founders"))
    return all_rows.unionByName(founder_rows)


def aggregate_yearly(analysis: DataFrame) -> DataFrame:
    expanded = add_overall_status(analysis)
    return expanded.groupBy("sample", "year", "immigration_status").agg(
        F.count(F.lit(1)).alias("person_years"),
        F.countDistinct("user_id").alias("people"),
        F.sum((F.col("n_patents") > 0).cast("long")).alias("patenting_person_years"),
        F.sum("n_patents").alias("patents"),
        F.avg("n_patents").alias("mean_patents_per_person_year"),
        F.expr("percentile_approx(n_patents, 0.5, 10000)").alias("median_patents_per_person_year"),
        F.sum("n_us_citations").alias("us_citations"),
        F.avg("n_us_citations").alias("mean_us_citations_per_person_year"),
        F.avg("citations_per_patent").alias("mean_citations_per_patenting_year"),
        F.avg("log1p_citations").alias("mean_log1p_citations"),
        F.avg(F.col("top10_patenting").cast("double")).alias("share_top10_patenting"),
    )


def aggregate_person_summary(analysis: DataFrame) -> DataFrame:
    person = analysis.groupBy("sample", "user_id", "immigration_status").agg(
        F.min("year").alias("first_US_year_in_window"),
        F.max("year").alias("last_US_year_in_window"),
        F.countDistinct("year").alias("US_years_observed"),
        F.sum("n_patents").alias("total_patents_in_window"),
        F.sum("n_us_citations").alias("total_us_citations_in_window"),
        F.max("top10_patenting").alias("top10_patenting"),
        F.max("ever_founder").alias("ever_founder"),
        F.first("country_of_origin_clean", ignorenulls=True).alias("country_of_origin"),
    )
    expanded = add_overall_status(person)
    return expanded.groupBy("sample", "immigration_status").agg(
        F.countDistinct("user_id").alias("people"),
        F.avg("US_years_observed").alias("mean_US_years_observed"),
        F.expr("percentile_approx(US_years_observed, 0.5, 10000)").alias("median_US_years_observed"),
        F.avg("total_patents_in_window").alias("mean_total_patents"),
        F.expr("percentile_approx(total_patents_in_window, 0.5, 10000)").alias("median_total_patents"),
        F.avg("total_us_citations_in_window").alias("mean_total_us_citations"),
        F.expr("percentile_approx(total_us_citations_in_window, 0.5, 10000)").alias("median_total_us_citations"),
        F.avg(F.col("top10_patenting").cast("double")).alias("share_top10_patenting"),
        F.avg(F.col("ever_founder").cast("double")).alias("share_ever_founder"),
    )


def categorical_distribution(
    analysis: DataFrame,
    column: str,
    label: str,
    top_n: int,
) -> Optional[DataFrame]:
    if column not in analysis.columns:
        return None
    cleaned = analysis.withColumn("category", F.trim(F.col(column).cast("string"))).filter(nonempty(F.col("category")))
    top = [
        r["category"]
        for r in (
            cleaned.filter(F.col("sample") == "all_inventors")
            .groupBy("category")
            .agg(F.countDistinct("user_id").alias("people"))
            .orderBy(F.col("people").desc(), F.col("category").asc())
            .limit(top_n)
            .collect()
        )
    ]
    if not top:
        return None
    d = cleaned.withColumn(
        "category_grouped",
        F.when(F.col("category").isin(*top), F.col("category")).otherwise(F.lit("Other")),
    )
    d = add_overall_status(d)
    return d.groupBy("sample", "immigration_status", "category_grouped").agg(
        F.lit(label).alias("dimension"),
        F.countDistinct("user_id").alias("people"),
        F.count(F.lit(1)).alias("person_years"),
        F.avg("n_patents").alias("mean_patents"),
        F.avg("n_us_citations").alias("mean_us_citations"),
    )


def build_origin_tables(analysis: DataFrame, args: argparse.Namespace) -> Tuple[DataFrame, DataFrame, DataFrame, List[str]]:
    person = (
        analysis.select(
            "sample",
            "user_id",
            "immigration_status",
            "country_of_origin_clean",
            "country_of_origin_source",
            "country_of_origin_nonUS",
        )
        .dropDuplicates(["sample", "user_id"])
    )
    immigrants = person.filter(F.col("immigration_status") == "immigrant").filter(nonempty(F.col("country_of_origin_clean")))
    top_origins = [
        r["country_of_origin_clean"]
        for r in (
            immigrants.filter(F.col("sample") == "all_inventors")
            .groupBy("country_of_origin_clean")
            .agg(F.countDistinct("user_id").alias("people"))
            .orderBy(F.col("people").desc(), F.col("country_of_origin_clean").asc())
            .limit(args.top_origins)
            .collect()
        )
    ]

    origin_distribution = immigrants.groupBy("sample", "country_of_origin_clean").agg(
        F.countDistinct("user_id").alias("people")
    )
    origin_distribution = origin_distribution.withColumn(
        "share_within_sample_immigrants",
        F.col("people") / F.sum("people").over(Window.partitionBy("sample")),
    )

    origin_source = person.groupBy("sample", "immigration_status", "country_of_origin_source").agg(
        F.countDistinct("user_id").alias("people")
    )

    origin_year = (
        analysis.filter(F.col("immigration_status") == "immigrant")
        .filter(nonempty(F.col("country_of_origin_clean")))
        .withColumn(
            "origin_grouped",
            F.when(F.col("country_of_origin_clean").isin(*top_origins), F.col("country_of_origin_clean")).otherwise(F.lit("Other")),
        )
        .groupBy("sample", "year", "origin_grouped")
        .agg(
            F.countDistinct("user_id").alias("people"),
            F.avg("n_patents").alias("mean_patents"),
            F.avg("n_us_citations").alias("mean_us_citations"),
        )
    )
    return origin_distribution, origin_source, origin_year, top_origins


def build_cluster_stats(analysis: DataFrame, cluster_col: str, cluster_def: str, args: argparse.Namespace) -> DataFrame:
    comp = (
        analysis.select(
            "sample",
            "user_id",
            "year",
            F.col(cluster_col).cast("string").alias("cluster_id"),
            "immigrant",
            "n_patents",
            "n_us_citations",
            "country_of_origin_key_clean",
        )
        .filter(nonempty(F.col("cluster_id")))
        .dropDuplicates(["sample", "user_id", "year", "cluster_id"])
    )

    size = comp.groupBy("sample", "cluster_id", "year").agg(
        F.countDistinct("user_id").alias("cluster_size"),
        F.sum("immigrant").alias("n_immigrants"),
        F.avg("immigrant").alias("immigrant_share"),
        F.avg("n_patents").alias("avg_patents"),
        F.avg("n_us_citations").alias("avg_us_citations"),
        F.avg(F.when(F.col("immigrant") == 1, F.col("n_patents"))).alias("avg_patents_immigrant"),
        F.avg(F.when(F.col("immigrant") == 0, F.col("n_patents"))).alias("avg_patents_nonimmigrant"),
        F.countDistinct(F.when(F.col("immigrant") == 1, F.col("user_id"))).alias("n_immigrant_people"),
        F.countDistinct(F.when(F.col("immigrant") == 0, F.col("user_id"))).alias("n_nonimmigrant_people"),
    ).filter(F.col("cluster_size") >= args.min_cluster)

    origin_comp = comp.filter(nonempty(F.col("country_of_origin_key_clean")))
    origin_counts = origin_comp.groupBy("sample", "cluster_id", "year", "country_of_origin_key_clean").agg(
        F.countDistinct("user_id").alias("n_origin")
    )
    origin_div = (
        origin_counts.withColumn("origin_total", F.sum("n_origin").over(Window.partitionBy("sample", "cluster_id", "year")))
        .withColumn("p", F.col("n_origin") / F.col("origin_total"))
        .groupBy("sample", "cluster_id", "year")
        .agg(
            (F.lit(1.0) - F.sum(F.col("p") * F.col("p"))).alias("origin_diversity_hhi"),
            (-F.sum(F.when(F.col("p") > 0, F.col("p") * F.log(F.col("p"))))).alias("origin_diversity_entropy"),
            F.max("origin_total").alias("origin_observed_people"),
        )
    )

    immigrant_origin_counts = (
        origin_comp.filter(F.col("immigrant") == 1)
        .groupBy("sample", "cluster_id", "year", "country_of_origin_key_clean")
        .agg(F.countDistinct("user_id").alias("n_origin"))
    )
    immigrant_origin_div = (
        immigrant_origin_counts.withColumn(
            "origin_total",
            F.sum("n_origin").over(Window.partitionBy("sample", "cluster_id", "year")),
        )
        .withColumn("p", F.col("n_origin") / F.col("origin_total"))
        .groupBy("sample", "cluster_id", "year")
        .agg(
            (F.lit(1.0) - F.sum(F.col("p") * F.col("p"))).alias("immigrant_origin_diversity_hhi"),
            (-F.sum(F.when(F.col("p") > 0, F.col("p") * F.log(F.col("p"))))).alias("immigrant_origin_diversity_entropy"),
        )
    )

    return (
        size.join(origin_div, ["sample", "cluster_id", "year"], "left")
        .join(immigrant_origin_div, ["sample", "cluster_id", "year"], "left")
        .withColumn("cluster_definition", F.lit(cluster_def))
        .withColumn("log_cluster_size", F.log(F.col("cluster_size")))
    )


def cluster_binned_relationships(cluster_stats: DataFrame) -> DataFrame:
    by_size = cluster_stats.withColumn(
        "bin",
        F.ntile(10).over(
            Window.partitionBy("sample", "cluster_definition").orderBy(F.col("log_cluster_size"))
        ),
    ).groupBy("sample", "cluster_definition", "bin").agg(
        F.lit("cluster_size_decile").alias("bin_type"),
        F.count(F.lit(1)).alias("cluster_years"),
        F.avg("cluster_size").alias("mean_cluster_size"),
        F.avg("immigrant_share").alias("mean_immigrant_share"),
        F.avg("avg_patents").alias("mean_cluster_patents"),
        F.avg("avg_us_citations").alias("mean_cluster_us_citations"),
        F.avg("origin_diversity_hhi").alias("mean_origin_diversity_hhi"),
        F.avg("immigrant_origin_diversity_hhi").alias("mean_immigrant_origin_diversity_hhi"),
    )

    by_share = cluster_stats.withColumn(
        "bin",
        F.ntile(10).over(
            Window.partitionBy("sample", "cluster_definition").orderBy(F.col("immigrant_share"))
        ),
    ).groupBy("sample", "cluster_definition", "bin").agg(
        F.lit("immigrant_share_decile").alias("bin_type"),
        F.count(F.lit(1)).alias("cluster_years"),
        F.avg("cluster_size").alias("mean_cluster_size"),
        F.avg("immigrant_share").alias("mean_immigrant_share"),
        F.avg("avg_patents").alias("mean_cluster_patents"),
        F.avg("avg_us_citations").alias("mean_cluster_us_citations"),
        F.avg("origin_diversity_hhi").alias("mean_origin_diversity_hhi"),
        F.avg("immigrant_origin_diversity_hhi").alias("mean_immigrant_origin_diversity_hhi"),
    )
    return by_size.unionByName(by_share)


def mixture_decomposition(cluster_stats: DataFrame, args: argparse.Namespace) -> DataFrame:
    eligible = cluster_stats.filter(
        (F.col("cluster_size") >= args.decomp_min_cluster)
        & (F.col("n_immigrant_people") > 0)
        & (F.col("n_nonimmigrant_people") > 0)
        & F.col("avg_patents_immigrant").isNotNull()
        & F.col("avg_patents_nonimmigrant").isNotNull()
    ).withColumn("beta", F.col("avg_patents_immigrant") - F.col("avg_patents_nonimmigrant")).withColumn(
        "epsilon", F.col("avg_patents_nonimmigrant")
    )

    w = Window.partitionBy("sample", "cluster_definition", "cluster_id").orderBy("year")
    ch = (
        eligible.withColumn("year_lag", F.lag("year").over(w))
        .withColumn("y_lag", F.lag("avg_patents").over(w))
        .withColumn("eps_lag", F.lag("epsilon").over(w))
        .withColumn("beta_lag", F.lag("beta").over(w))
        .withColumn("lambda_lag", F.lag("immigrant_share").over(w))
        .withColumn("size_lag", F.lag("cluster_size").over(w))
        .filter(F.col("year") - F.col("year_lag") == 1)
        .withColumn("d_y", F.col("avg_patents") - F.col("y_lag"))
        .withColumn("comp_baseline", F.col("epsilon") - F.col("eps_lag"))
        .withColumn(
            "comp_share",
            F.lit(0.5) * (F.col("beta") + F.col("beta_lag"))
            * (F.col("immigrant_share") - F.col("lambda_lag")),
        )
        .withColumn(
            "comp_gap",
            F.lit(0.5) * (F.col("immigrant_share") + F.col("lambda_lag"))
            * (F.col("beta") - F.col("beta_lag")),
        )
        .withColumn("weight", F.lit(0.5) * (F.col("cluster_size") + F.col("size_lag")))
        .withColumn(
            "residual",
            F.col("d_y") - F.col("comp_baseline") - F.col("comp_share") - F.col("comp_gap"),
        )
        .filter(
            F.col("d_y").isNotNull()
            & F.col("comp_baseline").isNotNull()
            & F.col("comp_share").isNotNull()
            & F.col("comp_gap").isNotNull()
            & (F.col("weight") > 0)
        )
    )

    moments = ch.groupBy("sample", "cluster_definition").agg(
        F.count(F.lit(1)).alias("n_adjacent_cluster_changes"),
        F.sum("weight").alias("sum_w"),
        F.sum(F.col("weight") * F.col("d_y")).alias("sum_w_y"),
        F.sum(F.col("weight") * F.col("d_y") * F.col("d_y")).alias("sum_w_y2"),
        F.sum(F.col("weight") * F.col("comp_baseline")).alias("sum_w_c1"),
        F.sum(F.col("weight") * F.col("comp_share")).alias("sum_w_c2"),
        F.sum(F.col("weight") * F.col("comp_gap")).alias("sum_w_c3"),
        F.sum(F.col("weight") * F.col("d_y") * F.col("comp_baseline")).alias("sum_w_yc1"),
        F.sum(F.col("weight") * F.col("d_y") * F.col("comp_share")).alias("sum_w_yc2"),
        F.sum(F.col("weight") * F.col("d_y") * F.col("comp_gap")).alias("sum_w_yc3"),
        F.avg(F.abs(F.col("residual"))).alias("mean_abs_identity_residual"),
        F.avg("d_y").alias("mean_change_in_cluster_patents"),
        F.avg("comp_baseline").alias("mean_baseline_component"),
        F.avg("comp_share").alias("mean_immigrant_share_component"),
        F.avg("comp_gap").alias("mean_productivity_gap_component"),
    )

    mean_y = F.col("sum_w_y") / F.col("sum_w")
    var_y = F.col("sum_w_y2") / F.col("sum_w") - mean_y * mean_y

    def alpha(sum_wc: str, sum_wyc: str) -> F.Column:
        mean_c = F.col(sum_wc) / F.col("sum_w")
        cov_yc = F.col(sum_wyc) / F.col("sum_w") - mean_y * mean_c
        return F.when(F.abs(var_y) > 1e-15, cov_yc / var_y)

    return (
        moments.withColumn("alpha_baseline_varshare_w", alpha("sum_w_c1", "sum_w_yc1"))
        .withColumn("alpha_immigrant_share_varshare_w", alpha("sum_w_c2", "sum_w_yc2"))
        .withColumn("alpha_productivity_gap_varshare_w", alpha("sum_w_c3", "sum_w_yc3"))
        .withColumn(
            "sum_alpha_varshare_w",
            F.col("alpha_baseline_varshare_w")
            + F.col("alpha_immigrant_share_varshare_w")
            + F.col("alpha_productivity_gap_varshare_w"),
        )
        .drop(
            "sum_w_y",
            "sum_w_y2",
            "sum_w_c1",
            "sum_w_c2",
            "sum_w_c3",
            "sum_w_yc1",
            "sum_w_yc2",
            "sum_w_yc3",
        )
    )


def build_mobility(analysis: DataFrame) -> DataFrame:
    w = Window.partitionBy("sample", "user_id").orderBy("year")
    d = (
        analysis.withColumn("lag_year", F.lag("year").over(w))
        .withColumn("lag_parent", F.lag("cluster_parent").over(w))
        .withColumn("lag_metro", F.lag("cluster_metro").over(w))
        .withColumn("consecutive", (F.col("year") - F.col("lag_year") == 1).cast("int"))
        .withColumn(
            "parent_move",
            F.when(
                (F.col("consecutive") == 1)
                & nonempty(F.col("cluster_parent"))
                & nonempty(F.col("lag_parent")),
                (F.col("cluster_parent") != F.col("lag_parent")).cast("int"),
            ),
        )
        .withColumn(
            "metro_move",
            F.when(
                (F.col("consecutive") == 1)
                & nonempty(F.col("cluster_metro"))
                & nonempty(F.col("lag_metro")),
                (F.col("cluster_metro") != F.col("lag_metro")).cast("int"),
            ),
        )
    )
    d = add_overall_status(d)
    return d.groupBy("sample", "year", "immigration_status").agg(
        F.countDistinct(F.when(F.col("consecutive") == 1, F.col("user_id"))).alias("people_with_consecutive_year"),
        F.avg("parent_move").alias("parent_move_rate"),
        F.avg("metro_move").alias("metro_move_rate"),
        F.sum(F.coalesce(F.col("parent_move"), F.lit(0))).alias("parent_moves"),
        F.sum(F.coalesce(F.col("metro_move"), F.lit(0))).alias("metro_moves"),
    )


def build_founder_tables(base: DataFrame, top_origins: Sequence[str], args: argparse.Namespace) -> Tuple[DataFrame, DataFrame, DataFrame]:
    people = (
        base.groupBy("user_id")
        .agg(
            F.max("ever_founder").alias("ever_founder"),
            F.max("ever_owner_title").alias("ever_owner_title"),
            F.max("ever_founder_owner_title").alias("ever_founder_owner_title"),
            F.first("founder_entry_year", ignorenulls=True).alias("founder_entry_year"),
            F.first("founder_entry_title", ignorenulls=True).alias("founder_entry_title"),
            F.first("immigration_status", ignorenulls=True).alias("immigration_status"),
            F.first("country_of_origin_clean", ignorenulls=True).alias("country_of_origin"),
            F.min("year").alias("first_US_year_in_window"),
            F.max("year").alias("last_US_year_in_window"),
            F.sum("n_patents").alias("total_patents"),
            F.sum("n_us_citations").alias("total_us_citations"),
            F.first("top10_patenting", ignorenulls=True).alias("top10_patenting"),
        )
    )

    prevalence = add_overall_status(people).groupBy("immigration_status").agg(
        F.countDistinct("user_id").alias("people"),
        F.sum("ever_founder").alias("founders"),
        F.avg(F.col("ever_founder").cast("double")).alias("founder_share"),
        F.sum("ever_owner_title").alias("people_with_owner_title"),
        F.avg(F.col("ever_owner_title").cast("double")).alias("owner_title_share"),
        F.avg("total_patents").alias("mean_total_patents"),
        F.avg("total_us_citations").alias("mean_total_us_citations"),
        F.avg(F.col("top10_patenting").cast("double")).alias("share_top10_patenting"),
    )

    founder_people = people.filter(F.col("ever_founder") == 1)
    entry = founder_people.withColumn(
        "origin_grouped",
        F.when(F.col("country_of_origin").isin(*list(top_origins)), F.col("country_of_origin")).otherwise(F.lit("Other")),
    ).groupBy("immigration_status", "origin_grouped").agg(
        F.countDistinct("user_id").alias("founders"),
        F.avg("founder_entry_year").alias("mean_founder_entry_year"),
        F.expr("percentile_approx(founder_entry_year, 0.5, 10000)").alias("median_founder_entry_year"),
        F.avg("total_patents").alias("mean_total_patents"),
        F.avg("total_us_citations").alias("mean_total_us_citations"),
    )

    event = (
        base.filter(F.col("ever_founder") == 1)
        .filter(F.col("founder_entry_year").isNotNull())
        .withColumn("event_time", F.col("year") - F.col("founder_entry_year"))
        .filter(
            (F.col("event_time") >= -args.founder_event_pre)
            & (F.col("event_time") <= args.founder_event_post)
        )
        .groupBy("immigration_status", "event_time")
        .agg(
            F.countDistinct("user_id").alias("founders_observed"),
            F.avg("n_patents").alias("mean_patents"),
            F.avg("n_us_citations").alias("mean_us_citations"),
            F.avg("log1p_patents").alias("mean_log1p_patents"),
            F.avg("log1p_citations").alias("mean_log1p_citations"),
        )
    )
    return prevalence, entry, event


def step6_metrics_summary(analysis: DataFrame, metrics: Sequence[str]) -> Optional[DataFrame]:
    metrics = [m for m in metrics if m in analysis.columns]
    if not metrics:
        return None
    expanded = add_overall_status(analysis)
    exprs: List[F.Column] = []
    for m in metrics:
        exprs.extend(
            [
                F.count(F.col(m)).alias(f"{m}__n"),
                F.avg(F.col(m).cast("double")).alias(f"{m}__mean"),
                F.sum(F.col(m).cast("double")).alias(f"{m}__sum"),
                F.avg((F.coalesce(F.col(m).cast("double"), F.lit(0.0)) != 0).cast("double")).alias(f"{m}__share_nonzero"),
            ]
        )
    wide = expanded.groupBy("sample", "immigration_status").agg(*exprs)
    structs = [
        F.struct(
            F.lit(m).alias("measure"),
            F.col(f"{m}__n").alias("nonmissing_person_years"),
            F.col(f"{m}__mean").alias("mean"),
            F.col(f"{m}__sum").alias("sum"),
            F.col(f"{m}__share_nonzero").alias("share_nonzero"),
        )
        for m in metrics
    ]
    return wide.select("sample", "immigration_status", F.explode(F.array(*structs)).alias("x")).select(
        "sample", "immigration_status", "x.*"
    )


def make_plots(
    out_dir: str,
    yearly: DataFrame,
    tenure: DataFrame,
    origin_distribution: DataFrame,
    founder_event: DataFrame,
    cluster_binned: DataFrame,
) -> List[str]:
    try:
        import matplotlib

        matplotlib.use("Agg")
        import matplotlib.pyplot as plt
        import pandas as pd
    except Exception as exc:  # pragma: no cover - cluster environment dependent
        msg(f"Plotting skipped because matplotlib/pandas could not be imported: {exc}")
        return []

    figures_dir = os.path.join(out_dir, "figures")
    os.makedirs(figures_dir, exist_ok=True)
    made: List[str] = []

    ypdf = yearly.toPandas()
    tpdf = tenure.toPandas()
    opdf = origin_distribution.toPandas()
    fpdf = founder_event.toPandas()
    cpdf = cluster_binned.toPandas()

    for sample in sorted(ypdf["sample"].dropna().unique()):
        d = ypdf[(ypdf["sample"] == sample) & (ypdf["immigration_status"].isin(["immigrant", "nonimmigrant"]))]
        if len(d) > 0:
            # Immigrant share over time.
            counts = d.pivot_table(index="year", columns="immigration_status", values="people", aggfunc="sum").fillna(0)
            if "immigrant" in counts.columns and "nonimmigrant" in counts.columns:
                share = counts["immigrant"] / (counts["immigrant"] + counts["nonimmigrant"])
                plt.figure(figsize=(9, 5.5))
                plt.plot(share.index, share.values, marker="o")
                plt.xlabel("Year")
                plt.ylabel("Immigrant share")
                plt.title(f"Immigrant share over time — {sample}")
                plt.tight_layout()
                path = os.path.join(figures_dir, f"immigrant_share_over_time__{sample}.png")
                plt.savefig(path, dpi=220)
                plt.close()
                made.append(path)

            for outcome, label in [
                ("mean_patents_per_person_year", "Mean patents per person-year"),
                ("mean_us_citations_per_person_year", "Mean U.S. citations per person-year"),
            ]:
                plt.figure(figsize=(9, 5.5))
                for status in ["nonimmigrant", "immigrant"]:
                    z = d[d["immigration_status"] == status].sort_values("year")
                    if len(z) > 0:
                        plt.plot(z["year"], z[outcome], marker="o", label=status)
                plt.xlabel("Year")
                plt.ylabel(label)
                plt.title(f"{label} — {sample}")
                plt.legend()
                plt.tight_layout()
                path = os.path.join(figures_dir, f"{outcome}__{sample}.png")
                plt.savefig(path, dpi=220)
                plt.close()
                made.append(path)

    for sample in sorted(tpdf["sample"].dropna().unique()):
        d = tpdf[(tpdf["sample"] == sample) & (tpdf["immigration_status"].isin(["immigrant", "nonimmigrant"]))]
        if len(d) > 0:
            plt.figure(figsize=(10, 5.8))
            for status in ["nonimmigrant", "immigrant"]:
                z = d[d["immigration_status"] == status].sort_values("tenure_bin_start")
                if len(z) > 0:
                    plt.plot(z["tenure_bin_start"], z["mean_patents"], marker="o", label=status)
            plt.xlabel("Tenure-bin start")
            plt.ylabel("Mean patents per person-year")
            plt.title(f"Patent productivity over tenure — {sample}")
            plt.legend()
            plt.tight_layout()
            path = os.path.join(figures_dir, f"patents_over_tenure__{sample}.png")
            plt.savefig(path, dpi=220)
            plt.close()
            made.append(path)

    for sample in sorted(opdf["sample"].dropna().unique()):
        d = opdf[opdf["sample"] == sample].sort_values("people", ascending=False).head(20)
        if len(d) > 0:
            d = d.sort_values("people")
            plt.figure(figsize=(10, 7))
            plt.barh(d["country_of_origin_clean"], d["people"])
            plt.xlabel("Immigrant inventors")
            plt.ylabel("Country-of-origin proxy")
            plt.title(f"Top immigrant origins — {sample}")
            plt.tight_layout()
            path = os.path.join(figures_dir, f"top_origins__{sample}.png")
            plt.savefig(path, dpi=220)
            plt.close()
            made.append(path)

    if len(fpdf) > 0:
        plt.figure(figsize=(9, 5.5))
        for status in ["nonimmigrant", "immigrant"]:
            z = fpdf[fpdf["immigration_status"] == status].sort_values("event_time")
            if len(z) > 0:
                plt.plot(z["event_time"], z["mean_patents"], marker="o", label=status)
        plt.axvline(0, linestyle="--")
        plt.xlabel("Years relative to first founder spell")
        plt.ylabel("Mean patents per U.S. person-year")
        plt.title("Patent profile around founder entry")
        plt.legend()
        plt.tight_layout()
        path = os.path.join(figures_dir, "founder_entry_patent_profile.png")
        plt.savefig(path, dpi=220)
        plt.close()
        made.append(path)

    for sample in sorted(cpdf["sample"].dropna().unique()):
        for cluster_def in sorted(cpdf["cluster_definition"].dropna().unique()):
            d = cpdf[
                (cpdf["sample"] == sample)
                & (cpdf["cluster_definition"] == cluster_def)
                & (cpdf["bin_type"] == "cluster_size_decile")
            ].sort_values("bin")
            if len(d) > 0:
                plt.figure(figsize=(8.5, 5.3))
                plt.plot(d["mean_cluster_size"], d["mean_immigrant_share"], marker="o")
                plt.xscale("log")
                plt.xlabel("Mean cluster size in decile (log scale)")
                plt.ylabel("Mean immigrant share")
                plt.title(f"Immigrant share and cluster size — {sample}, {cluster_def}")
                plt.tight_layout()
                path = os.path.join(figures_dir, f"immigrant_share_vs_cluster_size__{sample}__{cluster_def}.png")
                plt.savefig(path, dpi=220)
                plt.close()
                made.append(path)

    return made


def write_documentation(
    out_dir: str,
    args: argparse.Namespace,
    selected_columns: Sequence[str],
    step6_metrics: Sequence[str],
    top_origins: Sequence[str],
    figures: Sequence[str],
) -> None:
    readme = f"""# Corrected V3 immigrant descriptives

## Analysis population

- Input: `{args.input_panel}`
- Years: {args.year_start}–{args.year_end}
- U.S. person-years only: `current_position_observed == 1` and `current_country` is the United States.
- U.S.-educated restriction: `last_university_country` is the United States.
- Immigrant: `immig_any_edu_or_first_job_nonUS == 1`, i.e. any observed education abroad or first observed raw job abroad.
- Country of origin: corrected V3 LinkedIn-history proxy; not citizenship or birthplace.
- Founder: ever has a raw position title containing founder, co-founder/cofounder, or founding partner. Owner-only titles are diagnosed separately and excluded from the founder sample.

## Main outputs

The `tables/` directory contains sample-flow, annual, person-level, origin, productivity, tenure, field, education, demographics, geography, mobility, founder-entry, cluster-diversity, mixture-decomposition, and Step-6 colleague/collaboration summaries. The `figures/` directory contains compact visual summaries. The optional `data/cluster_year_metrics/` directory is written only with `--write-cluster-panel`.

## Important interpretation

The nonimmigrant category means no observed non-U.S. education and no first observed job abroad under the corrected data construction. It should not be interpreted as a legal nationality classification. Citations for recent filing years are mechanically less mature; the script separately tags years through {args.citation_end_year} as citation-mature.
"""
    with open(os.path.join(out_dir, "README.md"), "w", encoding="utf-8") as fh:
        fh.write(readme)

    dictionary = {
        "immigrant": "1 if any observed education is outside the U.S. or the first observed raw job is outside the U.S.",
        "country_of_origin": "Corrected LinkedIn-history proxy: earliest observed non-U.S. education country when available, then first non-U.S. job country, with U.S./other fallbacks for nonimmigrants.",
        "founder": "Ever-founder flag from all canonical raw position spells using founder, co-founder/cofounder, or founding partner title patterns.",
        "U.S. year": "An inventor-year with an observed current spell whose current_country is the United States.",
        "U.S.-educated": "The latest observed education record has country United States.",
        "tenure": "year - first education start year + 3; falls back to year - first position start year; restricted to 0–tenure_max.",
        "origin_diversity_hhi": "1 minus the Herfindahl index of country-of-origin shares within cluster-year.",
        "origin_diversity_entropy": "Shannon entropy of country-of-origin shares within cluster-year.",
        "mixture_decomposition": "Exact midpoint decomposition of adjacent cluster-year changes in mean patents into nonimmigrant baseline, immigrant-share, and immigrant/nonimmigrant productivity-gap components.",
    }
    with open(os.path.join(out_dir, "data_dictionary.json"), "w", encoding="utf-8") as fh:
        json.dump(dictionary, fh, indent=2, sort_keys=True)

    manifest = {
        "created_utc": datetime.now(timezone.utc).isoformat(),
        "input_panel": args.input_panel,
        "positions_dir": args.positions_dir,
        "out_dir": args.out_dir,
        "year_start": args.year_start,
        "year_end": args.year_end,
        "citation_end_year": args.citation_end_year,
        "selected_panel_columns": list(selected_columns),
        "step6_numeric_measures_summarized": list(step6_metrics),
        "top_origins": list(top_origins),
        "figures": [os.path.relpath(p, out_dir) for p in figures],
        "founder_regex": FOUNDER_PATTERN,
        "owner_regex_diagnostic_only": OWNER_PATTERN,
    }
    with open(os.path.join(out_dir, "manifest.json"), "w", encoding="utf-8") as fh:
        json.dump(manifest, fh, indent=2, sort_keys=True)



def main() -> None:
    args = parse_args()
    if args.year_start > args.year_end:
        raise SystemExit("--year-start must not exceed --year-end")
    if args.citation_end_year > args.year_end:
        args.citation_end_year = args.year_end

    out_dir = os.path.abspath(os.path.expanduser(args.out_dir))
    if os.path.exists(out_dir):
        if not args.overwrite:
            raise RuntimeError(f"Output already exists: {out_dir}. Use a new run directory or --overwrite.")
        safe_remove(out_dir)
    os.makedirs(os.path.join(out_dir, "tables"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "figures"), exist_ok=True)
    os.makedirs(os.path.join(out_dir, "data"), exist_ok=True)

    builder = (
        SparkSession.builder.appName("immigrant_descriptives_v3")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .config("spark.sql.files.maxPartitionBytes", "134217728")
    )
    if args.tmpdir:
        os.makedirs(args.tmpdir, exist_ok=True)
        builder = (
            builder.config("spark.local.dir", args.tmpdir)
            .config("spark.sql.warehouse.dir", os.path.join(args.tmpdir, "warehouse"))
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    try:
        msg(f"Opening corrected panel: {args.input_panel}")
        panel_all = (
            spark.read.option("datetimeRebaseMode", "LEGACY")
            .option("int96RebaseMode", "LEGACY")
            .parquet(args.input_panel)
        )
        selected_columns, step6_metrics = choose_panel_columns(panel_all)
        panel = panel_all.select(*selected_columns).filter(
            (F.col("year") >= args.year_start) & (F.col("year") <= args.year_end)
        )
        assert_unique(panel, ["user_id", "year"], "input panel in analysis years")

        founder_flags = build_founder_flags(spark, args.positions_dir).cache()
        sample_flow = create_sample_flow(panel, founder_flags)
        write_single_csv(sample_flow, os.path.join(out_dir, "tables", "sample_flow.csv"), ["stage"])

        msg("Applying U.S.-year and last-U.S.-education restrictions.")
        base = (
            panel.filter(F.col("current_position_observed") == 1)
            .filter(is_us(F.col("current_country")))
            .filter(is_us(F.col("last_university_country")))
            .join(founder_flags, "user_id", "left")
            .fillna(
                {
                    "ever_founder": 0,
                    "ever_owner_title": 0,
                    "ever_founder_owner_title": 0,
                }
            )
        )
        base = add_derived_variables(base, args)

        msg("Computing person-level patent totals and top-10% indicator without restricting the analysis to top inventors.")
        person_totals = base.groupBy("user_id").agg(
            F.sum("n_patents").alias("person_total_patents"),
            F.sum("n_us_citations").alias("person_total_us_citations"),
        )
        p90 = person_totals.approxQuantile("person_total_patents", [0.90], 0.001)[0]
        person_totals = person_totals.withColumn(
            "top10_patenting", (F.col("person_total_patents") >= F.lit(float(p90))).cast("int")
        )
        base = base.join(person_totals, "user_id", "left").cache()
        assert_unique(base, ["user_id", "year"], "restricted base panel")
        msg(f"Top-10 patent threshold in restricted sample: {p90}")

        analysis = duplicate_samples(base).cache()

        yearly = aggregate_yearly(analysis).cache()
        write_single_csv(yearly, os.path.join(out_dir, "tables", "yearly_counts_and_outcomes.csv"), ["sample", "year", "immigration_status"])

        person_summary = aggregate_person_summary(analysis)
        write_single_csv(person_summary, os.path.join(out_dir, "tables", "person_level_summary.csv"), ["sample", "immigration_status"])

        component = (
            analysis.select("sample", "user_id", "immigration_status", "immigration_component")
            .dropDuplicates(["sample", "user_id"])
            .groupBy("sample", "immigration_component")
            .agg(F.countDistinct("user_id").alias("people"))
            .withColumn("share", F.col("people") / F.sum("people").over(Window.partitionBy("sample")))
        )
        write_single_csv(component, os.path.join(out_dir, "tables", "immigration_component_overlap.csv"), ["sample", F.col("people").desc()])

        origin_distribution, origin_source, origin_year, top_origins = build_origin_tables(analysis, args)
        write_single_csv(origin_distribution, os.path.join(out_dir, "tables", "origin_country_distribution.csv"), ["sample", F.col("people").desc(), "country_of_origin_clean"])
        write_single_csv(origin_source, os.path.join(out_dir, "tables", "origin_source_distribution.csv"), ["sample", "immigration_status", F.col("people").desc()])
        write_single_csv(origin_year, os.path.join(out_dir, "tables", "origin_country_by_year.csv"), ["sample", "year", F.col("people").desc()])

        origin_outcomes = (
            analysis.filter(F.col("immigration_status") == "immigrant")
            .filter(nonempty(F.col("country_of_origin_clean")))
            .withColumn(
                "origin_grouped",
                F.when(F.col("country_of_origin_clean").isin(*top_origins), F.col("country_of_origin_clean")).otherwise(F.lit("Other")),
            )
            .groupBy("sample", "origin_grouped")
            .agg(
                F.countDistinct("user_id").alias("people"),
                F.count(F.lit(1)).alias("person_years"),
                F.avg("n_patents").alias("mean_patents"),
                F.avg("n_us_citations").alias("mean_us_citations"),
                F.avg(F.when(F.col("citation_mature_year") == 1, F.col("n_us_citations"))).alias("mean_us_citations_mature_years"),
                F.avg("citations_per_patent").alias("mean_citations_per_patenting_year"),
                F.avg("tenure").alias("mean_tenure"),
                F.avg(F.col("top10_patenting").cast("double")).alias("share_top10_patenting"),
            )
        )
        write_single_csv(origin_outcomes, os.path.join(out_dir, "tables", "outcomes_by_origin.csv"), ["sample", F.col("people").desc()])

        tenure = (
            add_overall_status(analysis.filter(F.col("tenure").isNotNull()))
            .groupBy("sample", "immigration_status", "tenure_bin_start", "tenure_bin")
            .agg(
                F.countDistinct("user_id").alias("people"),
                F.count(F.lit(1)).alias("person_years"),
                F.avg("n_patents").alias("mean_patents"),
                F.avg("n_us_citations").alias("mean_us_citations"),
                F.avg("citations_per_patent").alias("mean_citations_per_patenting_year"),
                F.avg(F.col("top10_patenting").cast("double")).alias("share_top10_patenting"),
            )
        ).cache()
        write_single_csv(tenure, os.path.join(out_dir, "tables", "outcomes_by_tenure.csv"), ["sample", "immigration_status", "tenure_bin_start"])

        # Period summaries make long-run changes easier to read than annual tables alone.
        period = add_overall_status(analysis).groupBy("sample", "period", "period_start", "immigration_status").agg(
            F.countDistinct("user_id").alias("people"),
            F.count(F.lit(1)).alias("person_years"),
            F.avg("n_patents").alias("mean_patents"),
            F.avg("n_us_citations").alias("mean_us_citations"),
            F.avg("tenure").alias("mean_tenure"),
        )
        write_single_csv(period, os.path.join(out_dir, "tables", "five_year_period_summary.csv"), ["sample", "period_start", "immigration_status"])

        # Categorical composition tables, selected dynamically from the corrected schema.
        category_specs = [
            (first_existing(analysis.columns, ["last_degree", "edu_last_degree"]), "last_degree"),
            (first_existing(analysis.columns, ["first_degree", "edu_first_degree"]), "first_degree"),
            (first_existing(analysis.columns, ["au_sex_predicted"]), "predicted_gender"),
            (first_existing(analysis.columns, ["au_ethnicity_predicted"]), "predicted_ethnicity"),
            (first_existing(analysis.columns, ["career_modal_section", "modal_section_y"]), "career_patent_section"),
            (first_existing(analysis.columns, ["career_modal_class", "modal_class_y"]), "career_patent_class"),
            (first_existing(analysis.columns, ["current_onet_major"]), "current_onet_major"),
            (first_existing(analysis.columns, ["current_naics2"]), "current_naics2"),
            (first_existing(analysis.columns, ["current_naics3"]), "current_naics3"),
            (first_existing(analysis.columns, ["current_mapped_role_v3"]), "current_mapped_role_v3"),
        ]
        category_tables: List[DataFrame] = []
        for colname, label in category_specs:
            if colname:
                tab = categorical_distribution(analysis, colname, label, args.top_categories)
                if tab is not None:
                    category_tables.append(tab)
        if category_tables:
            cat_all = category_tables[0]
            for tab in category_tables[1:]:
                cat_all = cat_all.unionByName(tab)
            write_single_csv(cat_all, os.path.join(out_dir, "tables", "education_demographics_field_industry_composition.csv"), ["dimension", "sample", "immigration_status", F.col("people").desc()])

        # Geography composition.
        geo_tabs: List[DataFrame] = []
        for colname, label in [
            ("cluster_state", "state"),
            ("cluster_metro", "metro"),
            ("cluster_parent", "parent_firm"),
        ]:
            if colname in analysis.columns:
                d = analysis.filter(nonempty(F.col(colname)))
                top = [
                    r[colname]
                    for r in (
                        d.filter(F.col("sample") == "all_inventors")
                        .groupBy(colname)
                        .agg(F.countDistinct("user_id").alias("people"))
                        .orderBy(F.col("people").desc())
                        .limit(args.top_categories)
                        .collect()
                    )
                ]
                d = d.withColumn(
                    "geography",
                    F.when(F.col(colname).isin(*top), F.col(colname)).otherwise(F.lit("Other")),
                )
                geo_tabs.append(
                    add_overall_status(d).groupBy("sample", "immigration_status", "geography").agg(
                        F.lit(label).alias("geography_level"),
                        F.countDistinct("user_id").alias("people"),
                        F.count(F.lit(1)).alias("person_years"),
                        F.avg("n_patents").alias("mean_patents"),
                        F.avg("n_us_citations").alias("mean_us_citations"),
                    )
                )
        if geo_tabs:
            geo_all = geo_tabs[0]
            for tab in geo_tabs[1:]:
                geo_all = geo_all.unionByName(tab)
            write_single_csv(geo_all, os.path.join(out_dir, "tables", "geography_distribution.csv"), ["geography_level", "sample", "immigration_status", F.col("people").desc()])

        founder_prevalence, founder_entry, founder_event = build_founder_tables(base, top_origins, args)
        founder_event = founder_event.cache()
        write_single_csv(founder_prevalence, os.path.join(out_dir, "tables", "founder_prevalence_by_immigration_status.csv"), ["immigration_status"])
        write_single_csv(founder_entry, os.path.join(out_dir, "tables", "founder_entry_and_origin_summary.csv"), ["immigration_status", F.col("founders").desc()])
        write_single_csv(founder_event, os.path.join(out_dir, "tables", "founder_entry_event_profile.csv"), ["immigration_status", "event_time"])

        mobility = build_mobility(analysis)
        write_single_csv(mobility, os.path.join(out_dir, "tables", "mobility_by_year.csv"), ["sample", "year", "immigration_status"])

        # Cluster-level composition and decomposition.
        cluster_frames: List[DataFrame] = []
        for colname, label in [
            ("cluster_state", "state"),
            ("cluster_metro", "metro"),
            ("cluster_parent", "parent"),
            ("cluster_metro_parent", "metro_parent"),
        ]:
            if colname in analysis.columns:
                msg(f"Building cluster-year metrics: {label}")
                cluster_frames.append(build_cluster_stats(analysis, colname, label, args))

        if not cluster_frames:
            raise RuntimeError("No usable cluster definitions were available.")
        cluster_stats = cluster_frames[0]
        for frame in cluster_frames[1:]:
            cluster_stats = cluster_stats.unionByName(frame)
        cluster_stats = cluster_stats.cache()

        cluster_year_summary = cluster_stats.groupBy("sample", "cluster_definition", "year").agg(
            F.countDistinct("cluster_id").alias("clusters"),
            F.avg("cluster_size").alias("mean_cluster_size"),
            F.expr("percentile_approx(cluster_size, 0.5, 10000)").alias("median_cluster_size"),
            F.avg("immigrant_share").alias("mean_unweighted_cluster_immigrant_share"),
            (F.sum(F.col("immigrant_share") * F.col("cluster_size")) / F.sum("cluster_size")).alias("person_weighted_immigrant_share"),
            F.avg("avg_patents").alias("mean_unweighted_cluster_patents"),
            F.avg("origin_diversity_hhi").alias("mean_origin_diversity_hhi"),
            F.avg("origin_diversity_entropy").alias("mean_origin_diversity_entropy"),
            F.avg("immigrant_origin_diversity_hhi").alias("mean_immigrant_origin_diversity_hhi"),
        )
        write_single_csv(cluster_year_summary, os.path.join(out_dir, "tables", "cluster_year_summary.csv"), ["sample", "cluster_definition", "year"])

        cluster_binned = cluster_binned_relationships(cluster_stats).cache()
        write_single_csv(cluster_binned, os.path.join(out_dir, "tables", "cluster_binned_relationships.csv"), ["sample", "cluster_definition", "bin_type", "bin"])

        decomp = mixture_decomposition(cluster_stats, args)
        write_single_csv(decomp, os.path.join(out_dir, "tables", "cluster_mixture_decomposition.csv"), ["sample", "cluster_definition"])

        if args.write_cluster_panel:
            cluster_path = os.path.join(out_dir, "data", "cluster_year_metrics")
            (
                cluster_stats.repartition("sample", "cluster_definition")
                .write.mode("overwrite")
                .partitionBy("sample", "cluster_definition")
                .option("compression", "snappy")
                .parquet(cluster_path)
            )
            msg(f"Wrote {cluster_path}")

        step6_summary = step6_metrics_summary(analysis, step6_metrics)
        if step6_summary is not None:
            write_single_csv(step6_summary, os.path.join(out_dir, "tables", "step6_colleague_citation_collaboration_summary.csv"), ["measure", "sample", "immigration_status"])

        figures: List[str] = []
        if not args.skip_plots:
            figures = make_plots(
                out_dir,
                yearly,
                tenure,
                origin_distribution,
                founder_event,
                cluster_binned,
            )

        write_documentation(
            out_dir,
            args,
            selected_columns,
            step6_metrics,
            top_origins,
            figures,
        )
        Path(os.path.join(out_dir, "_DESCRIPTIVES_COMPLETE")).touch()
        msg(f"[DONE] Detailed immigrant descriptives completed: {out_dir}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
