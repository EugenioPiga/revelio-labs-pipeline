#!/usr/bin/env python3
"""
founder_firm_outcomes_v3.py

Spell-safe founder--firm outcomes built from the validated V3 Revelio pipeline.

Why this differs from the old V2 scripts
-----------------------------------------
* Founder identification and founder--firm matching use raw V3 Step 3 position
  spells, never annual current_* fields. This prevents a founder title from
  being joined to a previous employer's RCID.
* Core founders (founder, co-founder, founding partner) are separate from
  founding employees, engineers, team members, and advisors.
* Founder spells without an RCID are kept in a separate unresolved output and
  are never attached to firm outcomes.
* The scalable two-stage design is retained from the sharded legacy script:
    1) build firm-year outcomes once;
    2) build founder outputs independently for each V3 source shard;
    3) merge the founder shards.

Modes
-----
firm_year
    Build one global firm-year outcome table from the V3 merged annual panel.
founder_shard
    Build founder links, pre-founder characteristics, and post-founder firm
    outcomes for one V3 raw-position / annual-panel shard.
merge_founder_shards
    Merge the completed founder-shard outputs into final analysis tables.

Outputs under --out-dir
-----------------------
firm_year_outcomes_current_rcid_v3/
founder_raw_spell_candidates_shards/shard=XX/
founder_company_links_shards/shard=XX/
founder_unresolved_spells_shards/shard=XX/
founder_pre_characteristics_shards/shard=XX/
founder_firm_outcomes_shards/shard=XX/
founder_company_links_v3/
founder_unresolved_spells_v3/
founder_pre_characteristics_v3/
founder_firm_outcomes_v3/
diagnostics/
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, Iterable, List, Sequence, Tuple

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


V3_ROOT = "/labs/khanna/linkedin_202507/processed/v3_spell_integrity"
DEFAULT_SPELL_SHARDS = f"{V3_ROOT}/step3_inventors_matched_positions_education"
DEFAULT_ANNUAL_SHARDS = f"{V3_ROOT}/inventor_year_shards"
DEFAULT_ANNUAL_PANEL = f"{V3_ROOT}/inventor_year_merged"
DEFAULT_COMPANY_REF = "/labs/khanna/linkedin_202507/academic_company_ref"
DEFAULT_OUT = "/home/epiga/revelio_labs/output/founder_firm_outcomes_v3"


# -----------------------------------------------------------------------------
# Arguments and execution helpers
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(
        description="Build V3 spell-safe founder--firm links and outcomes."
    )
    ap.add_argument(
        "--mode",
        required=True,
        choices=["preflight", "firm_year", "founder_shard", "merge_founder_shards"],
    )
    ap.add_argument("--spell-shards-dir", default=DEFAULT_SPELL_SHARDS)
    ap.add_argument("--annual-shards-dir", default=DEFAULT_ANNUAL_SHARDS)
    ap.add_argument("--annual-panel-dir", default=DEFAULT_ANNUAL_PANEL)
    ap.add_argument("--company-ref-dir", default=DEFAULT_COMPANY_REF)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)

    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, default=None)
    ap.add_argument(
        "--link-definition",
        choices=["core", "broad"],
        default="core",
        help="core = founder/co-founder/founding partner; broad also includes founding employees/team/advisors.",
    )

    ap.add_argument(
        "--attach-legacy-context",
        action="store_true",
        help="Optionally attach existing PPML/growth files. Off by default because those are legacy V2-derived enrichments.",
    )
    ap.add_argument(
        "--ppml-fe-dir",
        default="/home/epiga/revelio_labs/output/lifecycle_ppml_fe/ppml_fe",
    )
    ap.add_argument(
        "--growth-dir",
        default="/home/epiga/revelio_labs/output/lifecycle_growth_scatter",
    )

    ap.add_argument("--threads", type=int, default=32)
    ap.add_argument("--shuffle-partitions", type=int, default=1600)
    ap.add_argument("--coalesce", type=int, default=128)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument(
        "--overwrite",
        action="store_true",
        help="Overwrite only this script's own output path(s). Never points to V2 or V3 pipeline inputs.",
    )
    return ap.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


def make_spark(args: argparse.Namespace) -> SparkSession:
    # Do not set .master() here. The supplied sbatches use spark-submit and
    # provide the driver memory and local master explicitly.
    builder = (
        SparkSession.builder.appName(f"founder_firm_outcomes_v3_{args.mode}")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    )
    if args.tmpdir:
        tmp = os.path.abspath(args.tmpdir)
        builder = (
            builder.config("spark.local.dir", tmp)
            .config("spark.sql.warehouse.dir", os.path.join(tmp, "warehouse"))
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(tmp, 'tmp')}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(tmp, 'tmp')}")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# -----------------------------------------------------------------------------
# Generic utilities
# -----------------------------------------------------------------------------


def clean_text_expr(name: str):
    x = F.trim(F.col(name).cast("string"))
    return (
        F.when(x.isNull() | (x == ""), F.lit(None).cast("string"))
        .when(F.lower(x).isin("null", "none", "nan", "n/a", "na"), F.lit(None).cast("string"))
        .otherwise(x)
    )


def select_schema(df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    """Select fields that exist, adding typed nulls for optional V3 fields."""
    cols = []
    for name, dtype in fields.items():
        if name in df.columns:
            cols.append(F.col(name).cast(dtype).alias(name))
        else:
            cols.append(F.lit(None).cast(dtype).alias(name))
    return df.select(*cols)


def require_columns(df: DataFrame, columns: Sequence[str], label: str) -> None:
    missing = [c for c in columns if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label}: missing required columns: {missing}")


def assert_unique(df: DataFrame, keys: Sequence[str], label: str) -> None:
    duplicate = df.groupBy(*keys).count().filter(F.col("count") > 1).limit(1).count()
    if duplicate:
        raise RuntimeError(f"{label}: duplicate keys detected for {list(keys)}")


def output_mode(overwrite: bool) -> str:
    return "overwrite" if overwrite else "errorifexists"


def write_parquet(df: DataFrame, path: str, coalesce: int, overwrite: bool) -> None:
    log(f"[WRITE] {path} (mode={output_mode(overwrite)})")
    (
        df.coalesce(max(1, coalesce))
        .write.mode(output_mode(overwrite))
        .option("compression", "snappy")
        .parquet(path)
    )


def write_csv(df: DataFrame, path: str, overwrite: bool) -> None:
    log(f"[WRITE CSV] {path} (mode={output_mode(overwrite)})")
    (
        df.coalesce(1)
        .write.mode(output_mode(overwrite))
        .option("header", "true")
        .csv(path)
    )


def shard_path(root: str, shard_idx: int) -> str:
    return os.path.join(root, f"shard={shard_idx:02d}")


def read_csv_if_exists(spark: SparkSession, path: str):
    if not os.path.exists(path):
        log(f"[WARN] Optional legacy file not found; skipping: {path}")
        return None
    return spark.read.option("header", "true").option("inferSchema", "true").csv(path)


# -----------------------------------------------------------------------------
# Raw spell loading and founder classification
# -----------------------------------------------------------------------------


SPELL_FIELDS: Dict[str, str] = {
    "user_id": "string",
    "position_id": "long",
    "company_name": "string",
    "rcid": "long",
    "ultimate_parent_rcid": "long",
    "ultimate_parent_company_name": "string",
    "title_raw": "string",
    "title_translated": "string",
    "startdate": "date",
    "enddate": "date",
    "position_number": "double",
    "city": "string",
    "state": "string",
    "country": "string",
    "region": "string",
    "metro_area": "string",
    "msa": "string",
    "location_raw": "string",
    "naics_code": "string",
    "naics_description": "string",
    "naics6": "string",
    "naics4": "string",
    "naics3": "string",
    "naics2": "string",
    "job_category_v2": "string",
    "mapped_role_v3": "string",
    "role_k150_v2": "string",
    "role_k150_v3": "string",
    "onet_code": "string",
    "onet_title": "string",
    "seniority": "double",
    "salary": "double",
    "total_compensation": "double",
    "remote_suitability": "double",
    "position_weight": "double",
}

CORE_FOUNDER_RX = (
    r"(^|[^a-z])(?:co[\s-]*founder|cofounder|founder|founding\s+partner)([^a-z]|$)"
)
COFOUNDER_RX = r"(^|[^a-z])(?:co[\s-]*founder|cofounder)([^a-z]|$)"
FOUNDING_PARTNER_RX = r"(^|[^a-z])founding\s+partner([^a-z]|$)"
FOUNDING_EMPLOYEE_RX = (
    r"(^|[^a-z])(?:founding\s+engineer|founding\s+scientist|founding\s+member|"
    r"founding\s+team|founding\s+employee|founding\s+developer|founding\s+designer|"
    r"founding\s+advisor)([^a-z]|$)"
)


def read_company_parent_reference(spark: SparkSession, company_ref_dir: str) -> DataFrame:
    """Read only a same-company RCID -> parent-RCID fallback mapping."""
    if not os.path.exists(company_ref_dir):
        log(f"[WARN] Company reference missing; parent fallback uses raw parent then RCID: {company_ref_dir}")
        return spark.createDataFrame([], "rcid_ref long, parent_from_ref long")

    ref_raw = spark.read.parquet(company_ref_dir)
    needed = ["rcid", "ultimate_parent_rcid"]
    missing = [c for c in needed if c not in ref_raw.columns]
    if missing:
        log(f"[WARN] Company reference lacks {missing}; parent fallback uses raw parent then RCID.")
        return spark.createDataFrame([], "rcid_ref long, parent_from_ref long")

    return (
        ref_raw.select(
            F.col("rcid").cast("long").alias("rcid_ref"),
            F.col("ultimate_parent_rcid").cast("long").alias("parent_from_ref"),
        )
        .groupBy("rcid_ref")
        .agg(F.max("parent_from_ref").alias("parent_from_ref"))
    )


def read_standard_spells(spark: SparkSession, path: str, company_ref_dir: str) -> DataFrame:
    log(f"[LOAD RAW SPELLS] {path}")
    raw = spark.read.parquet(path)
    require_columns(raw, ["user_id", "position_id", "title_raw", "company_name", "rcid"], "raw V3 spells")

    pos = select_schema(raw, SPELL_FIELDS)
    for c in [
        "user_id", "company_name", "ultimate_parent_company_name", "title_raw", "title_translated",
        "city", "state", "country", "region", "metro_area", "msa", "location_raw",
        "naics_code", "naics_description", "naics6", "naics4", "naics3", "naics2",
        "job_category_v2", "mapped_role_v3", "role_k150_v2", "role_k150_v3", "onet_code", "onet_title",
    ]:
        pos = pos.withColumn(c, clean_text_expr(c))

    assert_unique(pos, ["user_id", "position_id"], "raw V3 position spells")

    ref = read_company_parent_reference(spark, company_ref_dir)
    pos = pos.join(ref, pos["rcid"] == ref["rcid_ref"], "left")
    pos = (
        pos.withColumn(
            "spell_parent_rcid",
            F.coalesce(F.col("ultimate_parent_rcid"), F.col("parent_from_ref"), F.col("rcid")),
        )
        .withColumn(
            "spell_parent_source",
            F.when(F.col("ultimate_parent_rcid").isNotNull(), F.lit("raw_spell"))
            .when(F.col("parent_from_ref").isNotNull(), F.lit("company_reference"))
            .when(F.col("rcid").isNotNull(), F.lit("self_parent_fallback"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "spell_parent_company_name",
            F.when(
                F.col("spell_parent_rcid").eqNullSafe(F.col("rcid")), F.col("company_name")
            ).otherwise(F.col("ultimate_parent_company_name")),
        )
        .drop("rcid_ref", "parent_from_ref")
    )
    return pos


def add_founder_flags(spells: DataFrame) -> DataFrame:
    title = F.lower(
        F.concat_ws(
            " ",
            F.coalesce(F.col("title_raw"), F.lit("")),
            F.coalesce(F.col("title_translated"), F.lit("")),
        )
    )
    title = F.trim(F.regexp_replace(F.regexp_replace(title, r"[/,&|()\[\]{}:;]+", " "), r"\s+", " "))

    return (
        spells.withColumn("_title_norm", title)
        .withColumn("core_founder_title_flag", F.col("_title_norm").rlike(CORE_FOUNDER_RX).cast("int"))
        .withColumn("cofounder_title_flag", F.col("_title_norm").rlike(COFOUNDER_RX).cast("int"))
        .withColumn("founding_partner_title_flag", F.col("_title_norm").rlike(FOUNDING_PARTNER_RX).cast("int"))
        .withColumn("founding_employee_title_flag", F.col("_title_norm").rlike(FOUNDING_EMPLOYEE_RX).cast("int"))
        .withColumn(
            "broad_founder_title_flag",
            F.greatest(F.col("core_founder_title_flag"), F.col("founding_employee_title_flag")),
        )
        .withColumn(
            "founder_title_class",
            F.when(F.col("cofounder_title_flag") == 1, F.lit("cofounder"))
            .when(F.col("founding_partner_title_flag") == 1, F.lit("founding_partner"))
            .when(F.col("core_founder_title_flag") == 1, F.lit("founder"))
            .when(F.col("founding_employee_title_flag") == 1, F.lit("founding_employee_or_team"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn("founder_entry_date", F.coalesce(F.col("startdate"), F.col("enddate")))
        .withColumn("founder_entry_year", F.year(F.col("founder_entry_date")).cast("int"))
        .drop("_title_norm")
    )


def build_founder_links(
    spells: DataFrame,
    link_definition: str,
) -> Tuple[DataFrame, DataFrame, DataFrame]:
    """Return raw candidates, resolved user-firm links, and unresolved spells."""
    flagged = add_founder_flags(spells)
    candidates = flagged.filter(F.col("broad_founder_title_flag") == 1)

    selected_flag = "core_founder_title_flag" if link_definition == "core" else "broad_founder_title_flag"
    selected = candidates.filter(F.col(selected_flag) == 1)

    unresolved = (
        selected.filter(F.col("rcid").isNull())
        .withColumn(
            "unresolved_firm_key",
            F.sha2(
                F.concat_ws(
                    "||",
                    F.coalesce(F.lower(F.col("company_name")), F.lit("")),
                    F.coalesce(F.lower(F.col("city")), F.lit("")),
                    F.coalesce(F.lower(F.col("state")), F.lit("")),
                    F.coalesce(F.lower(F.col("country")), F.lit("")),
                    F.col("position_id").cast("string"),
                ),
                256,
            ),
        )
        .withColumn("unresolved_reason", F.lit("raw_founder_spell_missing_rcid"))
    )

    resolved_rows = selected.filter(F.col("rcid").isNotNull())
    rank = Window.partitionBy("user_id", "rcid").orderBy(
        F.col("founder_entry_date").asc_nulls_last(),
        F.col("startdate").asc_nulls_last(),
        F.col("position_id").asc_nulls_last(),
    )
    first = resolved_rows.withColumn("_rn", F.row_number().over(rank)).filter(F.col("_rn") == 1).drop("_rn")

    first = first.select(
        "user_id",
        F.col("rcid").alias("founded_rcid"),
        F.col("position_id").alias("founder_position_id"),
        F.col("founder_entry_date").alias("first_founder_entry_date"),
        F.col("founder_entry_year").alias("first_founder_entry_year"),
        F.col("company_name").alias("founded_company_name"),
        F.col("spell_parent_rcid").alias("founder_parent_rcid"),
        F.col("spell_parent_company_name").alias("founder_parent_company_name"),
        F.col("spell_parent_source").alias("founder_parent_source"),
        F.col("title_raw").alias("founder_title_raw"),
        F.col("title_translated").alias("founder_title_translated"),
        "founder_title_class",
        "core_founder_title_flag",
        "cofounder_title_flag",
        "founding_partner_title_flag",
        "founding_employee_title_flag",
        "broad_founder_title_flag",
        F.col("startdate").alias("founder_spell_startdate"),
        F.col("enddate").alias("founder_spell_enddate"),
        F.col("city").alias("founder_city"),
        F.col("state").alias("founder_state"),
        F.col("country").alias("founder_country"),
        F.col("region").alias("founder_region"),
        F.col("metro_area").alias("founder_metro_area"),
        F.col("msa").alias("founder_msa"),
        F.col("location_raw").alias("founder_location_raw"),
        F.col("naics_code").alias("founder_naics_code"),
        F.col("naics_description").alias("founder_naics_description"),
        F.col("naics6").alias("founder_naics6"),
        F.col("naics3").alias("founder_naics3"),
        F.col("job_category_v2").alias("founder_job_category_v2"),
        F.col("mapped_role_v3").alias("founder_mapped_role_v3"),
        F.col("role_k150_v2").alias("founder_role_k150_v2"),
        F.col("role_k150_v3").alias("founder_role_k150_v3"),
        F.col("onet_code").alias("founder_onet_code"),
        F.col("onet_title").alias("founder_onet_title"),
        F.col("seniority").alias("founder_seniority"),
        F.col("salary").alias("founder_salary"),
        F.col("total_compensation").alias("founder_total_compensation"),
        F.col("remote_suitability").alias("founder_remote_suitability"),
        F.col("position_number").alias("founder_position_number"),
        F.col("position_weight").alias("founder_position_weight"),
    )

    agg = (
        resolved_rows.groupBy("user_id", "rcid")
        .agg(
            F.count("*").alias("n_selected_founder_spells"),
            F.countDistinct("position_id").alias("n_selected_founder_positions"),
            F.min("founder_entry_date").alias("first_founder_entry_date_all_spells"),
            F.max("enddate").alias("last_founder_spell_enddate"),
            F.max("core_founder_title_flag").alias("core_founder_ever_flag"),
            F.max("cofounder_title_flag").alias("cofounder_ever_flag"),
            F.max("founding_partner_title_flag").alias("founding_partner_ever_flag"),
            F.max("founding_employee_title_flag").alias("founding_employee_ever_flag"),
            F.max("broad_founder_title_flag").alias("broad_founder_ever_flag"),
        )
        .withColumnRenamed("rcid", "founded_rcid")
    )

    links = first.join(agg, ["user_id", "founded_rcid"], "left")
    assert_unique(links, ["user_id", "founded_rcid"], "resolved founder links")
    return candidates, links, unresolved


# -----------------------------------------------------------------------------
# Annual V3 data: profile context and pre-founder characteristics
# -----------------------------------------------------------------------------


ANNUAL_FIELDS: Dict[str, str] = {
    "user_id": "string",
    "year": "int",
    "n_patents": "double",
    "n_applications": "double",
    "n_first_inventor": "double",
    "n_us_citations": "double",
    "current_rcid": "long",
    "current_parent_rcid": "long",
    "current_company_name": "string",
    "current_parent_company_name": "string",
    "current_title_raw": "string",
    "current_title_translated": "string",
    "current_onet_code": "string",
    "current_onet_title": "string",
    "current_naics_code": "string",
    "current_naics3": "string",
    "current_naics6": "string",
    "current_seniority": "double",
    "current_salary": "double",
    "current_total_compensation": "double",
    "current_position_observed": "int",
    "au_firstname": "string",
    "au_lastname": "string",
    "au_fullname": "string",
    "au_highest_degree": "string",
    "au_prestige": "double",
    "au_sex_predicted": "string",
    "au_ethnicity_predicted": "string",
    "au_user_country": "string",
    "au_user_location": "string",
    "au_profile_title": "string",
    "has_education": "int",
    "n_educations": "int",
    "edu_first_university_country": "string",
    "edu_last_university_country": "string",
    "edu_first_degree": "string",
    "edu_last_degree": "string",
    "edu_first_field": "string",
    "edu_last_field": "string",
    "immig_deg_first_nonUS": "double",
    "immig_job_first_nonUS": "double",
    "immig_first_deg_or_job_nonUS": "double",
    "career_modal_section": "string",
    "career_modal_class": "string",
}


def read_standard_annual(spark: SparkSession, path: str) -> DataFrame:
    log(f"[LOAD ANNUAL PANEL] {path}")
    raw = spark.read.parquet(path)
    require_columns(raw, ["user_id", "year", "n_patents", "n_applications", "n_us_citations"], "V3 annual panel")
    annual = select_schema(raw, ANNUAL_FIELDS)
    for c in [
        "user_id", "current_company_name", "current_parent_company_name", "current_title_raw",
        "current_title_translated", "current_onet_code", "current_onet_title", "current_naics_code",
        "current_naics3", "current_naics6", "au_firstname", "au_lastname", "au_fullname",
        "au_highest_degree", "au_sex_predicted", "au_ethnicity_predicted", "au_user_country",
        "au_user_location", "au_profile_title", "edu_first_university_country", "edu_last_university_country",
        "edu_first_degree", "edu_last_degree", "edu_first_field", "edu_last_field",
        "career_modal_section", "career_modal_class",
    ]:
        annual = annual.withColumn(c, clean_text_expr(c))
    for c in ["n_patents", "n_applications", "n_first_inventor", "n_us_citations"]:
        annual = annual.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    assert_unique(annual, ["user_id", "year"], "V3 annual panel")
    return annual


def build_user_profile_context(annual: DataFrame) -> DataFrame:
    static_cols = [c for c in ANNUAL_FIELDS if c.startswith("au_") or c.startswith("edu_") or c.startswith("immig_") or c in [
        "has_education", "n_educations", "career_modal_section", "career_modal_class"
    ]]
    full_window = (
        Window.partitionBy("user_id")
        .orderBy(F.col("year").asc_nulls_last())
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    pick_one = Window.partitionBy("user_id").orderBy(F.col("year").asc_nulls_last())

    prof = annual.select(
        "user_id",
        *[F.first(F.col(c), ignorenulls=True).over(full_window).alias(c) for c in static_cols],
        F.row_number().over(pick_one).alias("_rn"),
    ).filter(F.col("_rn") == 1).drop("_rn")

    immig_cols = [
        "immig_deg_first_nonUS", "immig_job_first_nonUS", "immig_first_deg_or_job_nonUS",
    ]
    prof = prof.withColumn(
        "founder_immigrant_any",
        F.greatest(*[F.coalesce(F.col(c).cast("int"), F.lit(0)) for c in immig_cols]),
    )
    return prof


def build_pre_founder_characteristics(annual: DataFrame, links: DataFrame) -> DataFrame:
    keys = links.select("user_id", "founded_rcid", "first_founder_entry_year")
    pre = (
        annual.join(F.broadcast(keys), "user_id", "inner")
        .filter(F.col("first_founder_entry_year").isNotNull())
        .filter(F.col("year") < F.col("first_founder_entry_year"))
    )

    agg = (
        pre.groupBy("user_id", "founded_rcid")
        .agg(
            F.count("*").alias("prior_inventor_year_rows"),
            F.countDistinct("year").alias("prior_years_observed"),
            F.min("year").alias("prior_first_observed_year"),
            F.max("year").alias("prior_last_observed_year"),
            F.sum("n_patents").alias("prior_patents"),
            F.sum("n_applications").alias("prior_applications"),
            F.sum("n_first_inventor").alias("prior_first_inventor_patents"),
            F.sum("n_us_citations").alias("prior_us_citations"),
            F.countDistinct("current_rcid").alias("prior_unique_current_rcids"),
            F.countDistinct("current_parent_rcid").alias("prior_unique_parent_rcids"),
            F.countDistinct("current_onet_code").alias("prior_unique_onet_codes"),
            F.countDistinct("current_naics6").alias("prior_unique_naics6"),
            F.avg("current_seniority").alias("prior_avg_seniority"),
            F.max("current_seniority").alias("prior_max_seniority"),
            F.avg("current_salary").alias("prior_avg_salary"),
            F.avg("current_total_compensation").alias("prior_avg_total_compensation"),
        )
    )

    latest_w = Window.partitionBy("user_id", "founded_rcid").orderBy(F.col("year").desc())
    latest = (
        pre.withColumn("_rn", F.row_number().over(latest_w))
        .filter(F.col("_rn") == 1)
        .select(
            "user_id", "founded_rcid",
            F.col("year").alias("prior_latest_year"),
            F.col("current_rcid").alias("prior_latest_rcid"),
            F.col("current_parent_rcid").alias("prior_latest_parent_rcid"),
            F.col("current_company_name").alias("prior_latest_company_name"),
            F.col("current_parent_company_name").alias("prior_latest_parent_company_name"),
            F.col("current_title_raw").alias("prior_latest_title_raw"),
            F.col("current_title_translated").alias("prior_latest_title_translated"),
            F.col("current_onet_code").alias("prior_latest_onet_code"),
            F.col("current_onet_title").alias("prior_latest_onet_title"),
            F.col("current_naics_code").alias("prior_latest_naics_code"),
            F.col("current_naics3").alias("prior_latest_naics3"),
            F.col("current_naics6").alias("prior_latest_naics6"),
            F.col("current_seniority").alias("prior_latest_seniority"),
            F.col("current_salary").alias("prior_latest_salary"),
            F.col("current_total_compensation").alias("prior_latest_total_compensation"),
        )
    )

    out = agg.join(latest, ["user_id", "founded_rcid"], "left")
    return out


# -----------------------------------------------------------------------------
# Firm-year outcomes and founder-specific post windows
# -----------------------------------------------------------------------------


def build_firm_year_outcomes(annual: DataFrame) -> DataFrame:
    return (
        annual.filter(F.col("current_rcid").isNotNull())
        .groupBy(F.col("current_rcid").alias("firm_rcid"), "year")
        .agg(
            F.count("*").alias("firm_inventor_year_rows"),
            F.countDistinct("user_id").alias("firm_unique_inventors"),
            F.sum("n_patents").alias("firm_patents"),
            F.sum("n_applications").alias("firm_applications"),
            F.sum("n_first_inventor").alias("firm_first_inventor_patents"),
            F.sum("n_us_citations").alias("firm_us_citations"),
        )
        .withColumn(
            "firm_citations_per_patent",
            F.when(F.col("firm_patents") > 0, F.col("firm_us_citations") / F.col("firm_patents")),
        )
        .withColumn(
            "firm_patents_per_inventor_year",
            F.when(F.col("firm_inventor_year_rows") > 0, F.col("firm_patents") / F.col("firm_inventor_year_rows")),
        )
    )


def build_post_founder_outcomes(links: DataFrame, firm_year: DataFrame) -> DataFrame:
    keys = links.select("user_id", "founded_rcid", "first_founder_entry_year")
    fy = (
        firm_year.alias("fy")
        .join(
            keys.alias("k"),
            F.col("fy.firm_rcid") == F.col("k.founded_rcid"),
            "inner",
        )
        .select("k.user_id", "k.founded_rcid", "k.first_founder_entry_year", "fy.*")
        .filter(F.col("year") >= F.col("first_founder_entry_year"))
    )

    windows = [("y0_3", 0, 3), ("y0_5", 0, 5), ("y0_10", 0, 10), ("y1_3", 1, 3), ("y1_5", 1, 5), ("y1_10", 1, 10)]
    metrics = [
        ("firm_patents", "patents"),
        ("firm_us_citations", "us_citations"),
        ("firm_applications", "applications"),
        ("firm_first_inventor_patents", "first_inventor_patents"),
        ("firm_inventor_year_rows", "inventor_year_rows"),
        ("firm_unique_inventors", "unique_inventors_sum_over_years"),
    ]

    aggs = [
        F.countDistinct("year").alias("firm_post_years_observed"),
        F.min("year").alias("firm_first_post_year_observed"),
        F.max("year").alias("firm_last_post_year_observed"),
    ]
    for source, stub in metrics:
        aggs.append(F.sum(F.col(source)).alias(f"firm_{stub}_post_all"))

    for tag, lower, upper in windows:
        in_window = (
            (F.col("year") >= F.col("first_founder_entry_year") + F.lit(lower))
            & (F.col("year") <= F.col("first_founder_entry_year") + F.lit(upper))
        )
        for source, stub in metrics:
            aggs.append(
                F.sum(F.when(in_window, F.col(source)).otherwise(F.lit(0.0))).alias(f"firm_{stub}_{tag}")
            )

    out = fy.groupBy("user_id", "founded_rcid").agg(*aggs)
    for tag in ["post_all"] + [x[0] for x in windows]:
        patent_col = f"firm_patents_{tag}"
        citation_col = f"firm_us_citations_{tag}"
        iy_col = f"firm_inventor_year_rows_{tag}"
        out = out.withColumn(
            f"firm_citations_per_patent_{tag}",
            F.when(F.col(patent_col) > 0, F.col(citation_col) / F.col(patent_col)),
        ).withColumn(
            f"firm_patents_per_inventor_year_{tag}",
            F.when(F.col(iy_col) > 0, F.col(patent_col) / F.col(iy_col)),
        )
    return out


# -----------------------------------------------------------------------------
# Optional legacy-context attachment
# -----------------------------------------------------------------------------


def attach_optional_legacy_context(spark: SparkSession, df: DataFrame, args: argparse.Namespace) -> DataFrame:
    if not args.attach_legacy_context:
        return df

    out = df
    ppml_path = os.path.join(args.ppml_fe_dir, "baseline_ppml_parent_fixed_effects.csv")
    ppml = read_csv_if_exists(spark, ppml_path)
    if ppml is not None and {"cluster_id", "N", "fe_raw", "fe_centered"}.issubset(ppml.columns):
        ppml = ppml.select(
            F.col("cluster_id").cast("string").alias("_legacy_parent_id"),
            F.col("N").cast("double").alias("legacy_founder_parent_ppml_N"),
            F.col("fe_raw").cast("double").alias("legacy_founder_parent_ppml_fe_raw"),
            F.col("fe_centered").cast("double").alias("legacy_founder_parent_ppml_fe_centered"),
        )
        out = out.join(
            F.broadcast(ppml),
            F.col("founder_parent_rcid").cast("string") == F.col("_legacy_parent_id"),
            "left",
        ).drop("_legacy_parent_id")
    else:
        log("[WARN] PPML context unavailable or schema incompatible; skipped.")

    growth_path = os.path.join(args.growth_dir, "tables", "combined_growth_productivity.csv")
    growth = read_csv_if_exists(spark, growth_path)
    needed = {"cluster_label", "cluster_id", "growth_window", "growth_log_points", "growth_slope_per_year"}
    if growth is not None and needed.issubset(growth.columns):
        for label, id_col, prefix in [
            ("metro", "founder_metro_area", "legacy_metro_growth"),
            ("metro_parent", "founder_parent_metro_key", "legacy_metro_parent_growth"),
        ]:
            g = growth.filter(F.col("cluster_label") == label)
            wide = g.groupBy(F.col("cluster_id").cast("string").alias("_legacy_growth_id")).agg(
                F.max(F.when(F.col("growth_window") == "early_0_25", F.col("growth_log_points").cast("double"))).alias(f"{prefix}_early_growth_log_points"),
                F.max(F.when(F.col("growth_window") == "late_26_50", F.col("growth_log_points").cast("double"))).alias(f"{prefix}_late_growth_log_points"),
                F.max(F.when(F.col("growth_window") == "early_0_25", F.col("growth_slope_per_year").cast("double"))).alias(f"{prefix}_early_growth_slope"),
                F.max(F.when(F.col("growth_window") == "late_26_50", F.col("growth_slope_per_year").cast("double"))).alias(f"{prefix}_late_growth_slope"),
            )
            out = out.join(
                F.broadcast(wide),
                F.col(id_col).cast("string") == F.col("_legacy_growth_id"),
                "left",
            ).drop("_legacy_growth_id")
    else:
        log("[WARN] Growth context unavailable or schema incompatible; skipped.")

    return out


# -----------------------------------------------------------------------------
# Lightweight V3 schema preflight
# -----------------------------------------------------------------------------


def run_preflight(spark: SparkSession, args: argparse.Namespace) -> None:
    """Validate the exact V3 inputs and shard layout without a full data scan."""
    log("[MODE] preflight")
    probe_shard = 0
    paths = {
        "raw_step3_shard": shard_path(args.spell_shards_dir, probe_shard),
        "annual_step4_shard": shard_path(args.annual_shards_dir, probe_shard),
        "annual_step5_panel": args.annual_panel_dir,
        "company_reference": args.company_ref_dir,
    }
    for label, path in paths.items():
        if not os.path.exists(path):
            raise FileNotFoundError(f"{label}: missing path {path}")

    raw_spell = spark.read.parquet(paths["raw_step3_shard"])
    annual_shard = spark.read.parquet(paths["annual_step4_shard"])
    annual_panel = spark.read.parquet(paths["annual_step5_panel"])
    company = spark.read.parquet(paths["company_reference"])

    require_columns(raw_spell, ["user_id", "position_id", "title_raw", "company_name", "rcid"], "V3 Step 3 spell shard")
    require_columns(annual_shard, ["user_id", "year", "current_position_id", "current_rcid"], "V3 Step 4 annual shard")
    require_columns(annual_panel, ["user_id", "year", "current_rcid", "n_patents", "n_us_citations"], "V3 Step 5 annual panel")
    require_columns(company, ["rcid", "ultimate_parent_rcid"], "company reference")

    log("[PREFLIGHT PASS] Founder V3 inputs and required columns are present.")
    for label, frame in [
        ("V3 Step 3 spell shard", raw_spell),
        ("V3 Step 4 annual shard", annual_shard),
        ("V3 Step 5 annual panel", annual_panel),
        ("company reference", company),
    ]:
        log(f"[PREFLIGHT] {label} columns={len(frame.columns)}")


# -----------------------------------------------------------------------------
# Modes
# -----------------------------------------------------------------------------


def run_firm_year(spark: SparkSession, args: argparse.Namespace) -> None:
    log("[MODE] firm_year")
    annual = read_standard_annual(spark, args.annual_panel_dir)
    firm_year = build_firm_year_outcomes(annual)
    assert_unique(firm_year, ["firm_rcid", "year"], "firm-year outcomes")
    write_parquet(
        firm_year,
        os.path.join(args.out_dir, "firm_year_outcomes_current_rcid_v3"),
        args.coalesce,
        args.overwrite,
    )


def run_founder_shard(spark: SparkSession, args: argparse.Namespace) -> None:
    if args.shard_idx is None or not (0 <= args.shard_idx < args.shards):
        raise ValueError("founder_shard requires --shard-idx in [0, --shards).")

    log(f"[MODE] founder_shard | shard={args.shard_idx:02d}/{args.shards - 1:02d}")
    raw_path = shard_path(args.spell_shards_dir, args.shard_idx)
    annual_path = shard_path(args.annual_shards_dir, args.shard_idx)

    spells = read_standard_spells(spark, raw_path, args.company_ref_dir)
    candidates, links, unresolved = build_founder_links(spells, args.link_definition)

    annual = read_standard_annual(spark, annual_path)
    profile = build_user_profile_context(annual)
    links = links.join(profile, "user_id", "left")
    links = links.withColumn(
        "founder_parent_metro_key",
        F.when(
            F.col("founder_parent_rcid").isNotNull() & F.col("founder_metro_area").isNotNull(),
            F.concat_ws("__", F.col("founder_parent_rcid").cast("string"), F.col("founder_metro_area")),
        ),
    ).withColumn(
        "founder_onet_major2", F.regexp_extract(F.col("founder_onet_code"), r"^([0-9]{2})", 1)
    ).withColumn(
        "founder_stem_at_founder", F.col("founder_onet_major2").isin("15", "17", "19").cast("int")
    )

    pre = build_pre_founder_characteristics(annual, links)
    firm_year_path = os.path.join(args.out_dir, "firm_year_outcomes_current_rcid_v3")
    if not os.path.exists(firm_year_path):
        raise FileNotFoundError(
            f"Missing {firm_year_path}. Run --mode firm_year before founder_shard."
        )
    firm_year = spark.read.parquet(firm_year_path)
    post = build_post_founder_outcomes(links, firm_year)

    final = links.join(pre, ["user_id", "founded_rcid"], "left").join(post, ["user_id", "founded_rcid"], "left")
    pre_zero = [
        "prior_inventor_year_rows", "prior_years_observed", "prior_patents", "prior_applications",
        "prior_first_inventor_patents", "prior_us_citations", "prior_unique_current_rcids",
        "prior_unique_parent_rcids", "prior_unique_onet_codes", "prior_unique_naics6",
    ]
    for c in pre_zero:
        if c in final.columns:
            final = final.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
    final = final.withColumn("founder_prior_patenter", (F.col("prior_patents") > 0).cast("int")) \
                 .withColumn("founder_prior_cited", (F.col("prior_us_citations") > 0).cast("int"))
    final = attach_optional_legacy_context(spark, final, args)

    suffix = f"shard={args.shard_idx:02d}"
    write_parquet(candidates, os.path.join(args.out_dir, "founder_raw_spell_candidates_shards", suffix), args.coalesce, args.overwrite)
    write_parquet(links, os.path.join(args.out_dir, "founder_company_links_shards", suffix), args.coalesce, args.overwrite)
    write_parquet(unresolved, os.path.join(args.out_dir, "founder_unresolved_spells_shards", suffix), args.coalesce, args.overwrite)
    write_parquet(pre, os.path.join(args.out_dir, "founder_pre_characteristics_shards", suffix), args.coalesce, args.overwrite)
    write_parquet(final, os.path.join(args.out_dir, "founder_firm_outcomes_shards", suffix), args.coalesce, args.overwrite)


def require_all_shards(root: str, shards: int) -> None:
    missing = [shard_path(root, i) for i in range(shards) if not os.path.exists(shard_path(root, i))]
    if missing:
        raise FileNotFoundError("Missing founder shard outputs:\n" + "\n".join(missing[:20]))


def run_merge_founder_shards(spark: SparkSession, args: argparse.Namespace) -> None:
    log("[MODE] merge_founder_shards")
    table_roots = [
        ("founder_raw_spell_candidates_shards", "founder_raw_spell_candidates_v3", None),
        ("founder_company_links_shards", "founder_company_links_v3", ["user_id", "founded_rcid"]),
        ("founder_unresolved_spells_shards", "founder_unresolved_spells_v3", ["user_id", "position_id"]),
        ("founder_pre_characteristics_shards", "founder_pre_characteristics_v3", ["user_id", "founded_rcid"]),
        ("founder_firm_outcomes_shards", "founder_firm_outcomes_v3", ["user_id", "founded_rcid"]),
    ]

    merged: Dict[str, DataFrame] = {}
    for shard_name, output_name, keys in table_roots:
        root = os.path.join(args.out_dir, shard_name)
        require_all_shards(root, args.shards)
        df = spark.read.parquet(root)
        if keys:
            assert_unique(df, keys, output_name)
        write_parquet(df, os.path.join(args.out_dir, output_name), args.coalesce, args.overwrite)
        merged[output_name] = df

    diag_rows = [
        ("raw_founder_candidate_spells", merged["founder_raw_spell_candidates_v3"].count()),
        ("resolved_founder_links", merged["founder_company_links_v3"].count()),
        ("resolved_founder_users", merged["founder_company_links_v3"].select("user_id").distinct().count()),
        ("unresolved_founder_spells", merged["founder_unresolved_spells_v3"].count()),
        ("final_founder_firm_rows", merged["founder_firm_outcomes_v3"].count()),
    ]
    diagnostics = spark.createDataFrame(diag_rows, ["stat", "value"])
    write_csv(diagnostics, os.path.join(args.out_dir, "diagnostics", "sample_counts.csv"), args.overwrite)


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    spark = make_spark(args)
    try:
        log("=" * 100)
        log(f"V3 founder pipeline | mode={args.mode}")
        log(f"out_dir={args.out_dir}")
        log(f"link_definition={args.link_definition}")
        log("=" * 100)
        if args.mode == "preflight":
            run_preflight(spark, args)
        elif args.mode == "firm_year":
            run_firm_year(spark, args)
        elif args.mode == "founder_shard":
            run_founder_shard(spark, args)
        elif args.mode == "merge_founder_shards":
            run_merge_founder_shards(spark, args)
        else:
            raise ValueError(f"Unsupported mode: {args.mode}")
        log("[DONE] Founder V3 stage completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
