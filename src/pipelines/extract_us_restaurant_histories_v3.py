#!/usr/bin/env python3
"""
extract_us_restaurant_histories_v3.py

All-population (not inventor-only) LinkedIn extraction for people who ever
worked at a US restaurant, plus a core restaurant-founder subset.

Selection is from raw academic_individual_position records:
  US restaurant spell = US country field AND NAICS begins with 7225.
  Core restaurant founder = founder/co-founder/founding partner in that same
  US restaurant spell. Founding employees/engineers/team/advisors are retained
  as a separate broad-founder flag.

Modes
-----
preflight
    Validate raw position, company-reference, and user schemas before any full scan.
select_restaurant_population
    Scan raw positions once, write US restaurant spells, establishments,
    restaurant people, and founder subsets.
enrich_people
    Attach raw LinkedIn user profile information to the selected people.
extract_full_histories
    Scan raw positions once more and extract every career spell worldwide for
    the selected people. Each person's complete history is kept in one stable
    hash batch (career_batch). Use --history-scope us_only only when desired.

The restaurant selection is US-only. Full histories default to global careers
for selected people, because that is normally what is needed to study prior and
subsequent jobs; the selection itself remains strictly US restaurant based.
"""

from __future__ import annotations

import argparse
import os
from typing import Dict, Sequence

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


DEFAULT_POSITIONS = "/labs/khanna/linkedin_202507/academic_individual_position"
DEFAULT_USERS = "/labs/khanna/linkedin_202507/academic_individual_user"
DEFAULT_COMPANY_REF = "/labs/khanna/linkedin_202507/academic_company_ref"
DEFAULT_OUT = "/home/epiga/revelio_labs/output/us_restaurant_histories_v3"


# -----------------------------------------------------------------------------
# Arguments and generic helpers
# -----------------------------------------------------------------------------


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Extract US restaurant worker and founder career histories.")
    ap.add_argument(
        "--mode",
        required=True,
        choices=["preflight", "select_restaurant_population", "enrich_people", "extract_full_histories"],
    )
    ap.add_argument("--positions-dir", default=DEFAULT_POSITIONS)
    ap.add_argument("--users-dir", default=DEFAULT_USERS)
    ap.add_argument("--company-ref-dir", default=DEFAULT_COMPANY_REF)
    ap.add_argument("--out-dir", default=DEFAULT_OUT)
    ap.add_argument(
        "--restaurant-naics-prefix",
        default="7225",
        help="NAICS prefix. 7225 captures 7225, 72251, 722511, etc.",
    )
    ap.add_argument(
        "--history-scope",
        choices=["all", "us_only"],
        default="all",
        help="all = complete global history for selected people; us_only = their US spells only.",
    )
    ap.add_argument("--n-user-batches", type=int, default=512)
    ap.add_argument("--threads", type=int, default=48)
    ap.add_argument("--shuffle-partitions", type=int, default=3200)
    ap.add_argument("--coalesce", type=int, default=256)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument("--preflight-rows", type=int, default=1000,
                    help="Rows used only for the lightweight schema smoke test.")
    ap.add_argument("--overwrite", action="store_true")
    return ap.parse_args()


def log(message: str) -> None:
    print(message, flush=True)


def make_spark(args: argparse.Namespace) -> SparkSession:
    builder = (
        SparkSession.builder.appName(f"us_restaurant_histories_v3_{args.mode}")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
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


def require_columns(df: DataFrame, cols: Sequence[str], label: str) -> None:
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise RuntimeError(f"{label}: missing required fields {missing}")


def select_schema(df: DataFrame, fields: Dict[str, str]) -> DataFrame:
    return df.select(
        *[
            F.col(name).cast(dtype).alias(name) if name in df.columns else F.lit(None).cast(dtype).alias(name)
            for name, dtype in fields.items()
        ]
    )


def clean_text_expr(name: str):
    x = F.trim(F.col(name).cast("string"))
    return (
        F.when(x.isNull() | (x == ""), F.lit(None).cast("string"))
        .when(F.lower(x).isin("null", "none", "nan", "n/a", "na"), F.lit(None).cast("string"))
        .otherwise(x)
    )


def output_mode(overwrite: bool) -> str:
    return "overwrite" if overwrite else "errorifexists"


def write_parquet(df: DataFrame, path: str, coalesce: int, overwrite: bool) -> None:
    log(f"[WRITE] {path} (mode={output_mode(overwrite)})")
    df.coalesce(max(1, coalesce)).write.mode(output_mode(overwrite)).option("compression", "snappy").parquet(path)


def write_csv(df: DataFrame, path: str, overwrite: bool) -> None:
    df.coalesce(1).write.mode(output_mode(overwrite)).option("header", "true").csv(path)


# -----------------------------------------------------------------------------
# Position standardization and raw-title flags
# -----------------------------------------------------------------------------


POSITION_FIELDS: Dict[str, str] = {
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

CORE_FOUNDER_RX = r"(^|[^a-z])(?:co[\s-]*founder|cofounder|founder|founding\s+partner)([^a-z]|$)"
COFOUNDER_RX = r"(^|[^a-z])(?:co[\s-]*founder|cofounder)([^a-z]|$)"
FOUNDING_EMPLOYEE_RX = (
    r"(^|[^a-z])(?:founding\s+engineer|founding\s+scientist|founding\s+member|"
    r"founding\s+team|founding\s+employee|founding\s+developer|founding\s+designer|"
    r"founding\s+advisor)([^a-z]|$)"
)



def first_existing(columns: Sequence[str], candidates: Sequence[str]):
    """Return the first schema-matching name, or None."""
    available = set(columns)
    for name in candidates:
        if name in available:
            return name
    return None


def source_expr(raw: DataFrame, candidates: Sequence[str], alias: str, dtype: str):
    source = first_existing(raw.columns, candidates)
    if source is None:
        return F.lit(None).cast(dtype).alias(alias), None
    return F.col(source).cast(dtype).alias(alias), source


def read_company_reference(spark: SparkSession, path: str) -> DataFrame:
    """Return a one-row-per-RCID company lookup, including NAICS fields.

    The raw position table may contain no NAICS variables. In that usual case,
    NAICS is supplied by academic_company_ref. Field aliases are resolved
    explicitly and logged; a missing NAICS mapping is a hard error upstream.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(f"Company reference directory is missing: {path}")

    raw = spark.read.parquet(path)
    if "rcid" not in raw.columns:
        raise RuntimeError("company reference: required field rcid is missing")

    specs = [
        (["ultimate_parent_rcid", "parent_rcid"], "parent_from_ref", "long"),
        (["ultimate_parent_company_name", "parent_company_name"], "parent_name_from_ref", "string"),
        (["naics_code", "naics"], "ref_naics_code", "string"),
        (["naics_description", "naics_desc", "naics_name"], "ref_naics_description", "string"),
        (["naics6", "naics_6"], "ref_naics6", "string"),
        (["naics4", "naics_4"], "ref_naics4", "string"),
        (["naics3", "naics_3"], "ref_naics3", "string"),
        (["naics2", "naics_2"], "ref_naics2", "string"),
    ]
    exprs = [F.col("rcid").cast("long").alias("rcid_ref")]
    source_log = []
    for candidates, alias, dtype in specs:
        expr, source = source_expr(raw, candidates, alias, dtype)
        exprs.append(expr)
        source_log.append(f"{alias} <- {source or 'MISSING'}")
    log("[COMPANY REF FIELD MAP] " + "; ".join(source_log))

    selected = raw.select(*exprs)
    return selected.groupBy("rcid_ref").agg(
        F.max("parent_from_ref").alias("parent_from_ref"),
        F.max("parent_name_from_ref").alias("parent_name_from_ref"),
        F.max("ref_naics_code").alias("ref_naics_code"),
        F.max("ref_naics_description").alias("ref_naics_description"),
        F.max("ref_naics6").alias("ref_naics6"),
        F.max("ref_naics4").alias("ref_naics4"),
        F.max("ref_naics3").alias("ref_naics3"),
        F.max("ref_naics2").alias("ref_naics2"),
    )


def read_standard_positions(spark: SparkSession, args: argparse.Namespace) -> DataFrame:
    log(f"[LOAD RAW POSITIONS] {args.positions_dir}")
    raw = spark.read.parquet(args.positions_dir)
    require_columns(raw, ["user_id", "position_id", "company_name", "title_raw", "rcid"], "raw positions")

    country_source = first_existing(raw.columns, ["country", "country_code", "country_name"])
    if country_source is None:
        raise RuntimeError(
            "raw positions: no country field among country/country_code/country_name. "
            "Cannot construct a US-only restaurant sample safely."
        )
    log(f"[POSITION FIELD MAP] country <- {country_source}")
    if country_source != "country":
        raw = raw.withColumn("country", F.col(country_source))

    pos = select_schema(raw, POSITION_FIELDS)
    for c in [
        "user_id", "company_name", "ultimate_parent_company_name", "title_raw", "title_translated",
        "city", "state", "country", "region", "metro_area", "msa", "location_raw",
        "naics_code", "naics_description", "naics6", "naics4", "naics3", "naics2",
        "job_category_v2", "mapped_role_v3", "role_k150_v2", "role_k150_v3", "onet_code", "onet_title",
    ]:
        pos = pos.withColumn(c, clean_text_expr(c))

    ref = read_company_reference(spark, args.company_ref_dir)
    pos = pos.join(ref, pos["rcid"] == ref["rcid_ref"], "left")

    # The company lookup is authoritative for NAICS when the raw spell lacks it.
    pos = (
        pos
        .withColumn("naics_code", F.coalesce(F.col("naics_code"), F.col("ref_naics_code"), F.col("naics6"), F.col("ref_naics6")))
        .withColumn("naics_description", F.coalesce(F.col("naics_description"), F.col("ref_naics_description")))
        .withColumn("naics6", F.coalesce(F.col("naics6"), F.col("ref_naics6")))
        .withColumn("naics4", F.coalesce(F.col("naics4"), F.col("ref_naics4")))
        .withColumn("naics3", F.coalesce(F.col("naics3"), F.col("ref_naics3")))
        .withColumn("naics2", F.coalesce(F.col("naics2"), F.col("ref_naics2")))
        .withColumn("resolved_parent_rcid", F.coalesce(F.col("ultimate_parent_rcid"), F.col("parent_from_ref"), F.col("rcid")))
        .withColumn(
            "resolved_parent_company_name",
            F.coalesce(
                F.col("ultimate_parent_company_name"),
                F.col("parent_name_from_ref"),
                F.when(F.col("resolved_parent_rcid").eqNullSafe(F.col("rcid")), F.col("company_name")),
            ),
        )
        .drop(
            "rcid_ref", "parent_from_ref", "parent_name_from_ref",
            "ref_naics_code", "ref_naics_description", "ref_naics6", "ref_naics4", "ref_naics3", "ref_naics2",
        )
    )

    # Never silently produce an empty restaurant sample merely because NAICS was absent.
    naics_columns_available = any(
        first_existing(raw.columns, candidates) is not None
        for candidates in (["naics_code", "naics"], ["naics6", "naics_6"], ["naics4", "naics_4"], ["naics3", "naics_3"])
    ) or any(
        first_existing(spark.read.parquet(args.company_ref_dir).columns, candidates) is not None
        for candidates in (["naics_code", "naics"], ["naics6", "naics_6"], ["naics4", "naics_4"], ["naics3", "naics_3"])
    )
    if not naics_columns_available:
        raise RuntimeError(
            "Neither raw positions nor academic_company_ref has a recognized NAICS field. "
            "Stopping rather than writing a meaningless empty restaurant sample."
        )
    return pos


def run_preflight(spark: SparkSession, args: argparse.Namespace) -> None:
    """Cheap metadata-only validation before launching multihour full scans."""
    log("[MODE] preflight")
    pos_raw = spark.read.parquet(args.positions_dir)
    ref_raw = spark.read.parquet(args.company_ref_dir)
    users_raw = spark.read.parquet(args.users_dir)

    required_pos = ["user_id", "position_id", "company_name", "title_raw", "rcid"]
    require_columns(pos_raw, required_pos, "raw positions")
    require_columns(ref_raw, ["rcid"], "company reference")
    require_columns(users_raw, ["user_id"], "raw users")

    country_source = first_existing(pos_raw.columns, ["country", "country_code", "country_name"])
    pos_naics = first_existing(pos_raw.columns, ["naics_code", "naics", "naics6", "naics_6", "naics4", "naics_4", "naics3", "naics_3"])
    ref_naics = first_existing(ref_raw.columns, ["naics_code", "naics", "naics6", "naics_6", "naics4", "naics_4", "naics3", "naics_3"])
    if country_source is None:
        raise RuntimeError("No usable position-country field; cannot enforce US-only selection.")
    if pos_naics is None and ref_naics is None:
        raise RuntimeError("No usable NAICS field in positions or company reference.")

    profile_fields = [c for c in ["firstname", "first_name", "firstName", "lastname", "last_name", "lastName", "fullname", "full_name", "name", "country", "user_country", "location", "user_location", "location_raw", "profile_title", "headline", "title"] if c in users_raw.columns]
    log("[PREFLIGHT PASS] Required inputs are present.")
    log(f"[PREFLIGHT] position country source: {country_source}")
    log(f"[PREFLIGHT] position NAICS source: {pos_naics or 'not present'}")
    log(f"[PREFLIGHT] company-reference NAICS source: {ref_naics or 'not present'}")
    log(f"[PREFLIGHT] raw user profile fields available: {profile_fields}")
    log(f"[PREFLIGHT] raw position column count: {len(pos_raw.columns)}")
    log(f"[PREFLIGHT] company reference column count: {len(ref_raw.columns)}")
    log(f"[PREFLIGHT] raw user column count: {len(users_raw.columns)}")

def is_us_country(name: str):
    x = F.upper(F.trim(F.col(name).cast("string")))
    return x.isin("US", "USA", "U.S.", "U.S.A.", "UNITED STATES", "UNITED STATES OF AMERICA")


def add_restaurant_and_founder_flags(pos: DataFrame, args: argparse.Namespace) -> DataFrame:
    digits = F.regexp_extract(F.coalesce(F.col("naics_code"), F.lit("")), r"([0-9]+)", 1)
    title = F.lower(F.concat_ws(" ", F.coalesce(F.col("title_raw"), F.lit("")), F.coalesce(F.col("title_translated"), F.lit(""))))
    title = F.trim(F.regexp_replace(F.regexp_replace(title, r"[/,&|()\[\]{}:;]+", " "), r"\s+", " "))
    return (
        pos.withColumn("naics_digits", digits)
        .withColumn("is_us_position", is_us_country("country").cast("int"))
        .withColumn("is_restaurant_naics", F.col("naics_digits").startswith(args.restaurant_naics_prefix).cast("int"))
        .withColumn("is_us_restaurant_spell", ((F.col("is_us_position") == 1) & (F.col("is_restaurant_naics") == 1)).cast("int"))
        .withColumn("_title_norm", title)
        .withColumn("restaurant_core_founder_flag", F.col("_title_norm").rlike(CORE_FOUNDER_RX).cast("int"))
        .withColumn("restaurant_cofounder_flag", F.col("_title_norm").rlike(COFOUNDER_RX).cast("int"))
        .withColumn("restaurant_founding_employee_flag", F.col("_title_norm").rlike(FOUNDING_EMPLOYEE_RX).cast("int"))
        .withColumn(
            "restaurant_broad_founder_flag",
            F.greatest(F.col("restaurant_core_founder_flag"), F.col("restaurant_founding_employee_flag")),
        )
        .drop("_title_norm")
    )


# -----------------------------------------------------------------------------
# Selection outputs
# -----------------------------------------------------------------------------


def build_restaurant_people(restaurants: DataFrame) -> DataFrame:
    first_w = Window.partitionBy("user_id").orderBy(F.col("startdate").asc_nulls_last(), F.col("position_id").asc_nulls_last())
    last_w = Window.partitionBy("user_id").orderBy(F.col("enddate").desc_nulls_last(), F.col("position_id").desc_nulls_last())

    first = (
        restaurants.withColumn("_rn", F.row_number().over(first_w)).filter(F.col("_rn") == 1)
        .select(
            "user_id",
            F.col("position_id").alias("first_us_restaurant_position_id"),
            F.col("startdate").alias("first_us_restaurant_startdate"),
            F.col("company_name").alias("first_us_restaurant_company_name"),
            F.col("rcid").alias("first_us_restaurant_rcid"),
            F.col("city").alias("first_us_restaurant_city"),
            F.col("state").alias("first_us_restaurant_state"),
            F.col("naics_code").alias("first_us_restaurant_naics_code"),
        )
    )
    last = (
        restaurants.withColumn("_rn", F.row_number().over(last_w)).filter(F.col("_rn") == 1)
        .select(
            "user_id",
            F.col("position_id").alias("last_us_restaurant_position_id"),
            F.col("enddate").alias("last_us_restaurant_enddate"),
            F.col("company_name").alias("last_us_restaurant_company_name"),
            F.col("rcid").alias("last_us_restaurant_rcid"),
            F.col("city").alias("last_us_restaurant_city"),
            F.col("state").alias("last_us_restaurant_state"),
        )
    )
    agg = restaurants.groupBy("user_id").agg(
        F.countDistinct("position_id").alias("n_us_restaurant_spells"),
        F.countDistinct("rcid").alias("n_us_restaurant_rcids"),
        F.countDistinct(F.concat_ws("||", F.coalesce(F.col("company_name"), F.lit("")), F.coalesce(F.col("city"), F.lit("")), F.coalesce(F.col("state"), F.lit("")))).alias("n_us_restaurant_company_locations"),
        F.min("startdate").alias("first_us_restaurant_date_any_spell"),
        F.max("enddate").alias("last_us_restaurant_date_any_spell"),
        F.max("restaurant_core_founder_flag").alias("ever_restaurant_core_founder"),
        F.max("restaurant_cofounder_flag").alias("ever_restaurant_cofounder"),
        F.max("restaurant_founding_employee_flag").alias("ever_restaurant_founding_employee"),
        F.max("restaurant_broad_founder_flag").alias("ever_restaurant_broad_founder"),
    )
    return agg.join(first, "user_id", "left").join(last, "user_id", "left")


def run_select_restaurant_population(spark: SparkSession, args: argparse.Namespace) -> None:
    log("[MODE] select_restaurant_population")
    pos = add_restaurant_and_founder_flags(read_standard_positions(spark, args), args)
    restaurants = pos.filter(F.col("is_us_restaurant_spell") == 1)

    # Materialize raw restaurant spells once. Every later selection output reads
    # this exact raw-spell file rather than scanning the 1.7B-position source again.
    restaurant_spells_path = os.path.join(args.out_dir, "us_restaurant_spells_v3")
    write_parquet(restaurants, restaurant_spells_path, args.coalesce, args.overwrite)
    restaurants = spark.read.parquet(restaurant_spells_path)
    if restaurants.limit(1).count() == 0:
        raise RuntimeError(
            "US restaurant selection produced zero spells. Check the logged country and NAICS source mappings; "
            "the job stops here rather than reporting a false successful empty sample."
        )

    write_parquet(
        restaurants.filter(F.col("restaurant_core_founder_flag") == 1),
        os.path.join(args.out_dir, "us_restaurant_core_founder_spells_v3"),
        args.coalesce,
        args.overwrite,
    )
    write_parquet(
        restaurants.filter(F.col("restaurant_broad_founder_flag") == 1),
        os.path.join(args.out_dir, "us_restaurant_broad_founder_spells_v3"),
        args.coalesce,
        args.overwrite,
    )

    people = build_restaurant_people(restaurants)
    write_parquet(people, os.path.join(args.out_dir, "us_restaurant_people_v3"), args.coalesce, args.overwrite)

    establishments = (
        restaurants.withColumn(
            "restaurant_establishment_key",
            F.when(F.col("rcid").isNotNull(), F.concat(F.lit("rcid:"), F.col("rcid").cast("string")))
            .otherwise(
                F.concat(
                    F.lit("unresolved:"),
                    F.sha2(F.concat_ws("||", F.coalesce(F.lower(F.col("company_name")), F.lit("")), F.coalesce(F.lower(F.col("city")), F.lit("")), F.coalesce(F.lower(F.col("state")), F.lit(""))), 256),
                )
            ),
        )
        .groupBy("restaurant_establishment_key")
        .agg(
            F.max("rcid").alias("restaurant_rcid"),
            F.max("resolved_parent_rcid").alias("restaurant_parent_rcid"),
            F.min("company_name").alias("restaurant_company_name"),
            F.min("resolved_parent_company_name").alias("restaurant_parent_company_name"),
            F.min("city").alias("restaurant_city"),
            F.min("state").alias("restaurant_state"),
            F.min("metro_area").alias("restaurant_metro_area"),
            F.min("naics_code").alias("restaurant_naics_code"),
            F.min("naics_description").alias("restaurant_naics_description"),
            F.countDistinct("user_id").alias("n_linkedin_people"),
            F.countDistinct("position_id").alias("n_restaurant_spells"),
            F.max("restaurant_core_founder_flag").alias("has_core_founder_title"),
        )
    )
    write_parquet(establishments, os.path.join(args.out_dir, "us_restaurant_establishments_v3"), args.coalesce, args.overwrite)

    counts = spark.createDataFrame([
        ("us_restaurant_spells", restaurants.count()),
        ("us_restaurant_people", people.count()),
        ("us_restaurant_core_founder_spells", restaurants.filter(F.col("restaurant_core_founder_flag") == 1).count()),
        ("us_restaurant_core_founder_people", people.filter(F.col("ever_restaurant_core_founder") == 1).count()),
    ], ["stat", "value"])
    write_csv(counts, os.path.join(args.out_dir, "diagnostics", "selection_counts.csv"), args.overwrite)


# -----------------------------------------------------------------------------
# User profiles and full career histories
# -----------------------------------------------------------------------------


def user_field(raw: DataFrame, candidates: Sequence[str], alias: str):
    for name in candidates:
        if name in raw.columns:
            return clean_text_expr(name).alias(alias)
    return F.lit(None).cast("string").alias(alias)


def run_enrich_people(spark: SparkSession, args: argparse.Namespace) -> None:
    log("[MODE] enrich_people")
    people_path = os.path.join(args.out_dir, "us_restaurant_people_v3")
    if not os.path.exists(people_path):
        raise FileNotFoundError(f"Missing selection output: {people_path}")
    people = spark.read.parquet(people_path)

    log(f"[LOAD USERS] {args.users_dir}")
    raw_users = spark.read.parquet(args.users_dir)
    require_columns(raw_users, ["user_id"], "raw users")
    users = raw_users.select(
        F.col("user_id").cast("string").alias("user_id"),
        user_field(raw_users, ["firstname", "first_name", "firstName"], "user_firstname"),
        user_field(raw_users, ["lastname", "last_name", "lastName"], "user_lastname"),
        user_field(raw_users, ["fullname", "full_name", "name"], "user_fullname"),
        user_field(raw_users, ["country", "user_country"], "user_country"),
        user_field(raw_users, ["location", "user_location", "location_raw"], "user_location"),
        user_field(raw_users, ["profile_title", "headline", "title"], "user_profile_title"),
    ).dropDuplicates(["user_id"])

    enriched = people.join(users, "user_id", "left")
    write_parquet(enriched, os.path.join(args.out_dir, "us_restaurant_people_enriched_v3"), args.coalesce, args.overwrite)


def run_extract_full_histories(spark: SparkSession, args: argparse.Namespace) -> None:
    log("[MODE] extract_full_histories")
    people_path = os.path.join(args.out_dir, "us_restaurant_people_v3")
    if not os.path.exists(people_path):
        raise FileNotFoundError(f"Missing selection output: {people_path}")

    selected = (
        spark.read.parquet(people_path)
        .select(
            F.col("user_id").cast("string").alias("user_id"),
            "ever_restaurant_core_founder",
            "ever_restaurant_broad_founder",
            "n_us_restaurant_spells",
        )
    )
    pos = add_restaurant_and_founder_flags(read_standard_positions(spark, args), args)

    histories = pos.join(selected, "user_id", "left_semi")
    if args.history_scope == "us_only":
        histories = histories.filter(F.col("is_us_position") == 1)

    histories = (
        histories.join(selected, "user_id", "left")
        .withColumn("career_batch", F.pmod(F.abs(F.hash(F.col("user_id"))), F.lit(args.n_user_batches)))
    )

    output = os.path.join(args.out_dir, "us_restaurant_full_histories_v3")
    mode = output_mode(args.overwrite)
    log(f"[WRITE] {output} partitioned by career_batch (mode={mode})")
    (
        histories.repartition(args.n_user_batches, "career_batch")
        .write.mode(mode)
        .option("compression", "snappy")
        .partitionBy("career_batch")
        .parquet(output)
    )


def main() -> None:
    args = parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    spark = make_spark(args)
    try:
        log("=" * 100)
        log(f"US restaurant histories | mode={args.mode}")
        log(f"out_dir={args.out_dir}")
        log(f"restaurant_naics_prefix={args.restaurant_naics_prefix}")
        log(f"history_scope={args.history_scope}")
        log("=" * 100)
        if args.mode == "preflight":
            run_preflight(spark, args)
        elif args.mode == "select_restaurant_population":
            run_select_restaurant_population(spark, args)
        elif args.mode == "enrich_people":
            run_enrich_people(spark, args)
        elif args.mode == "extract_full_histories":
            run_extract_full_histories(spark, args)
        else:
            raise ValueError(args.mode)
        log("[DONE] Restaurant stage completed.")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
