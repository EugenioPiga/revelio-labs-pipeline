#!/usr/bin/env python3
"""
step4_inventor_year_v3.py

Build the spell-safe inventor-year panel.

Core semantic contract
----------------------
1. current_* variables always come from one exact raw position spell selected
   inside that calendar year. They are never forward-filled.
2. A position spell with a null company_name or rcid remains null. It never
   borrows those values from another spell held by the same person.
3. last_known_* variables are a separate, explicit career-state bundle:
   the most recently observed selected spell, copied atomically into years with
   no observed position.
4. first_* and last_* are deterministic first/last spell attributes within the
   calendar year; they are not forward-filled.
5. Open-ended positions are expanded through --panel-max-year, rather than only
   through their start year.

The output preserves the patent-side and education/profile variables used by
the prior V2 panel, while making employer/title/position provenance explicit.
"""

import argparse
import os
from functools import reduce

from pyspark.sql import SparkSession, Window, functions as F


VALID_YEAR_MIN = 1950


# (source column, output suffix, Spark type)
CURRENT_SPELL_SPECS = [
    ("position_id", "position_id", "long"),
    ("rcid", "rcid", "long"),
    ("ultimate_parent_rcid", "parent_rcid", "long"),
    ("company_name", "company_name", "string"),
    ("ultimate_parent_company_name", "parent_company_name", "string"),
    ("title_raw", "title_raw", "string"),
    ("title_translated", "title_translated", "string"),
    ("job_category_v2", "job_category_v2", "string"),

    ("city", "city", "string"),
    ("state", "state", "string"),
    ("country", "country", "string"),
    ("region", "region", "string"),
    ("metro_area", "metro_area", "string"),
    ("msa", "msa", "string"),
    ("location_raw", "location_raw", "string"),

    ("role_k50_v2", "role_k50_v2", "string"),
    ("role_k150_v2", "role_k150_v2", "string"),
    ("role_k300_v2", "role_k300_v2", "string"),
    ("role_k500_v2", "role_k500_v2", "string"),
    ("role_k1000_v2", "role_k1000_v2", "string"),
    ("role_k1500_v2", "role_k1500_v2", "string"),

    ("role_k10_v3", "role_k10_v3", "string"),
    ("role_k50_v3", "role_k50_v3", "string"),
    ("role_k150_v3", "role_k150_v3", "string"),
    ("role_k500_v3", "role_k500_v3", "string"),
    ("role_k1000_v3", "role_k1000_v3", "string"),
    ("role_k1500_v3", "role_k1500_v3", "string"),
    ("role_k5000_v3", "role_k5000_v3", "string"),
    ("role_k10000_v3", "role_k10000_v3", "string"),
    ("role_k15000_v3", "role_k15000_v3", "string"),
    ("mapped_role_v3", "mapped_role_v3", "string"),

    ("onet_code_clean", "onet_code", "string"),
    ("onet_title", "onet_title", "string"),
    ("onet_soc6", "onet_soc6", "string"),
    ("onet_major", "onet_major", "string"),
    ("onet_minor", "onet_minor", "string"),

    ("naics_code_clean", "naics_code", "string"),
    ("naics_description", "naics_description", "string"),
    ("naics6", "naics6", "string"),
    ("naics4", "naics4", "string"),
    ("naics3", "naics3", "string"),
    ("naics2", "naics2", "string"),
    ("rics_k50", "rics_k50", "string"),
    ("rics_k200", "rics_k200", "string"),
    ("rics_k400", "rics_k400", "string"),

    ("remote_suitability", "remote_suitability", "double"),
    ("weight", "position_weight", "double"),
    ("seniority", "seniority", "double"),
    ("salary", "salary", "double"),
    ("total_compensation", "total_compensation", "double"),
    ("position_number", "position_number", "double"),
    ("overlap_days", "position_overlap_days", "int"),
]

YEAR_EDGE_SPECS = [
    ("position_id", "position_id", "long"),
    ("rcid", "rcid", "long"),
    ("ultimate_parent_rcid", "parent_rcid", "long"),
    ("company_name", "company_name", "string"),
    ("city", "city", "string"),
    ("state", "state", "string"),
    ("country", "country", "string"),
    ("region", "region", "string"),
    ("metro_area", "metro_area", "string"),
    ("location_raw", "location_raw", "string"),
]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, required=True)

    ap.add_argument("--step1-dir", required=True)
    ap.add_argument("--step2-dir", required=True)
    ap.add_argument("--step3-dir", required=True)
    ap.add_argument(
        "--company-ref",
        default="/labs/khanna/linkedin_202507/academic_company_ref",
    )
    ap.add_argument("--uspto-aux-dir", required=True)
    ap.add_argument("--out-dir", required=True)

    ap.add_argument("--panel-max-year", type=int, default=2025)
    ap.add_argument(
        "--main-position-rule",
        choices=["year_end", "year_start", "longest_overlap"],
        default="year_end",
        help=(
            "Definition of current_* within a year. year_end selects the final "
            "spell in that calendar year; year_start selects the initial spell; "
            "longest_overlap reproduces the old main-job logic."
        ),
    )

    ap.add_argument("--tmpdir", default=None)
    ap.add_argument(
        "--threads",
        type=int,
        default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")),
    )
    ap.add_argument("--shuffle-partitions", type=int, default=1200)
    ap.add_argument("--coalesce", type=int, default=8)
    ap.add_argument(
        "--skip-integrity-check",
        action="store_true",
        help="Skip the exact current-spell provenance assertion. Not recommended.",
    )
    return ap.parse_args()


def shard_filter(df, shard_idx, shards, user_col="user_id"):
    df = df.withColumn(user_col, F.col(user_col).cast("string"))
    expr = F.pmod(F.abs(F.hash(F.col(user_col))), F.lit(shards))
    return df.withColumn("_shard", expr).filter(F.col("_shard") == shard_idx).drop("_shard")


def shard_or_root(base, shard_idx):
    candidate = os.path.join(base, f"shard={shard_idx:02d}")
    return candidate if os.path.isdir(candidate) else base


def add_missing(df, colname, dtype):
    if colname not in df.columns:
        return df.withColumn(colname, F.lit(None).cast(dtype))
    return df


def clean_empty_string(colname):
    return F.when(
        F.col(colname).isNull()
        | (F.trim(F.col(colname)) == "")
        | (F.lower(F.trim(F.col(colname))) == "empty"),
        F.lit(None).cast("string"),
    ).otherwise(F.col(colname))


def assert_unique_keys(df, keys, label):
    bad = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .limit(5)
    )
    if bad.count() > 0:
        print(f"[ERROR] Duplicate keys in {label}: {keys}")
        bad.show(truncate=False)
        raise RuntimeError(f"Duplicate keys in {label}: {keys}")


def select_prefixed(df, specs, prefix):
    return [
        F.col(source).alias(f"{prefix}{suffix}")
        for source, suffix, _ in specs
    ]


def main_position_window(rule):
    base = Window.partitionBy("user_id", "year")

    if rule == "year_end":
        return base.orderBy(
            F.col("active_at_year_end").desc(),
            F.col("spell_end_in_year").desc_nulls_last(),
            F.col("spell_start_in_year").desc_nulls_last(),
            F.col("position_number").desc_nulls_last(),
            F.col("position_id").desc_nulls_last(),
        )

    if rule == "year_start":
        return base.orderBy(
            F.col("active_at_year_start").desc(),
            F.col("spell_start_in_year").asc_nulls_last(),
            F.col("spell_end_in_year").asc_nulls_last(),
            F.col("position_number").asc_nulls_last(),
            F.col("position_id").asc_nulls_last(),
        )

    return base.orderBy(
        F.col("overlap_days").desc_nulls_last(),
        F.col("salary").desc_nulls_last(),
        F.col("total_compensation").desc_nulls_last(),
        F.col("seniority").desc_nulls_last(),
        F.col("spell_start_in_year").desc_nulls_last(),
        F.col("position_id").desc_nulls_last(),
    )


def validate_current_spell_integrity(pos_year_full, main_pos_year):
    """Assert that current_* fields equal the fields from the selected raw spell."""
    integrity_cols = [
        "current_position_id",
        "current_rcid",
        "current_parent_rcid",
        "current_company_name",
        "current_parent_company_name",
        "current_title_raw",
        "current_title_translated",
        "current_city",
        "current_state",
        "current_country",
        "current_metro_area",
        "current_naics3",
        "current_mapped_role_v3",
        "current_seniority",
    ]

    reference = main_pos_year.select(
        "user_id",
        "year",
        *[F.col(c).alias(f"ref_{c}") for c in integrity_cols],
    )

    actual = (
        pos_year_full
        .filter(F.col("current_position_id").isNotNull())
        .select("user_id", "year", *integrity_cols)
        .join(reference, on=["user_id", "year"], how="left")
    )

    mismatch = None
    for c in integrity_cols:
        condition = ~F.col(c).eqNullSafe(F.col(f"ref_{c}"))
        mismatch = condition if mismatch is None else (mismatch | condition)

    bad = actual.filter(mismatch).limit(10)
    if bad.count() > 0:
        print("[ERROR] current_* fields do not match the selected raw spell.")
        bad.show(truncate=False)
        raise RuntimeError("Current-spell provenance integrity check failed.")

    print("[CHECK] Current-spell provenance: PASSED.")


def main():
    args = parse_args()
    if not (0 <= args.shard_idx < args.shards):
        raise SystemExit(f"--shard-idx must be in [0, {args.shards - 1}]")

    if args.panel_max_year < VALID_YEAR_MIN:
        raise SystemExit("--panel-max-year is before the valid range.")

    builder = (
        SparkSession.builder
        .appName(f"inventor_year_v3_shard_{args.shard_idx:02d}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.mergeSchema", "false")
    )

    if args.tmpdir:
        builder = (
            builder
            .config("spark.local.dir", args.tmpdir)
            .config("spark.sql.warehouse.dir", os.path.join(args.tmpdir, "warehouse"))
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    step2_path = shard_or_root(args.step2_dir, args.shard_idx)
    step3_path = shard_or_root(args.step3_dir, args.shard_idx)

    print("=" * 104)
    print(f"[STEP4 V3] Spell-safe inventor-year panel | shard={args.shard_idx:02d}/{args.shards - 1:02d}")
    print(f"[MAIN POSITION RULE] {args.main_position_rule}")
    print(f"[STEP1] {args.step1_dir}")
    print(f"[STEP2] {step2_path}")
    print(f"[STEP3] {step3_path}")
    print(f"[OUT]   {args.out_dir}")
    print("=" * 104)

    checkpoint_dir = (
        os.path.join(args.tmpdir, "checkpoints")
        if args.tmpdir
        else "/labs/khanna/linkedin_202507/scratch/spark_checkpoints_v3_spell_integrity"
    )
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    # ------------------------------------------------------------------
    # 1. User-level education/profile variables
    # ------------------------------------------------------------------
    s3 = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(step3_path)
        .withColumn("user_id", F.col("user_id").cast("string"))
    )

    s3_cols = set(s3.columns)
    edu_cols = sorted(
        c for c in s3_cols if c.startswith("edu_first_") or c.startswith("edu_last_")
    )
    user_s3_cols = ["user_id"] + [c for c in ["has_education", "n_educations"] if c in s3_cols] + edu_cols
    user_from_s3 = s3.select(*user_s3_cols).dropDuplicates(["user_id"])

    s1 = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.step1_dir)
        .filter(F.col("user_id").isNotNull())
        .withColumn("user_id", F.col("user_id").cast("string"))
    )
    s1 = shard_filter(s1, args.shard_idx, args.shards)

    au_cols = [c for c in s1.columns if c.startswith("au_")]
    user_from_s1 = s1.select("user_id", *au_cols).dropDuplicates(["user_id"])

    user_vars = user_from_s3.join(user_from_s1, on="user_id", how="left")

    rename_map = {
        "edu_first_university_name": "first_university",
        "edu_last_university_name": "last_university",
        "edu_first_degree": "first_degree",
        "edu_last_degree": "last_degree",
        "edu_first_field": "first_field",
        "edu_last_field": "last_field",
        "edu_first_university_country": "first_university_country",
        "edu_last_university_country": "last_university_country",
        "edu_first_startdate": "first_startdate_edu",
        "edu_first_enddate": "first_enddate_edu",
        "edu_last_startdate": "last_startdate_edu",
        "edu_last_enddate": "last_enddate_edu",
    }
    for src, dst in rename_map.items():
        if src in user_vars.columns and dst not in user_vars.columns:
            user_vars = user_vars.withColumn(dst, F.col(src))

    # ------------------------------------------------------------------
    # 2. Patent-side aggregates
    # ------------------------------------------------------------------
    required_pat_cols = ["user_id", "patent_id", "application_id", "filing_date", "inventor_sequence"]
    missing_pat_cols = [c for c in required_pat_cols if c not in s1.columns]
    if missing_pat_cols:
        raise ValueError(f"STEP1 is missing patent fields: {missing_pat_cols}")

    pat = (
        s1.select(*required_pat_cols)
        .dropna(subset=["user_id", "patent_id"])
        .withColumn("filing_date", F.to_date("filing_date"))
        .withColumn(
            "filing_date",
            F.when(
                (F.col("filing_date") < F.to_date(F.lit(f"{VALID_YEAR_MIN}-01-01")))
                | (F.col("filing_date") > F.to_date(F.lit(f"{args.panel_max_year}-12-31"))),
                F.lit(None),
            ).otherwise(F.col("filing_date")),
        )
        .withColumn("year", F.year("filing_date").cast("int"))
        .filter(F.col("year").isNotNull())
    )

    us_cit = spark.read.parquet(
        os.path.join(args.uspto_aux_dir, "us_citations_by_patent")
    )
    cpc_main = spark.read.parquet(
        os.path.join(args.uspto_aux_dir, "cpc_main")
    )

    pat = (
        pat.join(us_cit, on="patent_id", how="left")
        .fillna({"n_us_citations": 0})
        .join(cpc_main, on="patent_id", how="left")
    )

    section_counts = (
        pat.filter(F.col("cpc_section_main").isNotNull())
        .groupBy("user_id", "year", "cpc_section_main")
        .agg(F.countDistinct("patent_id").alias("n"))
    )
    modal_section_y = (
        section_counts
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("user_id", "year").orderBy(
                    F.col("n").desc(), F.col("cpc_section_main").asc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select("user_id", "year", F.col("cpc_section_main").alias("modal_section_y"))
    )

    class_counts = (
        pat.filter(F.col("cpc_class_main").isNotNull())
        .groupBy("user_id", "year", "cpc_class_main")
        .agg(F.countDistinct("patent_id").alias("n"))
    )
    modal_class_y = (
        class_counts
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("user_id", "year").orderBy(
                    F.col("n").desc(), F.col("cpc_class_main").asc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select("user_id", "year", F.col("cpc_class_main").alias("modal_class_y"))
    )

    pat_year = (
        pat.groupBy("user_id", "year")
        .agg(
            F.countDistinct("patent_id").alias("n_patents"),
            F.countDistinct("application_id").alias("n_applications"),
            F.sum(F.when(F.col("inventor_sequence") == "1", 1.0).otherwise(0.0)).alias("n_first_inventor"),
            F.sum(F.col("n_us_citations")).alias("n_us_citations"),
        )
        .join(modal_section_y, on=["user_id", "year"], how="left")
        .join(modal_class_y, on=["user_id", "year"], how="left")
    )
    assert_unique_keys(pat_year, ["user_id", "year"], "pat_year")

    career_section_counts = (
        pat.filter(F.col("cpc_section_main").isNotNull())
        .groupBy("user_id", "cpc_section_main")
        .agg(F.countDistinct("patent_id").alias("n"))
    )
    career_modal_section = (
        career_section_counts
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.col("n").desc(), F.col("cpc_section_main").asc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select("user_id", F.col("cpc_section_main").alias("career_modal_section"))
    )

    career_class_counts = (
        pat.filter(F.col("cpc_class_main").isNotNull())
        .groupBy("user_id", "cpc_class_main")
        .agg(F.countDistinct("patent_id").alias("n"))
    )
    career_modal_class = (
        career_class_counts
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("user_id").orderBy(
                    F.col("n").desc(), F.col("cpc_class_main").asc()
                )
            ),
        )
        .filter(F.col("_rn") == 1)
        .select("user_id", F.col("cpc_class_main").alias("career_modal_class"))
    )

    # ------------------------------------------------------------------
    # 3. Position-side panel
    # ------------------------------------------------------------------
    s2 = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(step2_path)
        .withColumn("user_id", F.col("user_id").cast("string"))
    )

    # The direct shard path is preferred. If a root was supplied, filter again.
    if step2_path == args.step2_dir:
        s2 = shard_filter(s2, args.shard_idx, args.shards)

    source_needed = [
        "user_id",
        "position_id",
        "rcid",
        "company_name",
        "ultimate_parent_rcid",
        "ultimate_parent_company_name",
        "startdate",
        "enddate",
        "position_number",
        "salary",
        "start_salary",
        "end_salary",
        "total_compensation",
        "additional_compensation",
        "seniority",
        "remote_suitability",
        "weight",
        "city",
        "state",
        "country",
        "region",
        "metro_area",
        "msa",
        "location_raw",
        "title_raw",
        "title_translated",
        "job_category_v2",
        "role_k50_v2",
        "role_k150_v2",
        "role_k300_v2",
        "role_k500_v2",
        "role_k1000_v2",
        "role_k1500_v2",
        "role_k10_v3",
        "role_k50_v3",
        "role_k150_v3",
        "role_k500_v3",
        "role_k1000_v3",
        "role_k1500_v3",
        "role_k5000_v3",
        "role_k10000_v3",
        "role_k15000_v3",
        "mapped_role_v3",
        "onet_code",
        "onet_title",
        "naics_code",
        "naics_description",
        "rics_k50",
        "rics_k200",
        "rics_k400",
    ]
    existing = [c for c in source_needed if c in s2.columns]
    pos = s2.select(*existing).dropna(subset=["user_id", "position_id"])

    # Defensive schema completion.
    string_sources = [
        "company_name", "ultimate_parent_company_name", "title_raw",
        "title_translated", "job_category_v2", "city", "state", "country",
        "region", "metro_area", "msa", "location_raw", "onet_code",
        "onet_title", "naics_code", "naics_description", "rics_k50",
        "rics_k200", "rics_k400", "role_k50_v2", "role_k150_v2",
        "role_k300_v2", "role_k500_v2", "role_k1000_v2", "role_k1500_v2",
        "role_k10_v3", "role_k50_v3", "role_k150_v3", "role_k500_v3",
        "role_k1000_v3", "role_k1500_v3", "role_k5000_v3",
        "role_k10000_v3", "role_k15000_v3", "mapped_role_v3",
    ]
    double_sources = [
        "salary", "start_salary", "end_salary", "total_compensation",
        "additional_compensation", "seniority", "remote_suitability", "weight",
        "position_number",
    ]
    long_sources = ["position_id", "rcid", "ultimate_parent_rcid"]
    date_sources = ["startdate", "enddate"]

    for c in string_sources:
        pos = add_missing(pos, c, "string")
    for c in double_sources:
        pos = add_missing(pos, c, "double")
    for c in long_sources:
        pos = add_missing(pos, c, "long")
    for c in date_sources:
        pos = add_missing(pos, c, "date")

    for c in string_sources:
        pos = pos.withColumn(c, clean_empty_string(c))

    pos = (
        pos.withColumn("position_id", F.col("position_id").cast("long"))
        .withColumn("rcid", F.col("rcid").cast("long"))
        .withColumn("ultimate_parent_rcid", F.col("ultimate_parent_rcid").cast("long"))
        .withColumn("startdate", F.to_date("startdate"))
        .withColumn("enddate", F.to_date("enddate"))
    )
    for c in double_sources:
        pos = pos.withColumn(c, F.col(c).cast("double"))

    # Resolve parent only from the same spell's non-null RCID. A null raw rcid
    # remains null, preserving the fact that this spell has no canonical firm.
    company_ref = (
        spark.read.parquet(args.company_ref)
        .select(
            F.col("rcid").cast("long").alias("rcid_ref"),
            F.col("ultimate_parent_rcid").cast("long").alias("parent_from_ref"),
        )
        .dropDuplicates(["rcid_ref"])
    )
    pos = (
        pos.join(company_ref, pos["rcid"] == company_ref["rcid_ref"], how="left")
        .withColumn(
            "ultimate_parent_rcid",
            F.coalesce(
                F.col("ultimate_parent_rcid"),
                F.col("parent_from_ref"),
                F.col("rcid"),
            ),
        )
        .drop("rcid_ref", "parent_from_ref")
    )

    pos = (
        pos.withColumn("onet_code_clean", F.col("onet_code"))
        .withColumn("onet_soc6", F.substring("onet_code_clean", 1, 7))
        .withColumn("onet_major", F.substring("onet_code_clean", 1, 2))
        .withColumn("onet_minor", F.substring("onet_code_clean", 1, 5))
        .withColumn("naics_code_clean", F.regexp_replace(F.col("naics_code"), "[^0-9]", ""))
        .withColumn("naics6", F.when(F.length("naics_code_clean") >= 6, F.substring("naics_code_clean", 1, 6)))
        .withColumn("naics4", F.when(F.length("naics_code_clean") >= 4, F.substring("naics_code_clean", 1, 4)))
        .withColumn("naics3", F.when(F.length("naics_code_clean") >= 3, F.substring("naics_code_clean", 1, 3)))
        .withColumn("naics2", F.when(F.length("naics_code_clean") >= 2, F.substring("naics_code_clean", 1, 2)))
    )

    # STEP2 V3 should already be canonical. This assertion prevents later
    # position-level cardinality changes from being silently swallowed.
    assert_unique_keys(pos, ["user_id", "position_id"], "STEP2 canonical positions")

    first_pos = (
        pos.filter(F.col("startdate").isNotNull())
        .groupBy("user_id")
        .agg(F.min("startdate").alias("first_startdate_pos"))
    )

    # Open-ended positions are observed to panel_max_year.
    pos = (
        pos.withColumn("start_year", F.year("startdate").cast("int"))
        .withColumn(
            "effective_enddate",
            F.coalesce(F.col("enddate"), F.to_date(F.lit(f"{args.panel_max_year}-12-31"))),
        )
        .withColumn("end_year", F.year("effective_enddate").cast("int"))
        .filter(
            F.col("start_year").isNotNull()
            & (F.col("start_year") >= VALID_YEAR_MIN)
            & (F.col("start_year") <= args.panel_max_year)
        )
        .withColumn(
            "end_year",
            F.when(F.col("end_year") < F.col("start_year"), F.col("start_year"))
            .otherwise(F.least(F.col("end_year"), F.lit(args.panel_max_year))),
        )
    )

    pos_expanded = (
        pos.withColumn("year", F.explode(F.sequence(F.col("start_year"), F.col("end_year"))))
        .filter((F.col("year") >= VALID_YEAR_MIN) & (F.col("year") <= args.panel_max_year))
    )

    year_start = F.to_date(F.concat(F.col("year").cast("string"), F.lit("-01-01")))
    year_end = F.to_date(F.concat(F.col("year").cast("string"), F.lit("-12-31")))

    pos_expanded = (
        pos_expanded
        .withColumn("year_start", year_start)
        .withColumn("year_end", year_end)
        .withColumn("spell_start_in_year", F.greatest(F.col("startdate"), F.col("year_start")))
        .withColumn("spell_end_in_year", F.least(F.col("effective_enddate"), F.col("year_end")))
        .withColumn(
            "overlap_days",
            F.greatest(
                F.datediff(F.col("spell_end_in_year"), F.col("spell_start_in_year")) + F.lit(1),
                F.lit(1),
            ),
        )
        .withColumn(
            "active_at_year_start",
            (
                (F.col("startdate") <= F.col("year_start"))
                & (F.col("effective_enddate") >= F.col("year_start"))
            ).cast("int"),
        )
        .withColumn(
            "active_at_year_end",
            (
                (F.col("startdate") <= F.col("year_end"))
                & (F.col("effective_enddate") >= F.col("year_end"))
            ).cast("int"),
        )
    )

    # Exact first/last spell within year, with a deterministic order.
    w_first_year = Window.partitionBy("user_id", "year").orderBy(
        F.col("spell_start_in_year").asc_nulls_last(),
        F.col("spell_end_in_year").asc_nulls_last(),
        F.col("position_number").asc_nulls_last(),
        F.col("position_id").asc_nulls_last(),
    )
    w_last_year = Window.partitionBy("user_id", "year").orderBy(
        F.col("spell_end_in_year").desc_nulls_last(),
        F.col("spell_start_in_year").desc_nulls_last(),
        F.col("position_number").desc_nulls_last(),
        F.col("position_id").desc_nulls_last(),
    )

    first_candidate = (
        pos_expanded.withColumn("_rn", F.row_number().over(w_first_year))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    last_candidate = (
        pos_expanded.withColumn("_rn", F.row_number().over(w_last_year))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    first_pos_year = first_candidate.select(
        "user_id", "year", *select_prefixed(first_candidate, YEAR_EDGE_SPECS, "first_")
    )
    last_pos_year = last_candidate.select(
        "user_id", "year", *select_prefixed(last_candidate, YEAR_EDGE_SPECS, "last_")
    )

    w_main = main_position_window(args.main_position_rule)
    main_pos = (
        pos_expanded.withColumn("_rn", F.row_number().over(w_main))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    main_pos_year = (
        main_pos.select(
            "user_id",
            "year",
            *select_prefixed(main_pos, CURRENT_SPELL_SPECS, "current_"),
        )
        .withColumn("current_position_observed", F.lit(1))
        .withColumn("current_source_rcid_missing", F.col("current_rcid").isNull().cast("int"))
        .withColumn("current_source_company_missing", F.col("current_company_name").isNull().cast("int"))
        .withColumn("current_selection_rule", F.lit(args.main_position_rule))
    )

    # These are actual all-spell statistics inside the calendar year. They are
    # distinct from current_*, which always represent one selected spell.
    pos_year_stats = (
        pos_expanded.groupBy("user_id", "year")
        .agg(
            F.countDistinct("position_id").alias("n_positions"),
            F.countDistinct("rcid").alias("n_unique_companies"),
            F.countDistinct("ultimate_parent_rcid").alias("n_unique_parents"),
            F.countDistinct("onet_code_clean").alias("n_unique_onet_codes"),
            F.countDistinct("role_k150_v2").alias("n_unique_role_k150_v2"),
            F.countDistinct("naics6").alias("n_unique_naics6"),
            F.countDistinct("naics3").alias("n_unique_naics3"),
            F.mean("salary").alias("avg_salary"),
            F.min("salary").alias("min_salary"),
            F.max("salary").alias("max_salary"),
            F.mean("total_compensation").alias("avg_total_comp"),
            F.min("total_compensation").alias("min_total_comp"),
            F.max("total_compensation").alias("max_total_comp"),
            F.mean("seniority").alias("avg_seniority"),
            F.min("seniority").alias("min_seniority"),
            F.max("seniority").alias("max_seniority"),
            F.mean("remote_suitability").alias("avg_remote_suitability"),
            F.min("remote_suitability").alias("min_remote_suitability"),
            F.max("remote_suitability").alias("max_remote_suitability"),
        )
    )

    pos_year = (
        pos_year_stats
        .join(first_pos_year, on=["user_id", "year"], how="left")
        .join(last_pos_year, on=["user_id", "year"], how="left")
        .join(main_pos_year, on=["user_id", "year"], how="left")
    )
    assert_unique_keys(pos_year, ["user_id", "year"], "observed position-year")

    # ------------------------------------------------------------------
    # Continuous panel, with a separate atomic last-known spell bundle.
    # ------------------------------------------------------------------
    year_bounds = (
        pos_year.groupBy("user_id")
        .agg(F.min("year").alias("min_year"))
        .filter(F.col("min_year").isNotNull())
        .withColumn("max_year", F.lit(args.panel_max_year))
    )
    year_seq = (
        year_bounds
        .withColumn("year", F.explode(F.sequence(F.col("min_year"), F.col("max_year"))))
        .drop("min_year", "max_year")
    )

    pos_year_full = year_seq.join(pos_year, on=["user_id", "year"], how="left")
    pos_year_full = (
        pos_year_full
        .withColumn(
            "pos_extrapolated",
            F.when(F.col("current_position_observed").isNull(), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "current_position_observed",
            F.coalesce(F.col("current_position_observed"), F.lit(0)),
        )
    )
    assert_unique_keys(pos_year_full, ["user_id", "year"], "continuous position-year")

    w_ff = (
        Window.partitionBy("user_id")
        .orderBy("year")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    bundle_fields = [
        F.col(f"current_{suffix}").alias(suffix)
        for _, suffix, _ in CURRENT_SPELL_SPECS
    ] + [F.col("year").alias("position_year")]

    pos_year_full = pos_year_full.withColumn(
        "_observed_spell_bundle",
        F.when(
            F.col("current_position_observed") == 1,
            F.struct(*bundle_fields),
        ),
    )

    pos_year_full = pos_year_full.withColumn(
        "_last_known_spell_bundle",
        F.last("_observed_spell_bundle", ignorenulls=True).over(w_ff),
    )

    for _, suffix, _ in CURRENT_SPELL_SPECS:
        pos_year_full = pos_year_full.withColumn(
            f"last_known_{suffix}",
            F.col("_last_known_spell_bundle").getField(suffix),
        )

    pos_year_full = (
        pos_year_full
        .withColumn(
            "last_known_position_year",
            F.col("_last_known_spell_bundle").getField("position_year").cast("int"),
        )
        .withColumn(
            "years_since_last_known_position",
            F.when(
                F.col("last_known_position_year").isNotNull(),
                F.col("year") - F.col("last_known_position_year"),
            ),
        )
        .withColumn(
            "position_carried_forward",
            F.when(
                (F.col("current_position_observed") == 0)
                & F.col("last_known_position_id").isNotNull(),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
        .withColumn("position_measures_observed", F.col("current_position_observed"))
        .withColumn("position_measures_forward_filled", F.col("position_carried_forward"))
        .withColumn("position_pipeline_version", F.lit("v3_spell_integrity"))
        .drop("_observed_spell_bundle", "_last_known_spell_bundle")
    )

    # current_* is intentionally NOT forward-filled. Verify it against the
    # selected main spell before any patent/user merge.
    if not args.skip_integrity_check:
        validate_current_spell_integrity(pos_year_full, main_pos_year)

    # ------------------------------------------------------------------
    # 4. Merge patent, position, and user variables
    # ------------------------------------------------------------------
    pat_year = pat_year.fillna(
        {
            "n_patents": 0,
            "n_applications": 0,
            "n_first_inventor": 0,
            "n_us_citations": 0,
        }
    )

    inventor_year = (
        pat_year.join(pos_year_full, on=["user_id", "year"], how="outer")
        .join(first_pos, on="user_id", how="left")
        .join(user_vars, on="user_id", how="left")
        .join(career_modal_section, on="user_id", how="left")
        .join(career_modal_class, on="user_id", how="left")
    )

    inventor_year = inventor_year.fillna(
        {
            "n_patents": 0,
            "n_applications": 0,
            "n_first_inventor": 0,
            "n_us_citations": 0,
            "n_educations": 0,
            "current_position_observed": 0,
            "position_measures_observed": 0,
            "position_measures_forward_filled": 0,
            "position_carried_forward": 0,
            "pos_extrapolated": 0,
        }
    )
    for c in [
        "n_positions",
        "n_unique_companies",
        "n_unique_parents",
        "n_unique_onet_codes",
        "n_unique_role_k150_v2",
        "n_unique_naics6",
        "n_unique_naics3",
    ]:
        if c in inventor_year.columns:
            inventor_year = inventor_year.fillna({c: 0})

    if "has_education" in inventor_year.columns:
        inventor_year = inventor_year.fillna({"has_education": False})
    else:
        inventor_year = inventor_year.withColumn("has_education", F.col("n_educations") > 0)

    # ------------------------------------------------------------------
    # 5. Immigration measures based on observed spell locations
    # ------------------------------------------------------------------
    us = F.lit("United States")

    if "first_university_country" in inventor_year.columns:
        inventor_year = inventor_year.withColumn(
            "immig_deg_first_nonUS",
            F.when(
                F.col("first_university_country").isNotNull()
                & (F.col("first_university_country") != us),
                F.lit(1),
            ).otherwise(F.lit(0)),
        )
    else:
        inventor_year = inventor_year.withColumn("immig_deg_first_nonUS", F.lit(0))

    # current_country is exact and null in years without an observed spell.
    inventor_year = inventor_year.withColumn("pos_country_y", F.col("current_country"))

    w_all_ordered = (
        Window.partitionBy("user_id")
        .orderBy("year")
        .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )
    w_cum = (
        Window.partitionBy("user_id")
        .orderBy("year")
        .rowsBetween(Window.unboundedPreceding, 0)
    )
    w_all = Window.partitionBy("user_id").rowsBetween(
        Window.unboundedPreceding, Window.unboundedFollowing
    )

    inventor_year = inventor_year.withColumn(
        "first_pos_country",
        F.first("pos_country_y", ignorenulls=True).over(w_all_ordered),
    )
    inventor_year = inventor_year.withColumn(
        "immig_job_first_nonUS",
        F.when(
            F.col("first_pos_country").isNotNull()
            & (F.col("first_pos_country") != us),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )
    inventor_year = inventor_year.withColumn(
        "immig_first_deg_or_job_nonUS",
        F.when(
            (F.col("immig_deg_first_nonUS") == 1)
            | (F.col("immig_job_first_nonUS") == 1),
            F.lit(1),
        ).otherwise(F.lit(0)),
    )

    m1_event = F.when(
        F.col("first_university_country").isNotNull()
        & F.col("current_country").isNotNull()
        & (F.col("current_country") != F.col("first_university_country")),
        F.lit(1),
    ).otherwise(F.lit(0))
    inventor_year = inventor_year.withColumn("immig1_event", m1_event)
    inventor_year = inventor_year.withColumn("immig1_post", F.max("immig1_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig1_ever", F.max("immig1_event").over(w_all))

    # Compare each observed country to the last observed country, even when
    # positions have a gap between the two spells.
    inventor_year = inventor_year.withColumn(
        "_last_observed_country",
        F.last("pos_country_y", ignorenulls=True).over(w_cum),
    )
    inventor_year = inventor_year.withColumn(
        "_prev_observed_country",
        F.lag("_last_observed_country").over(Window.partitionBy("user_id").orderBy("year")),
    )

    within_year_change = F.when(
        F.col("first_country").isNotNull()
        & F.col("last_country").isNotNull()
        & (F.col("first_country") != F.col("last_country")),
        F.lit(1),
    ).otherwise(F.lit(0))

    across_year_change = F.when(
        F.col("current_country").isNotNull()
        & F.col("_prev_observed_country").isNotNull()
        & (F.col("current_country") != F.col("_prev_observed_country")),
        F.lit(1),
    ).otherwise(F.lit(0))

    inventor_year = inventor_year.withColumn(
        "immig2_event",
        F.when((within_year_change == 1) | (across_year_change == 1), F.lit(1)).otherwise(F.lit(0)),
    )
    inventor_year = inventor_year.withColumn("immig2_post", F.max("immig2_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig2_ever", F.max("immig2_event").over(w_all))
    inventor_year = inventor_year.drop(
        "pos_country_y",
        "first_pos_country",
        "_last_observed_country",
        "_prev_observed_country",
        "immig1_event",
        "immig2_event",
    )

    # ------------------------------------------------------------------
    # 6. Backfill modal patent fields (unchanged semantic purpose)
    # ------------------------------------------------------------------
    w_future = (
        Window.partitionBy("user_id")
        .orderBy("year")
        .rowsBetween(0, Window.unboundedFollowing)
    )
    w_past = (
        Window.partitionBy("user_id")
        .orderBy("year")
        .rowsBetween(Window.unboundedPreceding, 0)
    )

    for v in ["modal_section_y", "modal_class_y"]:
        if v in inventor_year.columns:
            inventor_year = inventor_year.withColumn(
                f"{v}_next_patent",
                F.first(v, ignorenulls=True).over(w_future),
            )
            inventor_year = inventor_year.withColumn(
                f"{v}_next_patent",
                F.last(f"{v}_next_patent", ignorenulls=True).over(w_past),
            )
            inventor_year = inventor_year.drop(v).withColumnRenamed(
                f"{v}_next_patent", v
            )

    assert_unique_keys(inventor_year, ["user_id", "year"], "final inventor-year shard")

    # ------------------------------------------------------------------
    # 7. Write
    # ------------------------------------------------------------------
    inventor_year = inventor_year.withColumn(
        "inventor_year_pipeline_version",
        F.lit("v3_spell_integrity"),
    ).checkpoint(eager=True)

    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    (
        inventor_year.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    print(f"[DONE] Wrote {out_path}")
    spark.stop()


if __name__ == "__main__":
    main()
