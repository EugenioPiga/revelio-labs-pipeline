#!/usr/bin/env python3
###############################################################################
# step4_inventor_year.py
#
# Build INVENTOR-YEAR panel using the new Step1/2/3 outputs.
#
# Inputs:
#   STEP1:
#     patent matches + au_* demographics/profile variables.
#
#   STEP2:
#     inventor-position data, now including occupation, O*NET, NAICS, RICS,
#     remote_suitability, seniority, salary, firm, and geography variables.
#
#   STEP3:
#     education summaries attached to Step2 rows.
#
#   USPTO aux:
#     precomputed parquet:
#       us_citations_by_patent: patent_id, n_us_citations
#       cpc_main: patent_id, cpc_section_main, cpc_class_main
#
# Output:
#   /labs/khanna/linkedin_202507/processed/inventor_year_shards_v2/shard=XX
#
# What this does:
#   1. Loads education/user variables from Step3.
#   2. Loads au_* profile variables from Step1.
#   3. Builds patent-year aggregates and CPC modal fields.
#   4. Builds position-year aggregates from Step2.
#   5. Adds current occupation/O*NET/NAICS/RICS measures based on main position.
#   6. Forward-fills firm/location/occupation/industry variables to continuous years.
#   7. Merges patent-year, position-year, education, and user variables.
#   8. Creates immigrant-status measures.
#   9. Writes one inventor-year shard.
###############################################################################

import os
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


VALID_YEAR_MIN = 1950
VALID_YEAR_MAX = 2025


# =============================================================================
# Utilities
# =============================================================================

def shard_filter(df, shard_idx: int, shards: int, user_col: str = "user_id"):
    """Deterministic sharding by hash(user_id) % shards."""
    df = df.withColumn(user_col, F.col(user_col).cast("string"))
    sh = F.pmod(F.abs(F.hash(F.col(user_col))), F.lit(shards))
    return df.withColumn("_shard", sh).filter(F.col("_shard") == shard_idx).drop("_shard")


def check_dupes(df, label, n=5, do_heavy=False):
    """Safe debug helper. Heavy checks are optional."""
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")

    try:
        df.limit(n).show(n, truncate=False)
    except Exception as e:
        print(f"[WARN] sample show failed: {e}")

    if not do_heavy:
        print("[INFO] Skipping full count/dupe scan because do_heavy=False.")
        return

    total = df.count()
    print(f"[INFO] Total rows: {total:,}")

    if "year" in df.columns:
        dupes = df.groupBy("user_id", "year").count().filter(F.col("count") > 1)
    else:
        dupes = df.groupBy("user_id").count().filter(F.col("count") > 1)

    n_dupes = dupes.count()
    print(f"[CHECK] duplicates: {n_dupes:,}")
    if n_dupes > 0:
        dupes.show(n, truncate=False)


def add_missing(df, colname, dtype):
    """Add a NULL column if missing."""
    if colname not in df.columns:
        df = df.withColumn(colname, F.lit(None).cast(dtype))
    return df


def clean_empty_string(colname):
    """Turn empty / 'empty' strings into NULL."""
    return (
        F.when(
            F.col(colname).isNull()
            | (F.trim(F.col(colname)) == "")
            | (F.lower(F.trim(F.col(colname))) == "empty"),
            F.lit(None).cast("string")
        )
        .otherwise(F.col(colname))
    )


# =============================================================================
# Args
# =============================================================================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, required=True)

    ap.add_argument("--step1-dir", required=True)
    ap.add_argument("--step2-dir", required=True)
    ap.add_argument("--step3-dir", required=True)

    ap.add_argument("--company-ref", default="/labs/khanna/linkedin_202507/academic_company_ref")

    ap.add_argument(
        "--uspto-aux-dir",
        required=True,
        help="Directory containing us_citations_by_patent/ and cpc_main/ parquet outputs"
    )

    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--tmpdir", default=None)

    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")))
    ap.add_argument("--shuffle-partitions", type=int, default=1200)
    ap.add_argument("--coalesce", type=int, default=8)

    return ap.parse_args()


# =============================================================================
# Main
# =============================================================================

def main():
    args = parse_args()

    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards - 1}]")

    # -------------------------------------------------------------------------
    # Spark session
    # -------------------------------------------------------------------------

    builder = (
        SparkSession.builder
        .appName(f"inventor_year_v2_shard_{args.shard_idx}_of_{args.shards}")
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
    spark.sparkContext.setLogLevel("INFO")

    print("=" * 90)
    print("[STEP4] Build inventor-year v2 shard")
    print(f"[CONF] shard={args.shard_idx}/{args.shards - 1}")
    print(f"[CONF] step1-dir={args.step1_dir}")
    print(f"[CONF] step2-dir={args.step2_dir}")
    print(f"[CONF] step3-dir={args.step3_dir}")
    print(f"[CONF] uspto-aux-dir={args.uspto_aux_dir}")
    print(f"[CONF] out-dir={args.out_dir}")
    print("=" * 90)

    checkpoint_dir = "/labs/khanna/linkedin_202507/scratch/spark_checkpoints_v2"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    # =========================================================================
    # [1/7] USER-LEVEL VARIABLES FROM STEP3 + DEMOGRAPHICS FROM STEP1
    # =========================================================================

    print("[1/7] Loading user-level vars from STEP3 ...")

    s3 = (
        spark.read
             .option("recursiveFileLookup", "true")
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.step3_dir)
    )

    s3 = shard_filter(s3, args.shard_idx, args.shards, user_col="user_id")

    s3_cols = s3.columns
    edu_cols = [c for c in s3_cols if c.startswith("edu_first_") or c.startswith("edu_last_")]

    base_user_cols = ["user_id"]
    for c in ["has_education", "n_educations"]:
        if c in s3_cols:
            base_user_cols.append(c)

    user_from_s3 = (
        s3.select(*list(dict.fromkeys(base_user_cols + edu_cols)))
          .dropDuplicates(["user_id"])
          .cache()
    )

    print(f"      user_from_s3 cols: {len(user_from_s3.columns)}")

    print("[1b/7] Loading demographics au_* from STEP1 ...")

    s1 = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.step1_dir)
    )

    s1 = shard_filter(s1, args.shard_idx, args.shards, user_col="user_id")

    s1_cols = s1.columns
    au_cols = [c for c in s1_cols if c.startswith("au_")]

    user_from_s1 = (
        s1.select("user_id", *au_cols)
          .dropDuplicates(["user_id"])
          .cache()
    )

    print(f"      user_from_s1 au_* cols: {len(user_from_s1.columns)}")

    user_vars = user_from_s3.join(user_from_s1, on="user_id", how="left").cache()

    # Old-style aliases for compatibility with existing scripts.
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

    # =========================================================================
    # [2/7] PATENT SIDE
    # =========================================================================

    print("[2/7] Building patent-side aggregates ...")

    pat = (
        s1.select("user_id", "patent_id", "application_id", "filing_date", "inventor_sequence")
          .dropna(subset=["user_id", "patent_id"])
    )

    pat = pat.withColumn(
        "filing_date",
        F.when(
            (F.col("filing_date") < F.to_date(F.lit(f"{VALID_YEAR_MIN}-01-01")))
            | (F.col("filing_date") > F.to_date(F.lit(f"{VALID_YEAR_MAX}-12-31"))),
            F.lit(None)
        ).otherwise(F.col("filing_date"))
    )

    us_cit_parq = os.path.join(args.uspto_aux_dir, "us_citations_by_patent")
    cpc_parq = os.path.join(args.uspto_aux_dir, "cpc_main")

    us_cit = spark.read.parquet(us_cit_parq)
    cpc_main = spark.read.parquet(cpc_parq)

    pat = pat.join(us_cit, on="patent_id", how="left").fillna({"n_us_citations": 0})
    pat = pat.join(cpc_main, on="patent_id", how="left")

    pat = (
        pat.withColumn("year", F.year("filing_date").cast("int"))
           .filter(F.col("year").isNotNull())
    )

    section_counts = (
        pat.filter(F.col("cpc_section_main").isNotNull())
           .groupBy("user_id", "year", "cpc_section_main")
           .agg(F.countDistinct("patent_id").alias("n_patents_in_section"))
    )

    w_sec = Window.partitionBy("user_id", "year").orderBy(
        F.col("n_patents_in_section").desc(),
        F.rand(seed=123)
    )

    modal_section_y = (
        section_counts.withColumn("rn", F.row_number().over(w_sec))
                      .filter(F.col("rn") == 1)
                      .select("user_id", "year", F.col("cpc_section_main").alias("modal_section_y"))
    )

    class_counts = (
        pat.filter(F.col("cpc_class_main").isNotNull())
           .groupBy("user_id", "year", "cpc_class_main")
           .agg(F.countDistinct("patent_id").alias("n_patents_in_class"))
    )

    w_cls = Window.partitionBy("user_id", "year").orderBy(
        F.col("n_patents_in_class").desc(),
        F.rand(seed=456)
    )

    modal_class_y = (
        class_counts.withColumn("rn", F.row_number().over(w_cls))
                    .filter(F.col("rn") == 1)
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

    check_dupes(pat_year, "After patent aggregation", do_heavy=False)

    # Career modal patent fields.
    career_section_counts = (
        pat.filter(F.col("cpc_section_main").isNotNull())
           .groupBy("user_id", "cpc_section_main")
           .agg(F.countDistinct("patent_id").alias("n_patents_section_total"))
    )

    w_csec = Window.partitionBy("user_id").orderBy(
        F.col("n_patents_section_total").desc(),
        F.rand(seed=789)
    )

    career_modal_section = (
        career_section_counts.withColumn("rn", F.row_number().over(w_csec))
                             .filter(F.col("rn") == 1)
                             .select("user_id", F.col("cpc_section_main").alias("career_modal_section"))
    )

    career_class_counts = (
        pat.filter(F.col("cpc_class_main").isNotNull())
           .groupBy("user_id", "cpc_class_main")
           .agg(F.countDistinct("patent_id").alias("n_patents_class_total"))
    )

    w_ccls = Window.partitionBy("user_id").orderBy(
        F.col("n_patents_class_total").desc(),
        F.rand(seed=987)
    )

    career_modal_class = (
        career_class_counts.withColumn("rn", F.row_number().over(w_ccls))
                           .filter(F.col("rn") == 1)
                           .select("user_id", F.col("cpc_class_main").alias("career_modal_class"))
    )

    # =========================================================================
    # [3/7] POSITION SIDE
    # =========================================================================

    print("[3/7] Building position-side aggregates from STEP2 ...")

    s2 = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.step2_dir)
    )

    s2 = shard_filter(s2, args.shard_idx, args.shards, user_col="user_id")

    # -------------------------------------------------------------------------
    # Repair ultimate_parent_rcid using company reference.
    # -------------------------------------------------------------------------

    company_ref = (
        spark.read
             .parquet(args.company_ref)
             .select(
                 F.col("rcid").cast("long").alias("rcid_ref"),
                 F.col("ultimate_parent_rcid").cast("long").alias("parent_from_ref")
             )
             .dropDuplicates(["rcid_ref"])
    )

    if "rcid" in s2.columns:
        s2 = s2.withColumn("rcid", F.col("rcid").cast("long"))
    else:
        s2 = s2.withColumn("rcid", F.lit(None).cast("long"))

    if "ultimate_parent_rcid" in s2.columns:
        s2 = s2.withColumn("ultimate_parent_rcid", F.col("ultimate_parent_rcid").cast("long"))
    else:
        s2 = s2.withColumn("ultimate_parent_rcid", F.lit(None).cast("long"))

    s2 = (
        s2.join(company_ref, s2["rcid"] == company_ref["rcid_ref"], how="left")
          .withColumn(
              "ultimate_parent_rcid",
              F.coalesce(
                  F.col("ultimate_parent_rcid"),
                  F.col("parent_from_ref"),
                  F.col("rcid")
              )
          )
          .drop("rcid_ref", "parent_from_ref")
    )

    desired_pos_cols = [
        # Keys / employer
        "user_id",
        "position_id",
        "rcid",
        "company_name",
        "ultimate_parent_rcid",
        "ultimate_parent_company_name",

        # Dates
        "startdate",
        "enddate",
        "position_number",

        # Compensation / hierarchy
        "salary",
        "start_salary",
        "end_salary",
        "total_compensation",
        "additional_compensation",
        "seniority",

        # Geography
        "city",
        "state",
        "country",
        "region",
        "metro_area",
        "msa",
        "location_raw",

        # Occupation / title
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

        # O*NET
        "onet_code",
        "onet_title",

        # Industry
        "naics_code",
        "naics_description",
        "rics_k50",
        "rics_k200",
        "rics_k400",

        # Job/task measure
        "remote_suitability",
        "weight",
    ]

    keep_pos_cols = [c for c in desired_pos_cols if c in s2.columns]
    pos = s2.select(*keep_pos_cols).dropna(subset=["user_id", "position_id"])

    # Defensive missing columns.
    string_cols = [
        "company_name", "ultimate_parent_company_name",
        "title_raw", "title_translated", "job_category_v2",
        "role_k50_v2", "role_k150_v2", "role_k300_v2", "role_k500_v2",
        "role_k1000_v2", "role_k1500_v2",
        "role_k10_v3", "role_k50_v3", "role_k150_v3", "role_k500_v3",
        "role_k1000_v3", "role_k1500_v3", "role_k5000_v3",
        "role_k10000_v3", "role_k15000_v3", "mapped_role_v3",
        "onet_code", "onet_title",
        "naics_code", "naics_description",
        "rics_k50", "rics_k200", "rics_k400",
        "city", "state", "country", "region", "metro_area", "msa", "location_raw",
    ]

    numeric_cols = [
        "salary", "start_salary", "end_salary", "total_compensation",
        "additional_compensation", "seniority", "remote_suitability", "weight",
    ]

    long_cols = [
        "position_id", "rcid", "ultimate_parent_rcid", "position_number",
    ]

    date_cols = ["startdate", "enddate"]

    for c in string_cols:
        pos = add_missing(pos, c, "string")

    for c in numeric_cols:
        pos = add_missing(pos, c, "double")

    for c in long_cols:
        pos = add_missing(pos, c, "long")

    for c in date_cols:
        pos = add_missing(pos, c, "date")

    # Normalize types.
    pos = (
        pos.withColumn("position_id", F.col("position_id").cast("long"))
           .withColumn("rcid", F.col("rcid").cast("long"))
           .withColumn("ultimate_parent_rcid", F.col("ultimate_parent_rcid").cast("long"))
           .withColumn("position_number", F.col("position_number").cast("int"))
           .withColumn("salary", F.col("salary").cast("double"))
           .withColumn("start_salary", F.col("start_salary").cast("double"))
           .withColumn("end_salary", F.col("end_salary").cast("double"))
           .withColumn("total_compensation", F.col("total_compensation").cast("double"))
           .withColumn("additional_compensation", F.col("additional_compensation").cast("double"))
           .withColumn("seniority", F.col("seniority").cast("double"))
           .withColumn("remote_suitability", F.col("remote_suitability").cast("double"))
           .withColumn("weight", F.col("weight").cast("double"))
           .withColumn("startdate", F.to_date("startdate"))
           .withColumn("enddate", F.to_date("enddate"))
    )

    # Clean strings.
    for c in string_cols:
        if c in pos.columns:
            pos = pos.withColumn(c, clean_empty_string(c))

    # Derived O*NET / NAICS variables.
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

    # First position start date per user.
    first_pos = (
        pos.filter(F.col("startdate").isNotNull())
           .groupBy("user_id")
           .agg(F.min("startdate").alias("first_startdate_pos"))
    )

    # Deduplicate raw spells.
    spell_keys = [
        c for c in ["user_id", "position_id", "startdate", "enddate", "rcid"]
        if c in pos.columns
    ]
    pos = pos.dropDuplicates(spell_keys)

    # Expand spells to years.
    pos = (
        pos.withColumn("start_year", F.year("startdate").cast("int"))
           .withColumn("end_year", F.year("enddate").cast("int"))
    )

    pos = pos.withColumn(
        "end_year",
        F.when(F.col("end_year").isNull(), F.col("start_year"))
         .otherwise(F.col("end_year"))
    )

    pos = pos.filter(
        F.col("user_id").isNotNull()
        & F.col("position_id").isNotNull()
        & F.col("start_year").isNotNull()
        & (F.col("start_year") >= VALID_YEAR_MIN)
        & (F.col("start_year") <= VALID_YEAR_MAX)
    )

    pos = (
        pos.withColumn(
            "end_year",
            F.when(F.col("end_year") < F.col("start_year"), F.col("start_year"))
             .otherwise(F.col("end_year"))
        )
        .withColumn(
            "end_year",
            F.when(F.col("end_year") > VALID_YEAR_MAX, F.lit(VALID_YEAR_MAX))
             .otherwise(F.col("end_year"))
        )
    )

    pos_expanded = pos.withColumn(
        "year",
        F.explode(F.sequence(F.col("start_year"), F.col("end_year")))
    )

    pos_expanded = pos_expanded.filter(
        (F.col("year") >= VALID_YEAR_MIN) & (F.col("year") <= VALID_YEAR_MAX)
    )

    # Main position per user-year.
    year_start = F.to_date(F.concat(F.col("year").cast("string"), F.lit("-01-01")))
    year_end = F.to_date(F.concat(F.col("year").cast("string"), F.lit("-12-31")))

    pos_expanded = (
        pos_expanded
        .withColumn("year_start", year_start)
        .withColumn("year_end", year_end)
        .withColumn("spell_start_in_year", F.greatest(F.col("startdate"), F.col("year_start")))
        .withColumn(
            "spell_end_in_year",
            F.least(F.coalesce(F.col("enddate"), F.col("startdate")), F.col("year_end"))
        )
        .withColumn(
            "overlap_days",
            F.greatest(
                F.datediff(F.col("spell_end_in_year"), F.col("spell_start_in_year")) + F.lit(1),
                F.lit(1)
            )
        )
    )

    w_main = (
        Window.partitionBy("user_id", "year")
        .orderBy(
            F.col("overlap_days").desc_nulls_last(),
            F.col("salary").desc_nulls_last(),
            F.col("total_compensation").desc_nulls_last(),
            F.col("seniority").desc_nulls_last(),
            F.col("startdate").desc_nulls_last(),
            F.col("position_id").desc_nulls_last(),
        )
    )

    main_pos = (
        pos_expanded
        .withColumn("_rn_main_pos", F.row_number().over(w_main))
        .filter(F.col("_rn_main_pos") == 1)
        .drop("_rn_main_pos")
    )

    main_pos_year = main_pos.select(
        "user_id", "year",

        F.col("position_id").alias("current_position_id"),
        F.col("rcid").alias("current_rcid"),
        F.col("ultimate_parent_rcid").alias("current_parent_rcid"),
        F.col("company_name").alias("current_company_name"),
        F.col("ultimate_parent_company_name").alias("current_parent_company_name"),

        F.col("title_raw").alias("current_title_raw"),
        F.col("title_translated").alias("current_title_translated"),
        F.col("job_category_v2").alias("current_job_category_v2"),

        F.col("role_k50_v2").alias("current_role_k50_v2"),
        F.col("role_k150_v2").alias("current_role_k150_v2"),
        F.col("role_k300_v2").alias("current_role_k300_v2"),
        F.col("role_k500_v2").alias("current_role_k500_v2"),
        F.col("role_k1000_v2").alias("current_role_k1000_v2"),
        F.col("role_k1500_v2").alias("current_role_k1500_v2"),

        F.col("role_k10_v3").alias("current_role_k10_v3"),
        F.col("role_k50_v3").alias("current_role_k50_v3"),
        F.col("role_k150_v3").alias("current_role_k150_v3"),
        F.col("role_k500_v3").alias("current_role_k500_v3"),
        F.col("role_k1000_v3").alias("current_role_k1000_v3"),
        F.col("role_k1500_v3").alias("current_role_k1500_v3"),
        F.col("role_k5000_v3").alias("current_role_k5000_v3"),
        F.col("role_k10000_v3").alias("current_role_k10000_v3"),
        F.col("role_k15000_v3").alias("current_role_k15000_v3"),
        F.col("mapped_role_v3").alias("current_mapped_role_v3"),

        F.col("onet_code_clean").alias("current_onet_code"),
        F.col("onet_title").alias("current_onet_title"),
        F.col("onet_soc6").alias("current_onet_soc6"),
        F.col("onet_major").alias("current_onet_major"),
        F.col("onet_minor").alias("current_onet_minor"),

        F.col("naics_code_clean").alias("current_naics_code"),
        F.col("naics_description").alias("current_naics_description"),
        F.col("naics6").alias("current_naics6"),
        F.col("naics4").alias("current_naics4"),
        F.col("naics3").alias("current_naics3"),
        F.col("naics2").alias("current_naics2"),
        F.col("rics_k50").alias("current_rics_k50"),
        F.col("rics_k200").alias("current_rics_k200"),
        F.col("rics_k400").alias("current_rics_k400"),

        F.col("remote_suitability").alias("current_remote_suitability"),
        F.col("weight").alias("current_position_weight"),
        F.col("seniority").alias("current_seniority"),
        F.col("salary").alias("current_salary"),
        F.col("total_compensation").alias("current_total_compensation"),
        F.col("position_number").alias("current_position_number"),
        F.col("overlap_days").alias("current_position_overlap_days"),
    )

    # Position-year aggregates.
    pos_year_base = (
        pos_expanded.groupBy("user_id", "year")
        .agg(
            F.countDistinct("position_id").alias("n_positions"),
            F.countDistinct("rcid").alias("n_unique_companies"),
            F.countDistinct("ultimate_parent_rcid").alias("n_unique_parents"),
            F.countDistinct("onet_code_clean").alias("n_unique_onet_codes"),
            F.countDistinct("role_k150_v2").alias("n_unique_role_k150_v2"),
            F.countDistinct("naics6").alias("n_unique_naics6"),
            F.countDistinct("naics3").alias("n_unique_naics3"),

            F.first("position_id", ignorenulls=True).alias("first_position_id"),

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

            F.first("city", ignorenulls=True).alias("first_city"),
            F.last("city", ignorenulls=True).alias("last_city"),
            F.first("state", ignorenulls=True).alias("first_state"),
            F.last("state", ignorenulls=True).alias("last_state"),
            F.first("country", ignorenulls=True).alias("first_country"),
            F.last("country", ignorenulls=True).alias("last_country"),
            F.first("region", ignorenulls=True).alias("first_region"),
            F.last("region", ignorenulls=True).alias("last_region"),
            F.first("metro_area", ignorenulls=True).alias("first_metro_area"),
            F.last("metro_area", ignorenulls=True).alias("last_metro_area"),
            F.first("location_raw", ignorenulls=True).alias("first_location_raw"),
            F.last("location_raw", ignorenulls=True).alias("last_location_raw"),

            F.first("rcid", ignorenulls=True).alias("first_rcid"),
            F.last("rcid", ignorenulls=True).alias("last_rcid"),
            F.first("ultimate_parent_rcid", ignorenulls=True).alias("first_parent_rcid"),
            F.last("ultimate_parent_rcid", ignorenulls=True).alias("last_parent_rcid"),
        )
    )

    pos_year = (
        pos_year_base
        .join(main_pos_year, on=["user_id", "year"], how="left")
        .withColumn("position_measures_observed", F.lit(1))
    )

    pos_year = (
        pos_year.groupBy("user_id", "year")
                .agg(*[
                    F.first(c, ignorenulls=True).alias(c)
                    for c in pos_year.columns if c not in ["user_id", "year"]
                ])
    )

    check_dupes(pos_year, "After position-year aggregation", do_heavy=False)

    # -------------------------------------------------------------------------
    # Forward-fill to continuous inventor-year panel.
    # -------------------------------------------------------------------------

    pos_year = pos_year.withColumn("pos_obs", F.lit(1))

    max_pat_year = pat_year.select(F.max("year")).collect()[0][0]
    max_pos_year = pos_year.select(F.max("year")).collect()[0][0]
    years = [y for y in [max_pat_year, max_pos_year] if y is not None]

    if len(years) == 0:
        print("[WARN] No valid years in this shard. Skipping shard.")
        spark.stop()
        return

    global_max_year = int(max(years))
    print(f"[INFO] Global maximum year detected in shard: {global_max_year}")

    year_bounds = (
        pos_year.groupBy("user_id")
                .agg(F.min("year").alias("min_year"))
                .filter(F.col("min_year").isNotNull())
                .withColumn("max_year", F.lit(global_max_year))
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
            F.when(F.col("pos_obs").isNull(), F.lit(1)).otherwise(F.lit(0))
        )
        .drop("pos_obs")
    )

    pos_year_full = (
        pos_year_full.groupBy("user_id", "year")
                    .agg(*[
                        F.first(c, ignorenulls=True).alias(c)
                        for c in pos_year_full.columns if c not in ["user_id", "year"]
                    ])
    )

    w_ff = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)

    cols_to_fill = [
        # Original firm/location fields
        "first_rcid", "first_city", "first_parent_rcid", "last_parent_rcid",
        "first_state", "first_country", "first_region", "first_metro_area",
        "first_location_raw", "last_rcid", "last_city", "last_state",
        "last_country", "last_region", "last_metro_area", "last_location_raw",
        "first_position_id",

        # New current occupation / O*NET / NAICS / RICS fields
        "current_position_id",
        "current_rcid",
        "current_parent_rcid",
        "current_company_name",
        "current_parent_company_name",
        "current_title_raw",
        "current_title_translated",
        "current_job_category_v2",

        "current_role_k50_v2",
        "current_role_k150_v2",
        "current_role_k300_v2",
        "current_role_k500_v2",
        "current_role_k1000_v2",
        "current_role_k1500_v2",

        "current_role_k10_v3",
        "current_role_k50_v3",
        "current_role_k150_v3",
        "current_role_k500_v3",
        "current_role_k1000_v3",
        "current_role_k1500_v3",
        "current_role_k5000_v3",
        "current_role_k10000_v3",
        "current_role_k15000_v3",
        "current_mapped_role_v3",

        "current_onet_code",
        "current_onet_title",
        "current_onet_soc6",
        "current_onet_major",
        "current_onet_minor",

        "current_naics_code",
        "current_naics_description",
        "current_naics6",
        "current_naics4",
        "current_naics3",
        "current_naics2",
        "current_rics_k50",
        "current_rics_k200",
        "current_rics_k400",

        "current_remote_suitability",
        "current_position_weight",
        "current_seniority",
        "current_salary",
        "current_total_compensation",
        "current_position_number",
        "current_position_overlap_days",
    ]

    for c in cols_to_fill:
        if c in pos_year_full.columns:
            pos_year_full = pos_year_full.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))

    pos_year_full = pos_year_full.withColumn(
        "position_measures_observed",
        F.coalesce(F.col("position_measures_observed"), F.lit(0))
    )

    pos_year_full = pos_year_full.withColumn(
        "position_measures_forward_filled",
        F.when(
            (F.col("position_measures_observed") == 0)
            & (
                F.col("current_onet_code").isNotNull()
                | F.col("current_naics3").isNotNull()
                | F.col("current_role_k150_v2").isNotNull()
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    pos_year = pos_year_full

    # =========================================================================
    # [4/7] MERGE PATENTS + POSITIONS + USER VARS
    # =========================================================================

    print("[4/7] Merging patents + positions + user vars ...")

    pat_year = pat_year.fillna({
        "n_patents": 0,
        "n_applications": 0,
        "n_first_inventor": 0,
        "n_us_citations": 0,
    })

    inventor_year = (
        pat_year.join(pos_year, on=["user_id", "year"], how="outer")
                .join(first_pos, on="user_id", how="left")
                .join(user_vars, on="user_id", how="left")
                .join(career_modal_section, on="user_id", how="left")
                .join(career_modal_class, on="user_id", how="left")
    )

    inventor_year = inventor_year.fillna({
        "n_patents": 0,
        "n_applications": 0,
        "n_first_inventor": 0,
        "n_us_citations": 0,
        "pos_extrapolated": 0,
        "n_educations": 0,
        "position_measures_observed": 0,
        "position_measures_forward_filled": 0,
    })

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

    # =========================================================================
    # [5/7] IMMIGRANT MEASURES
    # =========================================================================

    print("[5/7] Creating immigrant measures ...")

    US = F.lit("United States")

    if "first_university_country" in inventor_year.columns:
        inventor_year = inventor_year.withColumn(
            "immig_deg_first_nonUS",
            F.when(
                F.col("first_university_country").isNotNull()
                & (F.col("first_university_country") != US),
                F.lit(1)
            ).otherwise(F.lit(0))
        )
    else:
        inventor_year = inventor_year.withColumn("immig_deg_first_nonUS", F.lit(0))

    pos_country_y = F.coalesce(F.col("last_country"), F.col("first_country"))

    w_all_ordered = (
        Window.partitionBy("user_id")
              .orderBy("year")
              .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    )

    inventor_year = inventor_year.withColumn("pos_country_y", pos_country_y)

    inventor_year = inventor_year.withColumn(
        "first_pos_country",
        F.first("pos_country_y", ignorenulls=True).over(w_all_ordered)
    )

    inventor_year = inventor_year.withColumn(
        "immig_job_first_nonUS",
        F.when(
            F.col("first_pos_country").isNotNull()
            & (F.col("first_pos_country") != US),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    inventor_year = inventor_year.withColumn(
        "immig_first_deg_or_job_nonUS",
        F.when(
            (F.col("immig_deg_first_nonUS") == 1)
            | (F.col("immig_job_first_nonUS") == 1),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    inventor_year = inventor_year.drop("pos_country_y", "first_pos_country")

    w_cum = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    w_all = Window.partitionBy("user_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    uni_c = F.col("first_university_country")

    m1_event = F.when(
        uni_c.isNotNull()
        & F.col("first_country").isNotNull()
        & (F.col("first_country") != uni_c),
        F.lit(1)
    ).otherwise(F.lit(0))

    inventor_year = inventor_year.withColumn("immig1_event", m1_event)
    inventor_year = inventor_year.withColumn("immig1_post", F.max("immig1_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig1_ever", F.max("immig1_event").over(w_all))

    pos_country_y = F.coalesce(F.col("last_country"), F.col("first_country"))

    within_year_change = F.when(
        F.col("first_country").isNotNull()
        & F.col("last_country").isNotNull()
        & (F.col("first_country") != F.col("last_country")),
        F.lit(1)
    ).otherwise(F.lit(0))

    lag_country = F.lag(pos_country_y).over(Window.partitionBy("user_id").orderBy("year"))

    across_year_change = F.when(
        pos_country_y.isNotNull()
        & lag_country.isNotNull()
        & (pos_country_y != lag_country),
        F.lit(1)
    ).otherwise(F.lit(0))

    m2_event = F.when(
        (within_year_change == 1) | (across_year_change == 1),
        F.lit(1)
    ).otherwise(F.lit(0))

    inventor_year = inventor_year.withColumn("immig2_event", m2_event)
    inventor_year = inventor_year.withColumn("immig2_post", F.max("immig2_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig2_ever", F.max("immig2_event").over(w_all))
    inventor_year = inventor_year.drop("immig1_event", "immig2_event")

    # =========================================================================
    # [6/7] BACKFILL MODAL PATENT FIELDS
    # =========================================================================

    print("[6/7] Backfilling modal patent fields ...")

    w_future = Window.partitionBy("user_id").orderBy("year").rowsBetween(0, Window.unboundedFollowing)
    w_past = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)

    for v in ["modal_section_y", "modal_class_y"]:
        if v in inventor_year.columns:
            inventor_year = inventor_year.withColumn(
                f"{v}_next_patent",
                F.first(v, ignorenulls=True).over(w_future)
            )
            inventor_year = inventor_year.withColumn(
                f"{v}_next_patent",
                F.last(f"{v}_next_patent", ignorenulls=True).over(w_past)
            )
            inventor_year = inventor_year.drop(v).withColumnRenamed(f"{v}_next_patent", v)

    # =========================================================================
    # [7/7] WRITE OUTPUT SHARD
    # =========================================================================

    print("[7/7] Writing output shard ...")

    inventor_year = inventor_year.checkpoint(eager=True)
    inventor_year = inventor_year.coalesce(args.coalesce)

    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    os.makedirs(out_path, exist_ok=True)

    print(f"[WRITE] -> {out_path}")

    (
        inventor_year.write
        .mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    print(f"[INFO] Finished shard {args.shard_idx:02d}")
    spark.stop()


if __name__ == "__main__":
    main()
