#!/usr/bin/env python3
# step4_inventor_year.py
#
# Build INVENTOR-YEAR panel using the new Step1/2/3 outputs.
#
# Inputs:
#   STEP1 (patent matches + au_* demographics):
#     /labs/khanna/linkedin_202507/processed/step1_output/inventors_matched_users
#   STEP2 (positions: user_id x position_id x ...):
#     /labs/khanna/linkedin_202507/processed/step2_output/inventors_matched_positions_spark
#   STEP3 (education summaries attached to Step2 rows):
#     /labs/khanna/linkedin_202507/processed/step3_output/inventors_matched_education_spark
#   USPTO aux (PRECOMPUTED parquet, produced once):
#     <uspto-aux-dir>/us_citations_by_patent   (patent_id, n_us_citations)
#     <uspto-aux-dir>/cpc_main                (patent_id, cpc_section_main, cpc_class_main)
#   Company reference:
#     /labs/khanna/linkedin_202507/academic_company_ref
#
# Output:
#   /labs/khanna/linkedin_202507/processed/inventor_year_shards_v2/shard=XX
#
# Key design choices:
#   - TRUE sharding inside script: hash(user_id) % SHARDS
#   - Robust Spark parquet settings (read+write LEGACY, no vectorized reader)
#   - NO giant TSV aggregations inside shard jobs (use precomputed Parquet)

import os
import argparse
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# -----------------------------
# Utilities
# -----------------------------
def shard_filter(df, shard_idx: int, shards: int, user_col: str = "user_id"):
    """Deterministic sharding by hash(user_id) % shards."""
    df = df.withColumn(user_col, F.col(user_col).cast("string"))
    sh = F.pmod(F.abs(F.hash(F.col(user_col))), F.lit(shards))
    return df.withColumn("_shard", sh).filter(F.col("_shard") == shard_idx).drop("_shard")


def check_dupes(df, label, n=5, do_heavy=False):
    """
    SAFE debug helper: by default no df.count() or full duplicate scans.
    """
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")
    try:
        df.limit(n).show(n, truncate=False)
    except Exception as e:
        print(f"[WARN] sample show failed: {e}")
    if not do_heavy:
        print("[INFO] Skipping full count/dupe scan (do_heavy=False).")
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


# -----------------------------
# Args
# -----------------------------
def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, required=True)

    ap.add_argument("--step1-dir", required=True)
    ap.add_argument("--step2-dir", required=True)
    ap.add_argument("--step3-dir", required=True)

    ap.add_argument("--company-ref", default="/labs/khanna/linkedin_202507/academic_company_ref")

    # PRECOMPUTED USPTO aux parquet dir
    ap.add_argument("--uspto-aux-dir", required=True,
                    help="Directory containing us_citations_by_patent/ and cpc_main/ parquet outputs")

    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--tmpdir", default=None)

    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")))
    ap.add_argument("--shuffle-partitions", type=int, default=1200)
    ap.add_argument("--coalesce", type=int, default=8)

    return ap.parse_args()


def main():
    args = parse_args()
    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards-1}]")

    # ---------------- Spark session ----------------
    builder = (
        SparkSession.builder
        .appName(f"inventor_year_v2_shard_{args.shard_idx}_of_{args.shards}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")

        # Robust parquet handling
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
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir,'tmp')}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir,'tmp')}")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    print(f"[INFO] Starting shard {args.shard_idx}/{args.shards-1}")
    if args.tmpdir:
        print(f"[SCRATCH] {args.tmpdir}")

    # Checkpoint dir
    checkpoint_dir = "/labs/khanna/linkedin_202507/scratch/spark_checkpoints_v2"
    os.makedirs(checkpoint_dir, exist_ok=True)
    spark.sparkContext.setCheckpointDir(checkpoint_dir)

    # =========================================================
    # [1/7] USER-LEVEL VARIABLES (STEP3) + DEMOGRAPHICS (STEP1)
    # =========================================================
    print("[1/7] Loading user-level vars from STEP3 (education summaries)…")

    # Step3 lives under shard=XX folders; recursive lookup is safest.
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
    print(f"      -> user_from_s3 cols: {len(user_from_s3.columns)}")

    print("[1b/7] Loading demographics au_* from STEP1 …")
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
    print(f"      -> user_from_s1 (au_*) cols: {len(user_from_s1.columns)}")

    user_vars = user_from_s3.join(user_from_s1, on="user_id", how="left").cache()

    # Old-style aliases used later
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

    # =========================================================
    # [2/7] PATENT SIDE (STEP1 + PRECOMPUTED USPTO AUX)
    # =========================================================
    print("[2/7] Building patent-side aggregates (using precomputed USPTO aux) …")

    pat = (
        s1.select("user_id", "patent_id", "application_id", "filing_date", "inventor_sequence")
          .dropna(subset=["user_id", "patent_id"])
    )

    pat = pat.withColumn(
        "filing_date",
        F.when(
            (F.col("filing_date") < F.to_date(F.lit("1950-01-01"))) |
            (F.col("filing_date") > F.to_date(F.lit("2025-12-31"))),
            F.lit(None)
        ).otherwise(F.col("filing_date"))
    )

    # ---- load precomputed aux parquet ----
    us_cit_parq = os.path.join(args.uspto_aux_dir, "us_citations_by_patent")
    cpc_parq    = os.path.join(args.uspto_aux_dir, "cpc_main")

    us_cit = spark.read.parquet(us_cit_parq)   # patent_id, n_us_citations
    cpc_main = spark.read.parquet(cpc_parq)    # patent_id, cpc_section_main, cpc_class_main

    pat = pat.join(us_cit, on="patent_id", how="left").fillna({"n_us_citations": 0})
    pat = pat.join(cpc_main, on="patent_id", how="left")

    pat = pat.withColumn("year", F.year("filing_date")).filter(F.col("year").isNotNull())

    # Modal fields per inventor-year
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
           .join(modal_class_y,   on=["user_id", "year"], how="left")
    )
    check_dupes(pat_year, "After PATENT aggregation (v2)", do_heavy=False)

    # Career modal fields
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

    # =========================================================
    # [3/7] POSITION SIDE (STEP2 -> user-year panel)
    # =========================================================
    print("[3/7] Building position-side aggregates from STEP2 …")

    s2 = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.step2_dir)
    )
    s2 = shard_filter(s2, args.shard_idx, args.shards, user_col="user_id")

    company_ref = spark.read.parquet(args.company_ref).select("rcid", "ultimate_parent_rcid")
    s2 = (
        s2.join(company_ref.withColumnRenamed("ultimate_parent_rcid", "parent_from_ref"),
                on="rcid", how="left")
          .withColumn("ultimate_parent_rcid", F.coalesce(F.col("parent_from_ref"), F.col("rcid")))
          .drop("parent_from_ref")
    )

    keep_pos_cols = [
        "user_id", "position_id", "rcid", "ultimate_parent_rcid",
        "startdate", "enddate",
        "salary", "total_compensation", "seniority",
        "city", "state", "country", "region", "metro_area", "location_raw"
    ]
    keep_pos_cols = [c for c in keep_pos_cols if c in s2.columns]
    pos = s2.select(*keep_pos_cols).dropna(subset=["user_id", "position_id"])

    spell_keys = [c for c in ["user_id", "position_id", "startdate", "enddate", "rcid"] if c in pos.columns]
    pos = pos.dropDuplicates(spell_keys)

    first_pos = (
        pos.filter(F.col("startdate").isNotNull())
           .groupBy("user_id")
           .agg(F.min("startdate").alias("first_startdate_pos"))
    )

    pos = pos.withColumn("start_year", F.year("startdate").cast("int")) \
             .withColumn("end_year", F.year("enddate").cast("int"))
    pos = pos.withColumn("end_year", F.when(F.col("end_year").isNull(), F.col("start_year")).otherwise(F.col("end_year")))

    pos_expanded = pos.withColumn("year", F.explode(F.sequence(F.col("start_year"), F.col("end_year"))))

    pos_year = (
        pos_expanded.groupBy("user_id", "year")
        .agg(
            F.countDistinct("position_id").alias("n_positions"),
            F.countDistinct("rcid").alias("n_unique_companies"),
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
        pos_year.groupBy("user_id", "year")
                .agg(*[
                    F.first(c, ignorenulls=True).alias(c)
                    for c in pos_year.columns if c not in ["user_id", "year"]
                ])
    )
    check_dupes(pos_year, "After POS yearly aggregation (v2)", do_heavy=False)

    # Forward-fill to continuous panel
    pos_year = pos_year.withColumn("pos_obs", F.lit(1))

    max_pat_year = pat_year.select(F.max("year")).collect()[0][0]
    max_pos_year = pos_year.select(F.max("year")).collect()[0][0]
    years = [y for y in [max_pat_year, max_pos_year] if y is not None]
    if len(years) == 0:
        print("[WARN] No valid years in this shard. Skipping shard...")
        spark.stop()
        return
    global_max_year = int(max(years))
    print(f"[INFO] Global maximum year detected (shard): {global_max_year}")

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
    pos_year_full = pos_year_full.withColumn(
        "pos_extrapolated",
        F.when(F.col("pos_obs").isNull(), F.lit(1)).otherwise(F.lit(0))
    ).drop("pos_obs")

    pos_year_full = (
        pos_year_full.groupBy("user_id", "year")
                    .agg(*[
                        F.first(c, ignorenulls=True).alias(c)
                        for c in pos_year_full.columns if c not in ["user_id", "year"]
                    ])
    )

    w_ff = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    cols_to_fill = [
        "first_rcid", "first_city", "first_parent_rcid", "last_parent_rcid",
        "first_state", "first_country", "first_region", "first_metro_area",
        "first_location_raw", "last_rcid", "last_city", "last_state",
        "last_country", "last_region", "last_metro_area", "last_location_raw",
        "first_position_id"
    ]
    for c in cols_to_fill:
        if c in pos_year_full.columns:
            pos_year_full = pos_year_full.withColumn(c, F.last(c, ignorenulls=True).over(w_ff))

    pos_year = pos_year_full

    # =========================================================
    # [4/7] Merge patent-year + pos-year + user vars
    # =========================================================
    print("[4/7] Merging patents + positions + user vars …")

    pat_year = pat_year.fillna({
        "n_patents": 0,
        "n_applications": 0,
        "n_first_inventor": 0,
        "n_us_citations": 0
    })

    inventor_year = (
        pat_year.join(pos_year, on=["user_id", "year"], how="outer")
                .join(first_pos, on="user_id", how="left")
                .join(user_vars, on="user_id", how="left")
                .join(career_modal_section, on="user_id", how="left")
                .join(career_modal_class,   on="user_id", how="left")
    )

    inventor_year = inventor_year.fillna({
        "n_patents": 0,
        "n_applications": 0,
        "n_first_inventor": 0,
        "n_us_citations": 0,
        "pos_extrapolated": 0,
        "n_educations": 0
    })
    if "has_education" in inventor_year.columns:
        inventor_year = inventor_year.fillna({"has_education": False})

    # =========================================================
    # [5/7] Immigrant measures (same logic as old code)
    # =========================================================
    print("[5/7] Creating immigrant measures …")

    US = F.lit("United States")

    if "first_university_country" in inventor_year.columns:
        inventor_year = inventor_year.withColumn(
            "immig_deg_first_nonUS",
            F.when(
                F.col("first_university_country").isNotNull() &
                (F.col("first_university_country") != US),
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
            F.col("first_pos_country").isNotNull() &
            (F.col("first_pos_country") != US),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    inventor_year = inventor_year.withColumn(
        "immig_first_deg_or_job_nonUS",
        F.when(
            (F.col("immig_deg_first_nonUS") == 1) | (F.col("immig_job_first_nonUS") == 1),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    inventor_year = inventor_year.drop("pos_country_y", "first_pos_country")

    w_cum = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    w_all = Window.partitionBy("user_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

    uni_c = F.col("first_university_country")

    m1_event = F.when(
        uni_c.isNotNull() & (F.col("first_country").isNotNull() & (F.col("first_country") != uni_c)),
        F.lit(1)
    ).otherwise(F.lit(0))

    inventor_year = inventor_year.withColumn("immig1_event", m1_event)
    inventor_year = inventor_year.withColumn("immig1_post", F.max("immig1_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig1_ever", F.max("immig1_event").over(w_all))

    pos_country_y = F.coalesce(F.col("last_country"), F.col("first_country"))

    within_year_change = F.when(
        F.col("first_country").isNotNull() & F.col("last_country").isNotNull() &
        (F.col("first_country") != F.col("last_country")),
        F.lit(1)
    ).otherwise(F.lit(0))

    lag_country = F.lag(pos_country_y).over(Window.partitionBy("user_id").orderBy("year"))
    across_year_change = F.when(
        pos_country_y.isNotNull() & lag_country.isNotNull() & (pos_country_y != lag_country),
        F.lit(1)
    ).otherwise(F.lit(0))

    m2_event = F.when((within_year_change == 1) | (across_year_change == 1), F.lit(1)).otherwise(F.lit(0))

    inventor_year = inventor_year.withColumn("immig2_event", m2_event)
    inventor_year = inventor_year.withColumn("immig2_post", F.max("immig2_event").over(w_cum))
    inventor_year = inventor_year.withColumn("immig2_ever", F.max("immig2_event").over(w_all))
    inventor_year = inventor_year.drop("immig1_event", "immig2_event")

    # =========================================================
    # [6/7] Backfill modal fields using "next patent"
    # =========================================================
    print("[6/7] Backfilling modal fields (next patent) …")

    w_future = Window.partitionBy("user_id").orderBy("year").rowsBetween(0, Window.unboundedFollowing)
    w_past   = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)

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

    # =========================================================
    # [7/7] Write output shard
    # =========================================================
    print("[7/7] Writing output shard …")

    inventor_year = inventor_year.checkpoint(eager=True)
    inventor_year = inventor_year.coalesce(args.coalesce)

    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    os.makedirs(out_path, exist_ok=True)

    print(f"[WRITE] -> {out_path}")
    (
        inventor_year.write.mode("overwrite")
                    .option("compression", "snappy")
                    .parquet(out_path)
    )

    print(f"[INFO] Finished shard {args.shard_idx:02d}")
    spark.stop()


if __name__ == "__main__":
    main()
