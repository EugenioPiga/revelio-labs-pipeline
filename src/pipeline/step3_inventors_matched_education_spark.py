#!/usr/bin/env python3
# step3_inventors_matched_positions_education_spark.py
#
# STEP 3 (Unified): Attach education to STEP2 inventor-position dataset + enrich.
#
# INPUTS:
#   - STEP2: /labs/.../processed/step2_output/inventors_matched_positions_spark/shard=XX
#   - EDUCATION: /labs/.../academic_individual_user_education
#
# OUTPUT:
#   - OUT_DIR/shard=XX
#
# WHAT THIS DOES (important design choice):
#   - Avoids "row explosion" from joining all education spells to each position row.
#   - Instead, computes user-level education summaries:
#       has_education, n_educations,
#       first education record (by startdate),
#       last education record (by enddate),
#     including the enrichment cols university_country, university_location, education_number, etc.
#   - Then left-joins those summaries onto STEP2 rows (safe, scalable).

import os
import argparse
from pyspark.sql import SparkSession, functions as F, Window


# Education columns we want to carry into first/last education records.
# (This includes the "enrichment" cols that used to be added in a separate script.)
EDU_KEEP = [
    "user_id",
    "university_raw",
    "university_name",
    "rsid",
    "education_number",
    "startdate",
    "enddate",
    "degree",
    "field",
    "degree_raw",
    "field_raw",
    "university_country",
    "university_location",
    "ultimate_parent_school_name",
    "ultimate_parent_rsid",
    "description",
]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2-dir", required=True, help="Root parquet dir from step2 (contains shard=XX subfolders)")
    ap.add_argument("--education-dir", required=True, help="Parquet dir for academic_individual_user_education")
    ap.add_argument("--out-dir", required=True, help="Output root dir for step3 (will write shard=XX subfolders)")
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    ap.add_argument("--shuffle-partitions", type=int, default=1200)
    ap.add_argument("--coalesce", type=int, default=200)
    ap.add_argument("--tmpdir", default=None, help="Scratch base (we will use spark.local.dir=tmpdir)")
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "0")))
    return ap.parse_args()


def main():
    args = parse_args()
    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards-1}]")

    # ---------------- Spark session ----------------
    builder = (
        SparkSession.builder
        .appName(f"step3_inv_pos_edu_shard_{args.shard_idx}_of_{args.shards}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")

        # Robust parquet settings (you already needed these in step1/2)
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
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
    spark.sparkContext.setLogLevel("WARN")

    print(f"[CONF] threads={args.threads} shuffle={args.shuffle_partitions} shard={args.shard_idx}/{args.shards-1}")
    if args.tmpdir:
        print(f"[SCRATCH] {args.tmpdir}")

    # -----------------------------
    # [1/5] Load STEP2 shard
    # -----------------------------
    shard_path = os.path.join(args.step2_dir, f"shard={args.shard_idx:02d}")
    print("[1/5] Loading STEP2 shard …")
    print(f"      path={shard_path}")

    s2 = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(shard_path)
    )

    # Normalize join key type (step1 had int64; positions can vary)
    s2 = s2.withColumn("user_id", F.col("user_id").cast("string"))

    # Cache s2 because we reuse user ids multiple times
    s2 = s2.cache()
    s2_rows = s2.count()
    s2_users = s2.select("user_id").where(F.col("user_id").isNotNull()).distinct().count()
    print(f"      -> STEP2 rows: {s2_rows:,} | distinct users: {s2_users:,}")

    # Collect user_ids as a DF for filtering education
    user_ids = s2.select("user_id").where(F.col("user_id").isNotNull()).distinct().cache()

    # -----------------------------
    # [2/5] Load EDUCATION and filter to shard users
    # -----------------------------
    print("[2/5] Loading EDUCATION (and filtering to shard users) …")
    edu = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.education_dir)
             .withColumn("user_id", F.col("user_id").cast("string"))
    )

    # Keep only expected columns that exist
    edu_cols_present = [c for c in EDU_KEEP if c in edu.columns]
    edu = edu.select(*edu_cols_present)

    # Filter to this shard’s users
    # (This avoids scanning + shuffling unnecessary education rows in later steps.)
    edu = edu.join(user_ids, on="user_id", how="inner").cache()

    edu_rows = edu.count()
    edu_users = edu.select("user_id").distinct().count()
    print(f"      -> EDU rows (shard users only): {edu_rows:,} | users with ≥1 edu row: {edu_users:,}")
    print(f"      -> EDU cols: {edu.columns}")

    # -----------------------------
    # [3/5] Build user-level education summaries
    # -----------------------------
    print("[3/5] Building education summaries (has_education, n_educations, first/last record) …")

    # Count how many education rows each user has
    edu_n = edu.groupBy("user_id").agg(F.count("*").alias("n_educations"))

    # Define ordering:
    # - "first": earliest startdate (fallback on education_number if startdate missing)
    # - "last": latest enddate (fallback on startdate, then education_number)
    #
    # Note: startdate/enddate in this dataset are typically dates; if strings, Spark will still order lexicographically.
    w_first = Window.partitionBy("user_id").orderBy(
        F.col("startdate").asc_nulls_last(),
        F.col("education_number").asc_nulls_last(),
        F.col("enddate").asc_nulls_last()
    )
    w_last = Window.partitionBy("user_id").orderBy(
        F.col("enddate").desc_nulls_last(),
        F.col("startdate").desc_nulls_last(),
        F.col("education_number").desc_nulls_last()
    )

    # First education record per user
    edu_first = (
        edu.withColumn("_rn", F.row_number().over(w_first))
           .filter(F.col("_rn") == 1)
           .drop("_rn")
    )

    # Last education record per user
    edu_last = (
        edu.withColumn("_rn", F.row_number().over(w_last))
           .filter(F.col("_rn") == 1)
           .drop("_rn")
    )

    # Prefix columns (avoid collisions and make it explicit what they represent)
    for c in [x for x in edu_first.columns if x != "user_id"]:
        edu_first = edu_first.withColumnRenamed(c, f"edu_first_{c}")
    for c in [x for x in edu_last.columns if x != "user_id"]:
        edu_last = edu_last.withColumnRenamed(c, f"edu_last_{c}")

    # -----------------------------
    # [4/5] Join summaries onto STEP2 and create flags
    # -----------------------------
    print("[4/5] Joining education summaries onto STEP2 …")
    final = (
        s2.join(edu_n, on="user_id", how="left")
          .join(edu_first, on="user_id", how="left")
          .join(edu_last, on="user_id", how="left")
    )

    # has_education + fill n_educations=0 when missing
    final = final.withColumn("n_educations", F.coalesce(F.col("n_educations"), F.lit(0)))
    final = final.withColumn("has_education", (F.col("n_educations") > 0))

    # Optional: quick derived education-year features (useful later)
    if "edu_first_startdate" in final.columns:
        final = final.withColumn("edu_first_start_year", F.year("edu_first_startdate"))
    if "edu_last_enddate" in final.columns:
        final = final.withColumn("edu_last_end_year", F.year("edu_last_enddate"))

    # -----------------------------
    # [5/5] Write shard
    # -----------------------------
    out_shard = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    print("[5/5] Writing output …")
    print(f"      -> {out_shard}")

    (
        final.coalesce(args.coalesce)
             .write.mode("overwrite")
             .option("compression", "snappy")
             .parquet(out_shard)
    )

    # Light stats (avoid too many actions)
    have_edu = final.filter(F.col("has_education") == True).count()
    print(f"==> Done (shard={args.shard_idx:02d}). STEP2 rows: {s2_rows:,} | has_education=True rows: {have_edu:,}")

    spark.stop()


if __name__ == "__main__":
    main()
