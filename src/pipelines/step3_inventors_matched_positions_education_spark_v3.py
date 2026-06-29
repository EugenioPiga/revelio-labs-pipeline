#!/usr/bin/env python3
"""
step3_inventors_matched_positions_education_spark_v3.py

STEP 3: attach user-level education summaries to canonical STEP2 position spells.

The education join is deliberately one-to-many only in the safe direction:
one user-level education summary is left-joined to each existing position spell.
No position-level employer/title/location field is altered, filled, or recomputed.
"""

import argparse
import os
from pyspark.sql import SparkSession, Window, functions as F


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
    ap.add_argument("--step2-dir", required=True)
    ap.add_argument("--education-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    ap.add_argument("--shuffle-partitions", type=int, default=1200)
    ap.add_argument("--coalesce", type=int, default=100)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "0")))
    return ap.parse_args()


def main():
    args = parse_args()
    if not (0 <= args.shard_idx < args.shards):
        raise SystemExit(f"--shard-idx must be in [0, {args.shards - 1}]")

    step2_shard = os.path.join(args.step2_dir, f"shard={args.shard_idx:02d}")

    builder = (
        SparkSession.builder
        .appName(f"step3_inv_pos_edu_v3_{args.shard_idx:02d}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
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
            .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
            .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("=" * 92)
    print(f"[STEP3 V3] Education summaries | shard={args.shard_idx:02d}/{args.shards - 1:02d}")
    print(f"[STEP2] {step2_shard}")
    print(f"[EDU]   {args.education_dir}")
    print(f"[OUT]   {args.out_dir}")
    print("=" * 92)

    s2 = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(step2_shard)
        .withColumn("user_id", F.col("user_id").cast("string"))
    )

    # STEP2 V3 guarantees one row per canonical raw spell. Check that the
    # education join cannot silently multiply them.
    n_s2 = s2.count()
    n_s2_spells = s2.select("user_id", "position_id").distinct().count()
    if n_s2 != n_s2_spells:
        raise RuntimeError(
            f"Input STEP2 is not one row per spell: rows={n_s2:,}, spells={n_s2_spells:,}"
        )

    users = s2.select("user_id").filter(F.col("user_id").isNotNull()).dropDuplicates()

    edu_raw = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.education_dir)
        .withColumn("user_id", F.col("user_id").cast("string"))
    )

    edu_cols = [c for c in EDU_KEEP if c in edu_raw.columns]
    edu = (
        edu_raw.select(*edu_cols)
        .join(users, on="user_id", how="inner")
        .withColumn("startdate", F.to_date("startdate"))
        .withColumn("enddate", F.to_date("enddate"))
    )

    edu_n = edu.groupBy("user_id").agg(F.count("*").alias("n_educations"))

    # Fully deterministic first/last ties.
    w_first = Window.partitionBy("user_id").orderBy(
        F.col("startdate").asc_nulls_last(),
        F.col("enddate").asc_nulls_last(),
        F.col("education_number").asc_nulls_last(),
        F.col("rsid").asc_nulls_last(),
        F.col("university_name").asc_nulls_last(),
    )
    w_last = Window.partitionBy("user_id").orderBy(
        F.col("enddate").desc_nulls_last(),
        F.col("startdate").desc_nulls_last(),
        F.col("education_number").desc_nulls_last(),
        F.col("rsid").desc_nulls_last(),
        F.col("university_name").desc_nulls_last(),
    )

    edu_first = (
        edu.withColumn("_rn", F.row_number().over(w_first))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )
    edu_last = (
        edu.withColumn("_rn", F.row_number().over(w_last))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    for c in [c for c in edu_first.columns if c != "user_id"]:
        edu_first = edu_first.withColumnRenamed(c, f"edu_first_{c}")
    for c in [c for c in edu_last.columns if c != "user_id"]:
        edu_last = edu_last.withColumnRenamed(c, f"edu_last_{c}")

    final = (
        s2.join(edu_n, on="user_id", how="left")
        .join(edu_first, on="user_id", how="left")
        .join(edu_last, on="user_id", how="left")
        .withColumn("n_educations", F.coalesce(F.col("n_educations"), F.lit(0)))
        .withColumn("has_education", (F.col("n_educations") > 0))
        .withColumn("education_pipeline_version", F.lit("v3_spell_integrity"))
    )

    if "edu_first_startdate" in final.columns:
        final = final.withColumn("edu_first_start_year", F.year("edu_first_startdate"))
    if "edu_last_enddate" in final.columns:
        final = final.withColumn("edu_last_end_year", F.year("edu_last_enddate"))

    n_final = final.count()
    n_final_spells = final.select("user_id", "position_id").distinct().count()
    if n_final != n_s2 or n_final_spells != n_s2_spells:
        raise RuntimeError(
            "STEP3 integrity failure: education summary join changed position spell cardinality. "
            f"input_rows={n_s2:,}, output_rows={n_final:,}, "
            f"input_spells={n_s2_spells:,}, output_spells={n_final_spells:,}"
        )

    out_shard = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    (
        final.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_shard)
    )

    print(f"[DONE] rows={n_final:,} | spells preserved={n_final_spells:,}")
    spark.stop()


if __name__ == "__main__":
    main()
