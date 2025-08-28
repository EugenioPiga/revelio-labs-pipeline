#!/usr/bin/env python
# step3_inventors_matched_education_spark.py
#
# Attach education to step2 (inventor x user with positions), add has_education flag.
# Efficient PySpark pipeline with sharding by user_id.

import os
import argparse
from typing import List
from pyspark.sql import SparkSession, functions as F

# Columns we keep from step2 unless --keep-all-step2 is passed
DEFAULT_STEP2_KEEP = [
    "user_id", "patent_id", "patent_date",
    "company_name", "rcid",
    "ultimate_parent_company_name", "ultimate_parent_rcid",
    "has_position", "shard"
]

# Lean set of education columns
DEFAULT_EDU_KEEP = [
    "user_id",
    "university_name",
    "degree",
    "field",
    "startdate",
    "enddate",
    "rsid",
    "ultimate_parent_school_name",
    "ultimate_parent_rsid",
    "description",
]

def select_present(df, cols: List[str]):
    present = [c for c in cols if c in df.columns]
    return df.select(*present) if present else df

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2-dir", required=True, help="Parquet from step2 (positions), e.g. output/inventors_matched_positions_spark")
    ap.add_argument("--education-dir", required=True, help="Parquet root for academic_individual_user_education")
    ap.add_argument("--out-dir", required=True, help="Destination parquet dir, e.g. output/inventors_matched_education_spark")
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    ap.add_argument("--shuffle-partitions", type=int, default=600)
    ap.add_argument("--tmpdir", default=os.environ.get("SPARK_LOCAL_DIRS", os.path.join(os.environ.get("HOME", "."), ".spark_scratch")))
    ap.add_argument("--shards", type=int, default=8)
    ap.add_argument("--shard-index", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "0")))
    ap.add_argument("--keep-all-step2", action="store_true")
    ap.add_argument("--edu-cols", nargs="*", default=DEFAULT_EDU_KEEP)
    ap.add_argument("--coalesce", type=int, default=64)
    args = ap.parse_args()

    spark = (
        SparkSession.builder
        .master(f"local[{args.threads}]")
        .appName(f"step3_inv_edu_spark_shard_{args.shard_index}_of_{args.shards}")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.local.dir", args.tmpdir)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.memory.fraction", "0.6")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print(f"[SCRATCH] {args.tmpdir}")
    print(f"[CONF] threads={args.threads} shuffle={args.shuffle_partitions} shard={args.shard_index}/{args.shards-1}")

    # [1/5] Load step2 lean and shard
    print("[1/5] Loading STEP2 …")
    s2 = spark.read.parquet(args.step2_dir).withColumn("user_id", F.col("user_id").cast("string"))
    s2 = s2 if args.keep_all_step2 else select_present(s2, DEFAULT_STEP2_KEEP + ["user_id"])
    s2 = s2.withColumn("_shard", F.pmod(F.abs(F.hash("user_id")), F.lit(args.shards)))
    s2 = s2.filter(F.col("_shard") == args.shard_index).drop("_shard")
    print(f"     -> STEP2 shard rows: {s2.count():,}")

    # [2/5] Load education (lean)
    print("[2/5] Loading EDUCATION …")
    edu = spark.read.parquet(args.education_dir).withColumn("user_id", F.col("user_id").cast("string"))
    edu = select_present(edu, list(dict.fromkeys(["user_id"] + (args.edu_cols or []))))
    print(f"     -> EDU cols: {edu.columns}")

    # [3/5] Flag has_education (semi-join)
    print("[3/5] Flagging has_education …")
    edu_ids = edu.select("user_id").where(F.col("user_id").isNotNull()).distinct()
    s2_flag = (
        s2.alias("s")
        .join(edu_ids.alias("e"), on="user_id", how="left")
        .withColumn("has_education", F.col("e.user_id").isNotNull())
        .drop("e.user_id")
    )
    n_true = s2_flag.filter("has_education").count()
    print(f"     -> has_education=True rows: {n_true:,}")

    # [4/5] Join details only for those with education
    print("[4/5] Joining education details …")
    needs = s2_flag.filter("has_education")
    rest  = s2_flag.filter("NOT has_education")
    joined = needs.join(edu, on="user_id", how="left")

    # Align schemas for union
    add_cols = set(joined.columns) - set(rest.columns)
    for c in add_cols:
        rest = rest.withColumn(c, F.lit(None).cast(joined.schema[c].dataType))
    rest = rest.select(joined.columns)

    final = joined.unionByName(rest)

    # [5/5] Write shard
    shard_dir = os.path.join(args.out_dir, f"shard={args.shard_index:02d}")
    print(f"[WRITE] -> {shard_dir}")
    (final.coalesce(args.coalesce)
          .write.mode("overwrite")
          .option("compression", "snappy")
          .parquet(shard_dir))

    total = final.count()
    approx_users = final.select(F.approx_count_distinct("user_id").alias("n")).collect()[0]["n"]
    have_edu = final.filter("has_education").count()
    print(f"==> Done (shard={args.shard_index:02d}). Rows: {total:,} | distinct user_id (approx): {approx_users:,} | has_education=True: {have_edu:,}")

    spark.stop()

if __name__ == "__main__":
    main()
