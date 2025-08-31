#!/usr/bin/env python
# inventor_level_file.py
# Create inventor-level dataset with patents + education info
# Supports sharding for memory efficiency.

import argparse
import os
from pyspark.sql import SparkSession, functions as F

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input enriched dataset (parquet)")
    ap.add_argument("--out-dir", required=True, help="Output directory")
    ap.add_argument("--shards", type=int, default=1, help="Number of shards")
    ap.add_argument("--shard-idx", type=int, default=0, help="This shard index (0..shards-1)")

    # Extra Spark tuning args so sbatch launcher works consistently
    ap.add_argument("--threads", type=int, default=8, help="Spark threads")
    ap.add_argument("--shuffle-partitions", type=int, default=200, help="Spark shuffle partitions")
    ap.add_argument("--coalesce", type=int, default=80, help="Output files per shard")
    ap.add_argument("--tmpdir", default=None, help="Scratch directory for Spark spill")

    return ap.parse_args()

def main():
    args = parse_args()

    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards-1}]")

    # ---------------- Spark session ----------------
    builder = (
        SparkSession.builder
        .appName(f"inventor_level_shard_{args.shard_idx}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.files.maxPartitionBytes", "32m")
    )
    if args.tmpdir:
        builder = builder.config("spark.local.dir", args.tmpdir)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print(f"[INFO] Processing shard {args.shard_idx+1}/{args.shards}")

    # --- 1) Load input ---
    df = spark.read.parquet(os.path.join(args.input, "*"))
    print(f"[INFO] Loaded {df.count():,} rows")

    # --- 2) Shard assignment by user_id ---
    df = df.withColumn(
        "shard_assignment",
        (F.hash("user_id") % args.shards)
    )
    df = df.filter(F.col("shard_assignment") == args.shard_idx).drop("shard_assignment")

    # --- 3) Aggregations to inventor-level ---
    inventor_level = (
        df.groupBy("user_id")
          .agg(
              F.countDistinct("patent_id").alias("num_patents"),
              F.min("patent_date").alias("first_patent_date"),
              F.max("patent_date").alias("last_patent_date"),
              F.countDistinct("degree").alias("num_degrees"),
              F.first("field", ignorenulls=True).alias("first_field"),
              F.first("university_name", ignorenulls=True).alias("first_university")
          )
    )

    # --- 4) Write output ---
    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    (
        inventor_level.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    try:
        with open(os.path.join(out_path, "_SUCCESS"), "w") as f:
            f.write("")
    except Exception:
        pass

    # --- 5) Stats ---
    n_rows = inventor_level.count()
    print(f"[DONE] Shard {args.shard_idx+1}/{args.shards} | Inventors: {n_rows:,}")

    spark.stop()

if __name__ == "__main__":
    main()
