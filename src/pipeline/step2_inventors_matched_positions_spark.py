#!/usr/bin/env python
# step2_inventors_positions_spark.py
# Build inventor–position dataset: user_id × position_id
# Includes inventor/patent metadata (from STEP1) and position metadata (location, employer, salary, etc.)

import os
import argparse
from pyspark.sql import SparkSession

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1-dir", required=True, help="Path to STEP1 parquet (inventors_matched_users)")
    ap.add_argument("--positions-dir", required=True, help="Path to POSITIONS parquet")
    ap.add_argument("--out-dir", required=True, help="Output base directory")
    ap.add_argument("--threads", type=int, default=8, help="Spark threads")
    ap.add_argument("--shuffle-partitions", type=int, default=200, help="Spark shuffle partitions")
    ap.add_argument("--coalesce", type=int, default=80, help="Output files per shard")
    ap.add_argument("--tmpdir", default=None, help="Scratch directory for Spark spill")
    ap.add_argument("--shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--shard-idx", type=int, default=0, help="This shard index (0..shards-1)")
    return ap.parse_args()

def main():
    args = parse_args()
    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards-1}]")

    # ---------------- Spark session ----------------
    builder = (
        SparkSession.builder
        .appName(f"step2_inv_pos_shard_{args.shard_idx}_of_{args.shards}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
    )
    if args.tmpdir:
        builder = builder.config("spark.local.dir", args.tmpdir)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    # ---------------- Load STEP1 ----------------
    print("[1/5] Loading STEP1 …")
    step1 = spark.read.parquet(os.path.join(args.step1_dir, "*"))
    step1_cols = step1.columns   # keep all inventor/patent columns

    # ---------------- Load POSITIONS ----------------
    print("[2/5] Loading POSITIONS …")
    positions = spark.read.parquet(os.path.join(args.positions_dir, "*"))
    pos_cols = positions.columns

    # ---------------- Join ----------------
    print("[3/5] Join STEP1 with POSITIONS on user_id …")
    joined = positions.join(step1, on="user_id", how="inner")

    # ---------------- Select columns ----------------
    keep_pos_cols = [
        "position_id", "company_name", "rcid",
        "title_raw", "job_category_v2", "seniority", "salary",
        "startdate", "enddate",
        "city", "state", "country", "region", "metro_area", "location_raw",
        "total_compensation", "start_salary", "end_salary"
    ]
    keep_pos_cols = [c for c in keep_pos_cols if c in pos_cols]

    # Avoid duplicate user_id or other column collisions
    keep_cols = []
    seen = set()
    for c in ["user_id"] + step1_cols + keep_pos_cols:
        if c in joined.columns and c not in seen:
            keep_cols.append(c)
            seen.add(c)

    joined_final = joined.select(*keep_cols)
    
    # ---------------- Write output ----------------
    print("[4/5] Write output …")
    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    (
        joined_final.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    # Write success flag
    try:
        with open(os.path.join(out_path, "_SUCCESS"), "w") as f:
            f.write("")
    except Exception:
        pass

    # ---------------- Stats ----------------
    print("[5/5] Stats …")
    try:
        n_rows = joined_final.count()
        n_users = joined_final.select("user_id").distinct().count()
        n_pos = joined_final.select("position_id").distinct().count() if "position_id" in joined_final.columns else 0
        print(f"==> Done. Rows: {n_rows:,} | distinct users: {n_users:,} | distinct positions: {n_pos:,}")
    except Exception as e:
        print(f"Stats skipped: {e}")

    spark.stop()

if __name__ == "__main__":
    main()
