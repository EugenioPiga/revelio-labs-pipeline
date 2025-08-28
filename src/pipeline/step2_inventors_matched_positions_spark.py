#!/usr/bin/env python
# step2_inventors_matched_positions_spark.py
# Join STEP1 (inventors_matched_users) with POSITIONS on user_id using PySpark.
# Supports sharding to reduce memory pressure; each array task processes a disjoint shard.

import os
import argparse
from pyspark.sql import SparkSession, functions as F

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1-dir", required=True, help="e.g. output/inventors_matched_users")
    ap.add_argument("--positions-dir", required=True, help="e.g. academic_individual_position")
    ap.add_argument("--out-dir", required=True, help="Destination base dir")
    ap.add_argument("--threads", type=int, default=8, help="local[*] threads")
    ap.add_argument("--shuffle-partitions", type=int, default=200, help="spark.sql.shuffle.partitions")
    ap.add_argument("--tmpdir", default=None, help="Scratch dir for Spark (also uses SPARK_LOCAL_DIRS)")
    ap.add_argument("--coalesce", type=int, default=80, help="Coalesce output to N files")
    # NEW: sharding flags (match your sbatch)
    ap.add_argument("--shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--shard-idx", type=int, default=0, help="This task's shard index [0..shards-1]")
    return ap.parse_args()

def main():
    args = parse_args()
    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards-1}]")

    builder = (
        SparkSession.builder
        .appName(f"step2_inv_pos_spark_shard_{args.shard_idx}_of_{args.shards}")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .config("spark.network.timeout", "800s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.adaptive.enabled", "true")
    )
    # scratch / spill
    local_dirs = args.tmpdir or os.environ.get("SPARK_LOCAL_DIRS")
    if local_dirs:
        builder = builder.config("spark.local.dir", local_dirs)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] Loading STEP1 …")
    step1 = (
        spark.read.parquet(os.path.join(args.step1_dir, "*"))
        # Keep all STEP1 cols; we'll drop overlaps later
    )
    if "user_id" not in step1.columns:
        raise SystemExit("STEP1 is missing 'user_id'")
    step1 = step1.dropna(subset=["user_id"]).withColumn("user_id", F.col("user_id").cast("long"))
    print(f"     -> STEP1 cols: {len(step1.columns)}")

    print("[2/5] Loading POSITIONS …")
    positions = spark.read.parquet(os.path.join(args.positions_dir, "*"))
    if "user_id" not in positions.columns:
        raise SystemExit("POSITIONS is missing 'user_id'")
    positions = positions.dropna(subset=["user_id"]).withColumn("user_id", F.col("user_id").cast("long"))

    # Select a lean set first; keep whatever you truly need from positions.
    # Add/remove columns here as appropriate for your pipeline.
    # We always retain user_id + a marker for has_position.
    MIN_POS_COLS = ["user_id", "rcid", "company_name", "ultimate_parent_rcid", "ultimate_parent_company_name"]
    have_cols = [c for c in MIN_POS_COLS if c in positions.columns]
    positions = positions.select(*({ "user_id", *have_cols }))

    print(f"     -> POSITIONS cols (selected): {positions.columns}")

    print("[3/5] Shard POSITIONS by user_id …")
    if args.shards > 1:
        # Stable shard by user_id: pmod(abs(hash(user_id)), shards) == shard_idx
        positions = positions.where(
            F.pmod(F.abs(F.hash("user_id")), F.lit(args.shards)) == F.lit(args.shard_idx)
        )
    # Pre-partition by user_id to help the join
    positions = positions.repartition(max(1, args.shuffle_partitions), "user_id")

    print("[4/5] Resolve overlapping columns (prefer POSITIONS values) …")
    overlap = [c for c in step1.columns if c in positions.columns and c not in ("user_id",)]
    if overlap:
        step1 = step1.drop(*overlap)

    print("[5/5] Joining on user_id …")
    pos_with_flag = positions.withColumn("_pos_marker", F.lit(1))
    merged = (
        step1.join(pos_with_flag, on="user_id", how="left")
             .withColumn("has_position", F.col("_pos_marker").isNotNull())
             .drop("_pos_marker")
    )

    # Write out under shard directory so multiple array tasks don't clobber each other
    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    print(f"[WRITE] -> {out_path}")
    (merged
        .coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    # Success sentinel inside the shard folder
    try:
        with open(os.path.join(out_path, "_SUCCESS"), "w") as _f:
            _f.write("")
    except Exception:
        pass

    # Lightweight stats
    try:
        n_rows = merged.selectExpr("count(1) as c").collect()[0]["c"]
        n_users = merged.select("user_id").distinct().count()
        have_pos = merged.filter("has_position").count()
        print(f"==> Done. Rows: {n_rows:,} | distinct user_id: {n_users:,} | has_position=True rows: {have_pos:,}")
    except Exception:
        print("==> Done. (Skipped final count)")

    spark.stop()

if __name__ == "__main__":
    main()
