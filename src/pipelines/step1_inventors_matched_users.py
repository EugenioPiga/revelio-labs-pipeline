#!/usr/bin/env python3
# step1_inventors_matched_users_spark.py
#
# Build STEP1 dataset with Spark:
#  - Keep only inventor-match rows whose user_id exists in academic_individual_user
#  - Add ALL columns from academic_individual_user (sex_predicted, ethnicity_predicted, probs, etc.)
#
# Motivation:
#  - Dask version can OOM on single node (big join/filter).
#  - Spark handles this more robustly on the cluster.
#
# Fix for your failure:
#  - Disable Parquet vectorized reader + set LEGACY datetime/int96 rebase
#    to avoid VectorizedColumnReader.readBatch crashes.

import argparse
from pyspark.sql import SparkSession, functions as F


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users-dir", required=True, help="Path to academic_individual_user parquet dir")
    ap.add_argument("--inventors-dir", required=True, help="Path to revelio_patents_inventor_matches parquet dir")
    ap.add_argument("--out-dir", required=True, help="Output directory (parquet)")
    ap.add_argument("--shuffle-partitions", type=int, default=400, help="spark.sql.shuffle.partitions")
    ap.add_argument("--coalesce", type=int, default=200, help="Number of output files")
    ap.add_argument("--tmpdir", default=None, help="Scratch dir for spark.local.dir")
    ap.add_argument("--prefix-user-cols", action="store_true",
                    help="Prefix user cols with au_ (recommended to avoid name collisions)")
    ap.add_argument("--broadcast-users", action="store_true",
                    help="Broadcast USERS (ONLY if users table is much smaller than inventors)")
    return ap.parse_args()


def main():
    args = parse_args()

    # ---------------- Spark session ----------------
    builder = (
        SparkSession.builder
        .appName("step1_inventors_matched_users_spark")
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
        builder = builder.config("spark.local.dir", args.tmpdir)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    print("[DEBUG] vectorized =", spark.conf.get("spark.sql.parquet.enableVectorizedReader"))
    print("[DEBUG] datetimeRebaseModeInRead =", spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead"))
    print("[DEBUG] int96RebaseModeInRead =", spark.conf.get("spark.sql.parquet.int96RebaseModeInRead"))

    # ---------------- Load USERS ----------------
    print("[1/5] Load USERS …")
    users = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.users_dir)
             .filter(F.col("user_id").isNotNull())
             .dropDuplicates(["user_id"])  # avoid row multiplication in join
    )

   # Optionally prefix user columns (except join key)
    if args.prefix_user_cols:
        for c in users.columns:
            if c != "user_id":
                users = users.withColumnRenamed(c, f"au_{c}")

    # ---------------- Load INVENTORS ----------------
    print("[2/5] Load INVENTORS …")
    inv = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.inventors_dir)
             .filter(F.col("user_id").isNotNull())
    )

    # ---------------- Join ----------------
    print("[3/5] Join INVENTORS ⨝ USERS on user_id …")
    users_join = F.broadcast(users) if args.broadcast_users else users
    joined = inv.join(users_join, on="user_id", how="inner")

    # ---------------- Write output ----------------
    print("[4/5] Write output …")
    (
        joined.coalesce(args.coalesce)
              .write.mode("overwrite")
              .option("compression", "snappy")
              .parquet(args.out_dir)
    )

    # ---------------- Quick checks ----------------
    print("[5/5] Quick checks …")
    cols = joined.columns
    print("[INFO] Output columns:", len(cols))

    spark.stop()
    print("[INFO] Done.")


if __name__ == "__main__":
    main()
