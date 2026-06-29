#!/usr/bin/env python3
"""
step1_inventors_matched_users_spark_v3.py

STEP 1: Build the inventor-to-user base table.

This step is intentionally spell-neutral: it does not read positions and cannot
change employer, title, or location information. User/profile columns are
prefixed with au_ to prevent future schema collisions.

Output is a user/patent-level table. Later steps read this table directly for
patent counts and profile variables; Step 2 only uses its distinct user_ids to
select the relevant raw position spells.
"""

import argparse
import os
from pyspark.sql import SparkSession, functions as F


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users-dir", required=True)
    ap.add_argument("--inventors-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    ap.add_argument("--shuffle-partitions", type=int, default=400)
    ap.add_argument("--coalesce", type=int, default=200)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument(
        "--prefix-user-cols",
        action="store_true",
        default=True,
        help="Prefix academic_individual_user columns with au_. Enabled by default."
    )
    ap.add_argument("--broadcast-users", action="store_true")
    return ap.parse_args()


def main():
    args = parse_args()

    builder = (
        SparkSession.builder
        .appName("step1_inventors_matched_users_v3")
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

    print("=" * 88)
    print("[STEP1 V3] Inventor matches joined to user profiles")
    print(f"[USERS]     {args.users_dir}")
    print(f"[INVENTORS] {args.inventors_dir}")
    print(f"[OUT]       {args.out_dir}")
    print("=" * 88)

    users = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.users_dir)
        .filter(F.col("user_id").isNotNull())
        .withColumn("user_id", F.col("user_id").cast("string"))
        .dropDuplicates(["user_id"])
    )

    if args.prefix_user_cols:
        for c in list(users.columns):
            if c != "user_id" and not c.startswith("au_"):
                users = users.withColumnRenamed(c, f"au_{c}")

    inventors = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.inventors_dir)
        .filter(F.col("user_id").isNotNull())
        .withColumn("user_id", F.col("user_id").cast("string"))
    )

    users_join = F.broadcast(users) if args.broadcast_users else users
    out = inventors.join(users_join, on="user_id", how="inner")

    (
        out.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.out_dir)
    )

    print(f"[DONE] output columns={len(out.columns)}")
    spark.stop()


if __name__ == "__main__":
    main()
