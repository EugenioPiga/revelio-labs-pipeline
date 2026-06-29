#!/usr/bin/env python3
"""
step5_merge_inventor_year_shards_v3.py

Merge V3 spell-safe inventor-year shards into one final panel.

This is intentionally a separate publishing step:
* it never overwrites the old V2 panel;
* it validates one row per user_id x year;
* it validates the V3 current/last-known semantic flags;
* it writes to a temporary directory and only then publishes the new output.
"""

import argparse
import os
import shutil
import time

from pyspark.sql import SparkSession, functions as F


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-shards-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--tmp-out-dir", default=None)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")))
    ap.add_argument("--shuffle-partitions", type=int, default=2000)
    ap.add_argument("--out-partitions", type=int, default=300)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument("--no-publish", action="store_true")
    return ap.parse_args()


def path_exists(path):
    return os.path.exists(path) or os.path.islink(path)


def remove_path(path):
    if os.path.islink(path) or os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


def publish_output(tmp_out, out_dir):
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup = f"{out_dir}_BACKUP_PRE_V3_{timestamp}"

    if not os.path.isdir(tmp_out):
        raise RuntimeError(f"Temporary output does not exist: {tmp_out}")

    if path_exists(out_dir):
        print(f"[PUBLISH] Moving existing output to: {backup}")
        os.rename(out_dir, backup)

    print(f"[PUBLISH] Publishing {tmp_out} -> {out_dir}")
    os.rename(tmp_out, out_dir)


def assert_no_duplicate_user_year(panel):
    bad = (
        panel.groupBy("user_id", "year")
        .count()
        .filter(F.col("count") > 1)
        .limit(10)
    )
    if bad.count() > 0:
        bad.show(truncate=False)
        raise RuntimeError("STEP5 failed: duplicate user_id x year rows.")


def main():
    args = parse_args()
    tmp_out = args.tmp_out_dir or f"{args.out_dir}_tmp_{time.strftime('%Y%m%d_%H%M%S')}"

    builder = (
        SparkSession.builder
        .appName("step5_merge_inventor_year_shards_v3")
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

    print("=" * 94)
    print("[STEP5 V3] Merge spell-safe inventor-year shards")
    print(f"[SHARDS] {args.input_shards_dir}")
    print(f"[TMP]    {tmp_out}")
    print(f"[OUT]    {args.out_dir}")
    print("=" * 94)

    if path_exists(tmp_out):
        remove_path(tmp_out)

    panel = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(os.path.join(args.input_shards_dir, "shard=*"))
        .withColumn("user_id", F.col("user_id").cast("string"))
        .withColumn("year", F.col("year").cast("int"))
    )

    assert_no_duplicate_user_year(panel)

    required = [
        "current_position_id",
        "current_position_observed",
        "position_carried_forward",
        "last_known_position_id",
        "inventor_year_pipeline_version",
    ]
    missing = [c for c in required if c not in panel.columns]
    if missing:
        raise RuntimeError(f"STEP5 received a non-V3 panel; missing columns: {missing}")

    # Semantic checks: current_* must be missing in a carried-forward-only row;
    # a carried-forward row must have a last_known spell.
    semantic_bad = (
        panel.filter(
            (
                (F.col("position_carried_forward") == 1)
                & F.col("current_position_id").isNotNull()
            )
            | (
                (F.col("position_carried_forward") == 1)
                & F.col("last_known_position_id").isNull()
            )
            | (
                (F.col("current_position_observed") == 1)
                & F.col("current_position_id").isNull()
            )
        )
        .limit(10)
    )

    if semantic_bad.count() > 0:
        print("[ERROR] V3 current/last-known semantic validation failed.")
        semantic_bad.show(truncate=False)
        raise RuntimeError("STEP5 semantic validation failed.")

    panel = panel.withColumn(
        "merge_pipeline_version",
        F.lit("v3_spell_integrity"),
    )

    (
        panel.repartition(args.out_partitions)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(tmp_out)
    )

    spark.stop()

    if args.no_publish:
        print(f"[DONE] --no-publish set; merged panel remains at {tmp_out}")
    else:
        publish_output(tmp_out, args.out_dir)
        print(f"[DONE] Published merged V3 panel: {args.out_dir}")


if __name__ == "__main__":
    main()
