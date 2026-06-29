#!/usr/bin/env python3
"""
step2_inventors_matched_positions_spark_v3.py

STEP 2: build a canonical inventor-position-spell dataset.

Design guarantees
-----------------
* One row is one raw LinkedIn/Revelio position spell: user_id x position_id.
* Employer/title/location fields all come from the same selected raw spell.
* No employer, title, or location field is ever inherited from STEP1.
* STEP1 is used only to identify the inventor user universe, via a left-semi join.
  This avoids multiplying position spells by the number of patents held by a user.
* Explicit spell_* provenance fields are retained for future integrity checks.

This output deliberately does not carry patent-level rows. STEP4 reads STEP1
directly for patents and au_* profile variables.
"""

import argparse
import os
from pyspark.sql import SparkSession, Window, functions as F


POSITION_COLUMNS = [
    # Exact spell identity and employer mapping
    "position_id",
    "company_name",
    "rcid",
    "ultimate_parent_rcid",
    "ultimate_parent_company_name",

    # Spell dates/order
    "startdate",
    "enddate",
    "position_number",

    # Geography
    "city",
    "state",
    "country",
    "region",
    "metro_area",
    "msa",
    "location_raw",

    # Title / occupation
    "title_raw",
    "title_translated",
    "job_category_v2",
    "role_k50_v2",
    "role_k150_v2",
    "role_k300_v2",
    "role_k500_v2",
    "role_k1000_v2",
    "role_k1500_v2",
    "role_k10_v3",
    "role_k50_v3",
    "role_k150_v3",
    "role_k500_v3",
    "role_k1000_v3",
    "role_k1500_v3",
    "role_k5000_v3",
    "role_k10000_v3",
    "role_k15000_v3",
    "mapped_role_v3",

    # Occupation / industry
    "onet_code",
    "onet_title",
    "naics_code",
    "naics_description",
    "rics_k50",
    "rics_k200",
    "rics_k400",

    # Job characteristics
    "remote_suitability",
    "weight",
    "seniority",
    "salary",
    "start_salary",
    "end_salary",
    "total_compensation",
    "additional_compensation",
]


def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1-dir", required=True)
    ap.add_argument("--positions-dir", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "8")))
    ap.add_argument("--shuffle-partitions", type=int, default=400)
    ap.add_argument("--coalesce", type=int, default=80)
    ap.add_argument("--tmpdir", default=None)
    ap.add_argument("--shards", type=int, default=24)
    ap.add_argument("--shard-idx", type=int, default=int(os.environ.get("SLURM_ARRAY_TASK_ID", "0")))
    return ap.parse_args()


def shard_filter(df, shard_idx, shards, user_col="user_id"):
    df = df.withColumn(user_col, F.col(user_col).cast("string"))
    expr = F.pmod(F.abs(F.hash(F.col(user_col))), F.lit(shards))
    return df.withColumn("_shard", expr).filter(F.col("_shard") == shard_idx).drop("_shard")


def clean_string(c):
    return F.when(
        F.col(c).isNull()
        | (F.trim(F.col(c)) == "")
        | (F.lower(F.trim(F.col(c))) == "empty"),
        F.lit(None).cast("string"),
    ).otherwise(F.col(c))


def main():
    args = parse_args()
    if not (0 <= args.shard_idx < args.shards):
        raise SystemExit(f"--shard-idx must be in [0, {args.shards - 1}]")

    builder = (
        SparkSession.builder
        .appName(f"step2_inv_pos_v3_{args.shard_idx:02d}")
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

    print("=" * 96)
    print(f"[STEP2 V3] Canonical inventor-position spells | shard={args.shard_idx:02d}/{args.shards - 1:02d}")
    print(f"[STEP1]     {args.step1_dir}")
    print(f"[POSITIONS] {args.positions_dir}")
    print(f"[OUT]       {args.out_dir}")
    print("=" * 96)

    inventor_users = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.step1_dir)
        .select(F.col("user_id").cast("string").alias("user_id"))
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id"])
    )
    inventor_users = shard_filter(inventor_users, args.shard_idx, args.shards)

    positions_raw = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(args.positions_dir)
        .filter(F.col("user_id").isNotNull())
        .withColumn("user_id", F.col("user_id").cast("string"))
    )
    positions_raw = shard_filter(positions_raw, args.shard_idx, args.shards)

    available = set(positions_raw.columns)
    keep = [c for c in POSITION_COLUMNS if c in available]
    required = {"position_id", "startdate"}
    missing_required = required - set(keep)
    if missing_required:
        raise ValueError(f"Raw positions is missing required fields: {sorted(missing_required)}")

    # Left-semi join is intentional: it selects inventor users without duplicating
    # their spell rows by patent records or profile records.
    spells = (
        positions_raw
        .select("user_id", *keep)
        .join(inventor_users, on="user_id", how="left_semi")
        .filter(F.col("position_id").isNotNull())
        .withColumn("position_id", F.col("position_id").cast("long"))
        .withColumn("startdate", F.to_date("startdate"))
        .withColumn("enddate", F.to_date("enddate"))
    )

    for c in [
        "company_name", "ultimate_parent_company_name", "title_raw",
        "title_translated", "job_category_v2", "city", "state", "country",
        "region", "metro_area", "msa", "location_raw", "onet_code",
        "onet_title", "naics_code", "naics_description", "rics_k50",
        "rics_k200", "rics_k400",
    ]:
        if c in spells.columns:
            spells = spells.withColumn(c, clean_string(c))

    for c in ["rcid", "ultimate_parent_rcid"]:
        if c in spells.columns:
            spells = spells.withColumn(c, F.col(c).cast("long"))

    for c in [
        "position_number", "seniority", "salary", "start_salary",
        "end_salary", "total_compensation", "additional_compensation",
        "remote_suitability", "weight",
    ]:
        if c in spells.columns:
            spells = spells.withColumn(c, F.col(c).cast("double"))

    # Deterministic handling of accidental duplicate raw rows.
    # Crucially, selection occurs within a position_id only; no information is
    # borrowed from another spell.
    completeness = (
        F.when(F.col("rcid").isNotNull(), 1).otherwise(0)
        + F.when(F.col("company_name").isNotNull(), 1).otherwise(0)
        + F.when(F.col("title_raw").isNotNull(), 1).otherwise(0)
        + F.when(F.col("startdate").isNotNull(), 1).otherwise(0)
        + F.when(F.col("enddate").isNotNull(), 1).otherwise(0)
    )
    spells = spells.withColumn("_completeness", completeness)

    w_spell = Window.partitionBy("user_id", "position_id").orderBy(
        F.col("_completeness").desc(),
        F.col("enddate").desc_nulls_last(),
        F.col("startdate").desc_nulls_last(),
        F.col("position_number").desc_nulls_last(),
        F.col("rcid").desc_nulls_last(),
        F.col("company_name").desc_nulls_last(),
    )

    spells = (
        spells.withColumn("_rn", F.row_number().over(w_spell))
        .filter(F.col("_rn") == 1)
        .drop("_rn", "_completeness")
    )

    # Explicit provenance flags. These are never imputed by later steps.
    spells = (
        spells
        .withColumn("spell_has_rcid", F.col("rcid").isNotNull().cast("int"))
        .withColumn("spell_has_company_name", F.col("company_name").isNotNull().cast("int"))
        .withColumn("spell_has_parent_rcid", F.col("ultimate_parent_rcid").isNotNull().cast("int"))
        .withColumn("position_pipeline_version", F.lit("v3_spell_integrity"))
    )

    out_shard = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    (
        spells.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_shard)
    )

    n_rows = spells.count()
    n_users = spells.select("user_id").distinct().count()
    n_spells = spells.select("user_id", "position_id").distinct().count()

    if n_rows != n_spells:
        raise RuntimeError(
            f"STEP2 integrity failure: rows={n_rows:,}, distinct user-position spells={n_spells:,}"
        )

    print(f"[DONE] rows={n_rows:,} | users={n_users:,} | canonical spells={n_spells:,}")
    spark.stop()


if __name__ == "__main__":
    main()
