#!/usr/bin/env python3
###############################################################################
# step2_inventors_matched_positions_spark.py
#
# Build inventor-position dataset: user_id × position_id.
#
# Input:
#   STEP1:
#     inventor/patent matches already joined to academic_individual_user,
#     including au_* user/profile variables.
#
#   POSITIONS:
#     raw academic_individual_position parquet files.
#
# Output:
#   OUT_DIR/shard=XX
#
# What this does:
#   1. Reads STEP1.
#   2. Reads raw position spells.
#   3. Deterministically shards both datasets by hash(user_id) % shards.
#   4. Joins STEP1 to positions on user_id.
#   5. Keeps a rich set of position variables needed later:
#        - role clusters
#        - O*NET code/title
#        - NAICS code/description
#        - RICS industry groups
#        - remote_suitability
#        - seniority/salary/compensation
#        - firm and geography fields
#   6. Writes one shard.
###############################################################################

import os
import argparse
from pyspark.sql import SparkSession, functions as F


# =============================================================================
# Helpers
# =============================================================================

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1-dir", required=True, help="Path to STEP1 parquet")
    ap.add_argument("--positions-dir", required=True, help="Path to POSITIONS parquet")
    ap.add_argument("--out-dir", required=True, help="Output base directory")

    ap.add_argument("--threads", type=int, default=8, help="Spark threads")
    ap.add_argument("--shuffle-partitions", type=int, default=200, help="Spark shuffle partitions")
    ap.add_argument("--coalesce", type=int, default=80, help="Output files per shard")
    ap.add_argument("--tmpdir", default=None, help="Scratch directory for Spark spill")

    ap.add_argument("--shards", type=int, default=1, help="Total number of shards")
    ap.add_argument("--shard-idx", type=int, default=0, help="This shard index, 0 to shards-1")
    return ap.parse_args()


def shard_filter(df, shard_idx: int, shards: int, user_col: str = "user_id"):
    """
    Deterministic user-level sharding.
    This makes Step2 output shard=XX contain only the users assigned to that shard.
    """
    df = df.withColumn(user_col, F.col(user_col).cast("string"))
    shard_expr = F.pmod(F.abs(F.hash(F.col(user_col))), F.lit(shards))
    return df.withColumn("_shard", shard_expr).filter(F.col("_shard") == shard_idx).drop("_shard")


def existing_cols(df, cols):
    """Return columns from cols that exist in df."""
    return [c for c in cols if c in df.columns]


# =============================================================================
# Main
# =============================================================================

def main():
    args = parse_args()

    if args.shard_idx < 0 or args.shard_idx >= args.shards:
        raise SystemExit(f"--shard-idx must be in [0,{args.shards - 1}]")

    # -------------------------------------------------------------------------
    # Spark session
    # -------------------------------------------------------------------------

    builder = (
        SparkSession.builder
        .appName(f"step2_inv_pos_shard_{args.shard_idx}_of_{args.shards}")
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

    print("=" * 80)
    print("[STEP2] Inventors matched to positions")
    print(f"[CONF] shard={args.shard_idx}/{args.shards - 1}")
    print(f"[CONF] step1-dir={args.step1_dir}")
    print(f"[CONF] positions-dir={args.positions_dir}")
    print(f"[CONF] out-dir={args.out_dir}")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # 1. Load STEP1
    # -------------------------------------------------------------------------

    print("[1/5] Loading STEP1 ...")
    step1 = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.step1_dir)
             .filter(F.col("user_id").isNotNull())
    )

    step1 = shard_filter(step1, args.shard_idx, args.shards, user_col="user_id")
    step1_cols = step1.columns

    print(f"      STEP1 columns: {len(step1_cols)}")
    print(f"      STEP1 sample columns: {step1_cols[:25]}")

    # -------------------------------------------------------------------------
    # 2. Load POSITIONS
    # -------------------------------------------------------------------------

    print("[2/5] Loading positions ...")
    positions = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.positions_dir)
             .filter(F.col("user_id").isNotNull())
    )

    positions = shard_filter(positions, args.shard_idx, args.shards, user_col="user_id")
    pos_cols = positions.columns

    print(f"      Position columns: {len(pos_cols)}")
    print(f"      Position sample columns: {pos_cols[:40]}")

    # -------------------------------------------------------------------------
    # 3. Keep rich position-side variables
    # -------------------------------------------------------------------------

    desired_pos_cols = [
        # IDs / employer
        "position_id",
        "company_name",
        "rcid",
        "ultimate_parent_rcid",
        "ultimate_parent_company_name",

        # Dates
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

        # Raw title / occupation
        "title_raw",
        "title_translated",
        "job_category_v2",

        # Revelio role clusters: v2
        "role_k50_v2",
        "role_k150_v2",
        "role_k300_v2",
        "role_k500_v2",
        "role_k1000_v2",
        "role_k1500_v2",

        # Revelio role clusters: v3
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

        # O*NET
        "onet_code",
        "onet_title",

        # Industry
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

    keep_pos_cols = existing_cols(positions, desired_pos_cols)

    # Avoid duplicate column names from STEP1 except user_id.
    keep_pos_cols = [c for c in keep_pos_cols if c not in step1_cols]

    missing_pos_cols = [c for c in desired_pos_cols if c not in pos_cols]
    print(f"[INFO] Keeping {len(keep_pos_cols)} position columns.")
    if missing_pos_cols:
        print("[WARN] These desired position columns were not found and will be skipped:")
        for c in missing_pos_cols:
            print(f"       - {c}")

    positions_sel = positions.select("user_id", *keep_pos_cols)

    # -------------------------------------------------------------------------
    # 4. Join
    # -------------------------------------------------------------------------

    print("[3/5] Joining STEP1 with POSITIONS on user_id ...")

    joined_final = positions_sel.join(step1, on="user_id", how="inner")

    print(f"[INFO] Joined output columns: {len(joined_final.columns)}")
    print("[INFO] Joined columns:")
    for c in joined_final.columns:
        print(f"  - {c}")

    # -------------------------------------------------------------------------
    # 5. Write output
    # -------------------------------------------------------------------------

    print("[4/5] Writing output ...")
    out_path = os.path.join(args.out_dir, f"shard={args.shard_idx:02d}")
    os.makedirs(out_path, exist_ok=True)

    (
        joined_final.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_path)
    )

    # Optional success flag
    try:
        with open(os.path.join(out_path, "_SUCCESS_STEP2"), "w") as f:
            f.write("")
    except Exception:
        pass

    # -------------------------------------------------------------------------
    # 6. Stats
    # -------------------------------------------------------------------------

    print("[5/5] Stats ...")
    try:
        n_rows = joined_final.count()
        n_users = joined_final.select("user_id").distinct().count()
        n_pos = (
            joined_final.select("position_id").distinct().count()
            if "position_id" in joined_final.columns else 0
        )
        print(f"==> Done. Rows: {n_rows:,} | distinct users: {n_users:,} | distinct positions: {n_pos:,}")
    except Exception as e:
        print(f"[WARN] Stats skipped: {e}")

    spark.stop()
    print("[INFO] Step2 finished successfully.")


if __name__ == "__main__":
    main()
