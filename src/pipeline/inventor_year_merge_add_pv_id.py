#!/usr/bin/env python
from __future__ import annotations
"""
inventor_year_merge_add_pv_id.py

Attach PV inventor identifiers to the balanced inventor-year panel.

Steps:
1. Read the raw matches (revelio_patents_inventor_matches) and construct a
   one-to-one correspondence between user_id and PV_INVENTOR_ID.
2. Persist that correspondence.
3. Join the correspondence onto inventor_year_merged and write the enriched
   dataset.
"""

import argparse
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession, functions as F

DEFAULT_MATCHES_PATH = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
DEFAULT_INVENTOR_YEAR_PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
DEFAULT_CORRESPONDENCE_PATH = "/labs/khanna/linkedin_202507/processed/userid_pvid_correspondence"
DEFAULT_DUPLICATES_PATH = os.path.expanduser("~/revelio_labs/output/userid_pvid_correspondence_duplicates")
DEFAULT_OUTPUT_PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_with_pvid"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Attach PV inventor IDs to inventor_year_merged.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--matches-path", default=DEFAULT_MATCHES_PATH,
                        help="Parquet directory with revelio_patents_inventor_matches.")
    parser.add_argument("--inventor-year-path", default=DEFAULT_INVENTOR_YEAR_PATH,
                        help="Parquet directory containing the balanced inventor-year panel.")
    parser.add_argument("--correspondence-path", default=DEFAULT_CORRESPONDENCE_PATH,
                        help="Destination for the user_id ↔ PV_INVENTOR_ID mapping parquet.")
    parser.add_argument("--duplicates-path", default=DEFAULT_DUPLICATES_PATH,
                        help="Destination for the user_id ↔ PV_INVENTOR_ID pairs involved in duplicates.")
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH,
                        help="Destination for inventor_year_merged enriched with PV_INVENTOR_ID.")
    parser.add_argument("--user-id-col", default="user_id",
                        help="Column name in matches dataset representing the internal user identifier.")
    parser.add_argument("--pv-id-col", default="PV_INVENTOR_ID",
                        help="Column name in matches dataset representing the PV inventor identifier.")
    parser.add_argument("--inventor-name-col", default="INVENTOR_FULLNAME_CLEANED",
                        help="Optional column with inventor full name for diagnostics.")
    parser.add_argument("--user-name-col", default="USER_FULLNAME_CLEANED",
                        help="Optional column with LinkedIn user full name for diagnostics.")
    parser.add_argument("--shuffle-partitions", type=int, default=400,
                        help="spark.sql.shuffle.partitions to use for joins.")
    return parser.parse_args()

def resolve_column(columns: list[str], preferred: list[str], desc: str, required: bool = True) -> str | None:
    """Return the first matching column (case-insensitive)."""
    lower_map = {c.lower(): c for c in columns}
    for name in preferred:
        if name and name.lower() in lower_map:
            return lower_map[name.lower()]
    if required:
        raise RuntimeError(
            f"Could not resolve column for {desc}. Tried: {preferred}. "
            f"Available columns: {sorted(columns)}"
        )
    return None

def ensure_one_to_one(pairs_df, detail_df, duplicates_path: str):
    """Validate that each user_id and PV_INVENTOR_ID participate in a single pair."""
    multi_user = (
        pairs_df.groupBy("user_id")
        .agg(F.countDistinct("PV_INVENTOR_ID").alias("n_ids"))
        .filter(F.col("n_ids") > 1)
    )
    multi_user_cnt = multi_user.count()
    if multi_user_cnt:
        print(f"[ERROR] Found {multi_user_cnt} user_id values mapping to multiple PV_INVENTOR_IDs.")

    multi_pvid = (
        pairs_df.groupBy("PV_INVENTOR_ID")
        .agg(F.countDistinct("user_id").alias("n_users"))
        .filter(F.col("n_users") > 1)
    )
    multi_pvid_cnt = multi_pvid.count()
    if multi_pvid_cnt:
        print(f"[ERROR] Found {multi_pvid_cnt} PV_INVENTOR_ID values mapping to multiple user_id entries.")

    if multi_user_cnt or multi_pvid_cnt:
        dup_dfs = []
        if multi_user_cnt:
            dup_user_ids = multi_user.select("user_id")
            dup_dfs.append(
                detail_df.join(dup_user_ids, on="user_id", how="inner")
            )
        if multi_pvid_cnt:
            dup_pvid_ids = multi_pvid.select("PV_INVENTOR_ID")
            dup_dfs.append(
                detail_df.join(dup_pvid_ids, on="PV_INVENTOR_ID", how="inner")
            )
        duplicates = dup_dfs[0]
        for extra_df in dup_dfs[1:]:
            duplicates = duplicates.unionByName(extra_df, allowMissingColumns=True)
        duplicates = duplicates.dropDuplicates()
        print(f"[INFO] Writing duplicate mappings to {duplicates_path}")
        (
            duplicates
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(duplicates_path)
        )
        raise ValueError(
            f"Non one-to-one mapping detected. Duplicates written to {duplicates_path}. "
            f"user_id duplicates: {multi_user_cnt:,}, PV_INVENTOR_ID duplicates: {multi_pvid_cnt:,}."
        )


def main() -> None:
    args = parse_args()

    # Expand user shortcuts for file system paths
    args.matches_path = os.path.expanduser(args.matches_path)
    args.inventor_year_path = os.path.expanduser(args.inventor_year_path)
    args.correspondence_path = os.path.expanduser(args.correspondence_path)
    args.duplicates_path = os.path.expanduser(args.duplicates_path)
    args.output_path = os.path.expanduser(args.output_path)

    # Ensure parent directories exist where we have write access
    for p in [args.correspondence_path, args.duplicates_path, args.output_path]:
        parent = Path(p).expanduser().parent
        parent.mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("inventor_year_merge_add_pv_id")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] Reading revelio_patents_inventor_matches …")
    matches_raw = spark.read.parquet(args.matches_path)
    columns = matches_raw.columns

    user_col = resolve_column(columns, [args.user_id_col], "user_id")
    pv_preferred = [
        args.pv_id_col,
        "pv_inventor_id",
        "pv_inventorid",
        "pv_inventor",
        "pvid",
        "pv_id",
        "inventor_id",
        "pv_person_id",
    ]
    pv_col = resolve_column(columns, pv_preferred, "PV inventor ID")
    inventor_name_col = resolve_column(
        columns,
        [args.inventor_name_col, "inventor_fullname_cleaned", "inventor_fullname", "inventor_name"],
        "inventor name",
        required=False,
    )
    user_name_col = resolve_column(
        columns,
        [args.user_name_col, "user_fullname_cleaned", "user_fullname", "user_name"],
        "user name",
        required=False,
    )

    print(f"[INFO] Resolved columns -> user_id: {user_col}, PV ID: {pv_col}")
    if inventor_name_col:
        print(f"[INFO] Using inventor name column: {inventor_name_col}")
    if user_name_col:
        print(f"[INFO] Using user name column: {user_name_col}")

    select_exprs = [
        F.col(user_col).alias("user_id"),
        F.col(pv_col).alias("PV_INVENTOR_ID"),
    ]
    optional_selects = []
    if inventor_name_col:
        optional_selects.append(F.col(inventor_name_col).alias("INVENTOR_FULLNAME_CLEANED"))
    if user_name_col:
        optional_selects.append(F.col(user_name_col).alias("USER_FULLNAME_CLEANED"))
    select_exprs.extend(optional_selects)

    matches = matches_raw.select(*select_exprs)

    pairs = (
        matches
        .filter(F.col("user_id").isNotNull() & F.col("PV_INVENTOR_ID").isNotNull())
        .withColumn("user_id", F.trim(F.col("user_id").cast("string")))
        .withColumn("PV_INVENTOR_ID", F.trim(F.col("PV_INVENTOR_ID").cast("string")))
    )
    pair_cols = ["user_id", "PV_INVENTOR_ID"]
    pairs_detail = pairs.dropDuplicates(pair_cols + [c for c in ["INVENTOR_FULLNAME_CLEANED", "USER_FULLNAME_CLEANED"] if c in pairs.columns])
    pairs_unique = pairs_detail.select(*pair_cols).dropDuplicates(pair_cols)

    print("[2/5] Validating one-to-one correspondence …")
    ensure_one_to_one(pairs_unique, pairs_detail, args.duplicates_path)

    correspondence = pairs_unique.repartition("user_id").cache()
    correspondence_count = correspondence.count()
    print(f"     -> Correspondence rows: {correspondence_count:,}")

    print("[3/5] Writing user_id ↔ PV_INVENTOR_ID mapping …")
    (
        correspondence
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.correspondence_path)
    )
    print(f"     -> Saved correspondence to {args.correspondence_path}")

    print("[4/5] Reading inventor_year_merged …")
    inventor_year = spark.read.parquet(args.inventor_year_path)
    if "user_id" not in inventor_year.columns:
        raise RuntimeError("inventor_year_merged is missing the 'user_id' column.")

    inventor_year = inventor_year.withColumn("user_id", F.trim(F.col("user_id").cast("string")))
    inventor_year = inventor_year.repartition("user_id")

    print("[5/5] Joining PV IDs onto inventor_year_merged …")
    enriched = inventor_year.join(correspondence, on="user_id", how="left")

    missing_count = enriched.filter(F.col("PV_INVENTOR_ID").isNull()).count()
    if missing_count:
        print(f"[WARN] {missing_count:,} inventor-year rows have no PV_INVENTOR_ID match.")
    else:
        print("[INFO] All inventor-year rows received a PV_INVENTOR_ID.")

    correspondence.unpersist()

    (
        enriched
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.output_path)
    )
    print(f"==> Done. Enriched dataset written to {args.output_path}")

    spark.stop()


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover
        print(f"[ERROR] {exc}", file=sys.stderr)
        raise
