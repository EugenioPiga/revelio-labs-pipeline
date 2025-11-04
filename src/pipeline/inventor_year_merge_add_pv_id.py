#!/usr/bin/env python
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
import sys
from pyspark.sql import SparkSession, functions as F

DEFAULT_MATCHES_PATH = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
DEFAULT_INVENTOR_YEAR_PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
DEFAULT_CORRESPONDENCE_PATH = "/labs/khanna/linkedin_202507/processed/userid_pvid_correspondence"
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
    parser.add_argument("--output-path", default=DEFAULT_OUTPUT_PATH,
                        help="Destination for inventor_year_merged enriched with PV_INVENTOR_ID.")
    parser.add_argument("--shuffle-partitions", type=int, default=400,
                        help="spark.sql.shuffle.partitions to use for joins.")
    return parser.parse_args()


def ensure_one_to_one(df):
    """Validate that each user_id and PV_INVENTOR_ID participate in a single pair."""
    multi_user = (
        df.groupBy("user_id")
        .agg(F.countDistinct("PV_INVENTOR_ID").alias("n_ids"))
        .filter(F.col("n_ids") > 1)
    )
    multi_user_cnt = multi_user.count()
    if multi_user_cnt:
        sample = multi_user.orderBy(F.desc("n_ids")).limit(20).collect()
        raise ValueError(
            f"Found {multi_user_cnt} user_id values mapping to multiple PV_INVENTOR_IDs. "
            f"Examples: {sample}"
        )

    multi_pvid = (
        df.groupBy("PV_INVENTOR_ID")
        .agg(F.countDistinct("user_id").alias("n_users"))
        .filter(F.col("n_users") > 1)
    )
    multi_pvid_cnt = multi_pvid.count()
    if multi_pvid_cnt:
        sample = multi_pvid.orderBy(F.desc("n_users")).limit(20).collect()
        raise ValueError(
            f"Found {multi_pvid_cnt} PV_INVENTOR_ID values mapping to multiple user_id entries. "
            f"Examples: {sample}"
        )


def main() -> None:
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("inventor_year_merge_add_pv_id")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[1/5] Reading revelio_patents_inventor_matches …")
    matches = (
        spark.read.parquet(args.matches_path)
        .select("user_id", "PV_INVENTOR_ID")
    )
    pairs = (
        matches
        .filter(F.col("user_id").isNotNull() & F.col("PV_INVENTOR_ID").isNotNull())
        .withColumn("user_id", F.trim(F.col("user_id").cast("string")))
        .withColumn("PV_INVENTOR_ID", F.trim(F.col("PV_INVENTOR_ID").cast("string")))
        .dropDuplicates(["user_id", "PV_INVENTOR_ID"])
    )

    print("[2/5] Validating one-to-one correspondence …")
    ensure_one_to_one(pairs)

    correspondence = pairs.repartition("user_id").cache()
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
