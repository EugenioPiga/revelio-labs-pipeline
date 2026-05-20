#!/usr/bin/env python
# aggregate_patent_position_to_inventor_position.py
# Collapse patent–position dataset -> inventor–position dataset

import os
import argparse
from pyspark.sql import SparkSession, functions as F

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input patent–position dataset (parquet)")
    ap.add_argument("--out-dir", required=True, help="Output directory for inventor–position dataset")
    ap.add_argument("--threads", type=int, default=8, help="Spark threads")
    ap.add_argument("--shuffle-partitions", type=int, default=400, help="Shuffle partitions")
    ap.add_argument("--coalesce", type=int, default=200, help="Output files")
    return ap.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("aggregate_patent_to_inventor_position")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    print("[1/3] Load dataset …")
    df = spark.read.parquet(os.path.join(args.input, "*"))

    # --- aggregation ---
    print("[2/3] Aggregate patents per (user_id, position_id) …")
    agg_df = (
        df.groupBy("user_id", "position_id")
          .agg(
              F.countDistinct("patent_id").alias("num_patents"),
              F.min("filing_date").alias("first_filing_date"),
              F.max("filing_date").alias("last_filing_date"),
              F.min("patent_date").alias("first_patent_date"),
              F.max("patent_date").alias("last_patent_date"),
              # example: collect list of patent_ids (optional)
              F.collect_set("patent_id").alias("patent_ids")
          )
    )

    # --- join back with static info (position + inventor metadata) ---
    # We assume metadata does not vary within (user_id, position_id)
    static_cols = [
        "company_name", "rcid", "title_raw", "job_category_v2", "seniority", "salary",
        "startdate", "enddate", "city", "state", "country", "region", "metro_area",
        "location_raw", "total_compensation", "start_salary", "end_salary",
        "user_fullname", "user_firstname", "user_lastname",
        "user_fullname_cleaned", "user_firstname_cleaned", "user_lastname_cleaned",
        "user_profile_linkedin_url"
    ]
    static_df = df.select("user_id", "position_id", *[c for c in static_cols if c in df.columns]).dropDuplicates(["user_id","position_id"])

    final_df = (
        agg_df.join(static_df, on=["user_id","position_id"], how="left")
    )

    print("[3/3] Write output …")
    (
        final_df.coalesce(args.coalesce)
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(args.out_dir)
    )

    print("==> Done.")
    spark.stop()

if __name__ == "__main__":
    main()

