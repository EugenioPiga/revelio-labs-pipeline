#!/usr/bin/env python3
"""
merge_position_education_spark.py
Merge inventor_position_file with inventors_matched_education_enriched using PySpark.
Renames duplicate columns from education with suffix '_edu'.
"""

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def main(pos_dir, edu_dir, out_dir, threads, shuffle_partitions, coalesce, tmpdir):
    spark = (
        SparkSession.builder
        .appName("MergeInventorPositionEducation")
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .config("spark.local.dir", tmpdir)
        .config("spark.driver.memory", "90g")
        .config("spark.executor.memory", "90g")
        .getOrCreate()
    )

    print("[SPARK] Reading inventor_position_file ...")
    df_pos = spark.read.parquet(pos_dir).withColumn("user_id", col("user_id").cast("string"))

    print("[SPARK] Reading inventors_matched_education_enriched ...")
    df_edu = spark.read.parquet(edu_dir).withColumn("user_id", col("user_id").cast("string"))

    # Automatically rename duplicate columns in df_edu
    duplicates = set(df_pos.columns).intersection(set(df_edu.columns)) - {"user_id"}
    for colname in duplicates:
        df_edu = df_edu.withColumnRenamed(colname, f"{colname}_edu")

    print(f"[SPARK] Renamed duplicates from education: {duplicates}")

    print("[SPARK] Performing merge on user_id ...")
    df_merged = df_pos.join(df_edu, on="user_id", how="left")

    print(f"[SPARK] Writing merged dataset to {out_dir} ...")
    (
        df_merged
        .coalesce(int(coalesce))  # reduce number of output files
        .write
        .mode("overwrite")
        .parquet(out_dir)
    )

    print("[SPARK] Done!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pos-dir", required=True, help="Path to inventor_position_file parquet dir")
    parser.add_argument("--edu-dir", required=True, help="Path to inventors_matched_education_enriched parquet dir")
    parser.add_argument("--out-dir", required=True, help="Output path for merged dataset")
    parser.add_argument("--threads", type=int, default=56, help="Number of Spark threads")
    parser.add_argument("--shuffle-partitions", type=int, default=200, help="Shuffle partitions")
    parser.add_argument("--coalesce", type=int, default=80, help="Coalesce output partitions")
    parser.add_argument("--tmpdir", type=str, required=True, help="Scratch tmpdir for Spark")
    args = parser.parse_args()

    main(args.pos_dir, args.edu_dir, args.out_dir, args.threads, args.shuffle_partitions, args.coalesce, args.tmpdir)
