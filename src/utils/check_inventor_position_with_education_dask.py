#!/usr/bin/env python
"""
check_inventor_position_with_education_spark.py

Sanity checks on inventor_position_with_education dataset using PySpark.
Uses shards and avoids heavy global counts.
"""

import os
from pyspark.sql import SparkSession, functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("check_inventor_position_with_education_spark")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    path = "/labs/khanna/linkedin_202507/processed/inventor_position_with_education"
    print(f"[LOAD] {path}")

    df = spark.read.parquet(os.path.join(path, "*.parquet"))

    # --- 1. Sample rows
    sample = df.sample(withReplacement=False, fraction=0.0001, seed=42)
    print("\n[1] Sample rows:")
    for row in sample.limit(10).collect():
        print(row.asDict())

    # --- 2. Distribution of num_degrees
    print("\n[2] Distribution of num_degrees (sample):")
    sample.groupBy("num_degrees").count().orderBy("num_degrees").show(20, truncate=False)

    # --- 3. Null checks
    print("\n[3] Null checks (sample):")
    cols_to_check = ["first_university_name", "last_university_name", "num_degrees"]
    n_total = sample.count()
    for c in cols_to_check:
        n_null = sample.filter(F.col(c).isNull()).count()
        frac_null = n_null / max(n_total, 1)
        print(f"{c}: {frac_null:.2%} null")

    # --- 4. num_degrees stats
    print("\n[4] num_degrees stats (sample):")
    sample.describe(["num_degrees"]).show()

    # --- 5. Sample of user_id / position_id / num_degrees
    print("\n[5] Sample of user_id / position_id / num_degrees:")
    sample.select("user_id", "position_id", "num_degrees").limit(10).show(truncate=False)

    print("\n[OK] Finished sanity checks (sample-based)")

    spark.stop()

if __name__ == "__main__":
    main()
