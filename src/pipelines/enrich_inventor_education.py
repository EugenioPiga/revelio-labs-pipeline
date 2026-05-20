#!/usr/bin/env python
"""
enrich_inventor_education.py

Take inventors_matched_education_spark (step3 output) and enrich with extra vars
from academic_individual_user_education (original LinkedIn education data).

Adds: university_country, university_location, education_number
"""

import os
from pyspark.sql import SparkSession

def main():
    spark = (
        SparkSession.builder
        .appName("enrich_inventor_education")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Paths
    matched_dir = "output/inventors_matched_education_spark"
    original_dir = "academic_individual_user_education"
    out_dir = "output/inventors_matched_education_enriched"

    print("[1/4] Load matched dataset …")
    matched = spark.read.parquet(os.path.join(matched_dir, "*"))

    print("[2/4] Load original education dataset (only needed cols) …")
    original = spark.read.parquet(os.path.join(original_dir, "*")).select(
        "user_id", "university_country", "university_location", "education_number"
    )

    print("[3/4] Join on user_id …")
    enriched = matched.join(original, on="user_id", how="left")

    print("[4/4] Write out …")
    (
        enriched
        .repartition(200)   # smaller partitions = lighter memory
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_dir)
    )

    # Quick stats
    n_users = enriched.select("user_id").distinct().count()
    n_rows = enriched.count()
    print(f"==> Done. Rows: {n_rows:,} | distinct users: {n_users:,}")
    spark.stop()

if __name__ == "__main__":
    main()
