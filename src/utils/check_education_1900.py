#!/usr/bin/env python3
"""
check_education_1900.py

Inspect inventor_position_education dataset for placeholder education dates (1900-01-01).
Prints a few examples with user_id, fullname, and education details.
"""

import os
from pyspark.sql import SparkSession, functions as F

def main():
    spark = (
        SparkSession.builder
        .appName("check_education_1900")
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # =============================
    # Paths and Data Load
    # =============================
    INPUT = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
    print(f"[INFO] Reading dataset from {INPUT} ...")
    df = spark.read.parquet(INPUT)

    print("[INFO] Columns available:")
    print(df.columns)

    # =============================
    # Inspect Education Dates
    # =============================
    print("\n[INFO] Checking for education start dates equal to 1900-01-01...")

    df_edu = df.filter(F.col("startdate_edu").isNotNull())

    edu_1900 = df_edu.filter(F.to_date("startdate_edu") == F.lit("1900-01-01"))

    count_1900 = edu_1900.count()
    print(f"[INFO] Found {count_1900:,} records with 1900-01-01 education start date.")

    # =============================
    # Show Examples
    # =============================
    if count_1900 > 0:
        print("\n[INFO] Sample of 20 records with 1900 education dates:\n")
        edu_1900.select(
            "user_id",
            "fullname",
            "university_name",
            "degree",
            "field",
            "startdate_edu",
            "enddate_edu"
        ).show(20, truncate=False)
    else:
        print("[INFO] ✅ No records with 1900-01-01 education dates found.")

    spark.stop()


if __name__ == "__main__":
    main()
