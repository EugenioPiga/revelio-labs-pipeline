#!/usr/bin/env python

from pyspark.sql import SparkSession
from pathlib import Path

# paths (adjust if needed)
DATA_DIR = Path("output/inventors_matched_education_spark")
EDU_DIR = Path("academic_individual_user_education")

spark = SparkSession.builder \
    .appName("TestEnrichEdu") \
    .master("local[4]") \
    .getOrCreate()

# --- Load a small sample (just one shard/file each)
matched = spark.read.parquet(str(DATA_DIR / "shard=00")).limit(1000)

# pick one education file (first one)
edu_file = list(EDU_DIR.glob("individual_user_education_0000_part_00.parquet"))[0]
original = spark.read.parquet(str(edu_file)).limit(1000)

print("Matched cols:", matched.columns)
print("Original cols:", original.columns)

# --- Select extra fields from original
extra_cols = [
    "university_country", "university_location", "education_number"
]
edu_extra = original.select("user_id", *[c for c in extra_cols if c in original.columns])

# --- Join: add missing fields to matched
enriched = matched.join(edu_extra, on="user_id", how="left")

print("Enriched cols:", enriched.columns)

# --- Quick check
enriched.show(5, truncate=False)
