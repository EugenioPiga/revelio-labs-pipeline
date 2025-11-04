#!/usr/bin/env python
# %%
from pathlib import Path

from pyspark.sql import SparkSession, functions as F

# %%
# Configure input locations (edit as needed)
duplicates_path = Path("/home/gps-yuhei/revelio_labs/output/userid_pvid_correspondence_duplicates")

# %%
# Start Spark session
spark = (
    SparkSession.builder
    .appName("inspect_pvid_duplicates")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# %%
# Load all parquet files and cache consolidated DataFrame
duplicates_df = spark.read.parquet(str(duplicates_path))
duplicates_df.cache()
total_rows = duplicates_df.count()
print(f"[INFO] Loaded {total_rows:,} duplicate rows from {duplicates_path}")

# %%
# Inspect schema and a few sample rows
duplicates_df.printSchema()
duplicates_df.show(10, truncate=False)

# %%
# Summaries: user_id mapping to multiple PV IDs, and vice versa
user_conflict_ids = (
    duplicates_df.groupBy("user_id")
    .agg(F.countDistinct("PV_INVENTOR_ID").alias("n_pv_ids"))
    .filter(F.col("n_pv_ids") > 1)
)

user_conflicts = (
    duplicates_df.join(user_conflict_ids, on="user_id", how="inner")
    .select(
        "user_id",
        "PV_INVENTOR_ID",
        "INVENTOR_FULLNAME_CLEANED",
        "USER_FULLNAME_CLEANED",
        "n_pv_ids",
    )
    .dropDuplicates()
    .orderBy(F.desc("n_pv_ids"), "user_id", "PV_INVENTOR_ID")
)

pv_conflict_ids = (
    duplicates_df.groupBy("PV_INVENTOR_ID")
    .agg(F.countDistinct("user_id").alias("n_users"))
    .filter(F.col("n_users") > 1)
)

pv_conflicts = (
    duplicates_df.join(pv_conflict_ids, on="PV_INVENTOR_ID", how="inner")
    .select(
        "PV_INVENTOR_ID",
        "user_id",
        "INVENTOR_FULLNAME_CLEANED",
        "USER_FULLNAME_CLEANED",
        "n_users",
    )
    .dropDuplicates()
    .orderBy(F.desc("n_users"), "PV_INVENTOR_ID", "user_id")
)

print("\n[INFO] user_id mapped to multiple PV_INVENTOR_ID (one row per pair)")
user_conflicts.show(10, truncate=False)

print("\n[INFO] PV_INVENTOR_ID mapped to multiple user_id (one row per pair)")
pv_conflicts.show(10, truncate=False)

# %%
# Optional: examine a specific identifier by editing these values
sample_user_id = None  # e.g., "user:12345"
sample_pv_id = None    # e.g., "pv:67890"

if sample_user_id:
    print(f"\n[DETAIL] Rows for user_id = {sample_user_id}")
    duplicates_df.filter(F.col("user_id") == sample_user_id).show(truncate=False)

if sample_pv_id:
    print(f"\n[DETAIL] Rows for PV_INVENTOR_ID = {sample_pv_id}")
    duplicates_df.filter(F.col("PV_INVENTOR_ID") == sample_pv_id).show(truncate=False)

# %%
# Keep Spark session alive for further interactive exploration
print("\n[READY] Spark session `spark` and DataFrame `duplicates_df` are available for interactive use.")
