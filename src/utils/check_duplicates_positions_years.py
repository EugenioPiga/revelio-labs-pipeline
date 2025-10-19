#!/usr/bin/env python3
###############################################################################
# check_duplicate_positions_and_years.py
# Diagnose source of duplicate (user_id, year) pairs in inventor_year
# Author: Eugenio
###############################################################################

from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("check_duplicate_positions_and_years")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================
# CONFIG
# ======================
POS_PATH = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
SHARD_PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_shards"
SHARD_EXAMPLE = 0   # choose a small shard index to inspect

print(f"[INFO] Reading position data from: {POS_PATH}")
pos = spark.read.parquet(POS_PATH).filter(F.col("shard") == SHARD_EXAMPLE)

print(f"[INFO] Reading merged year-level shard {SHARD_EXAMPLE} from: {SHARD_PATH}")
year_shard = spark.read.parquet(f"{SHARD_PATH}/inventor_year_shard={SHARD_EXAMPLE}.parquet")

# ======================
# 1️⃣ Check duplicate position spells before expansion
# ======================
dup_positions = (
    pos.groupBy("user_id", "position_id", "startdate", "enddate")
       .count()
       .filter("count > 1")
)

dup_pos_count = dup_positions.count()
print(f"[CHECK] Duplicate position spells: {dup_pos_count:,}")
if dup_pos_count > 0:
    dup_positions.show(10)

# ======================
# 2️⃣ Check duplicate (user_id, year) pairs in year-level shard
# ======================
dup_years = (
    year_shard.groupBy("user_id", "year")
              .count()
              .filter("count > 1")
)
dup_year_count = dup_years.count()
print(f"[CHECK] Duplicate (user_id, year) pairs in year shard: {dup_year_count:,}")

if dup_year_count > 0:
    dup_years.show(10)

# ======================
# 3️⃣ Cross-check if duplicated user_ids in shard also have duplicate spells
# ======================
dup_user_ids = [r["user_id"] for r in dup_years.select("user_id").distinct().limit(10).collect()]
print(f"[INFO] Sampling up to 10 users with duplicate years: {dup_user_ids}")

for uid in dup_user_ids:
    print(f"\n[DEBUG] User {uid}: checking raw position spells")
    pos.filter(F.col("user_id") == uid) \
       .select("position_id", "startdate", "enddate", "rcid", "title", "company") \
       .orderBy("startdate") \
       .show(20, truncate=False)

print("\n[INFO] Diagnostic completed successfully.")
spark.stop()
