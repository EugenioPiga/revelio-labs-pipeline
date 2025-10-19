#!/usr/bin/env python3
###############################################################################
# check_shard_inventor_year.py
# Sanity checks for inventor_year_shards output
# Author: Eugenio
###############################################################################

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# =========================================================
# Spark setup
# =========================================================
spark = (
    SparkSession.builder
    .appName("check_inventor_year_shards")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# =========================================================
# Paths and read
# =========================================================
path = "/labs/khanna/linkedin_202507/processed/inventor_year_shards"

print("[INFO] Reading all shards...")
df_all = spark.read.parquet(path)

print("[INFO] Total rows:", df_all.count())
print("[INFO] Total unique users:", df_all.select("user_id").distinct().count())
print("\n[INFO] Number of columns:", len(df_all.columns))
print(df_all.columns)

# =========================================================
# Schema and sample from one shard
# =========================================================
print("\n[INFO] Reading sample shard 0...")
df = spark.read.parquet(f"{path}/inventor_year_shard=0.parquet")
df.printSchema()
df.show(5, truncate=False)

# =========================================================
# Check forward fill completeness
# =========================================================
print("\n[CHECK] Any user with null first_rcid after entry?")

null_users = (
    df_all.groupBy("user_id")
    .agg(F.sum(F.when(F.col("first_rcid").isNull(), 1).otherwise(0)).alias("null_count"))
    .filter("null_count > 0")
    .count()
)

if null_users == 0:
    print("[OK] All users have non-null first_rcid after entry.")
else:
    print(f"[WARN] {null_users} users have at least one null first_rcid.")

# =========================================================
# Example user trajectory (replace with real ID)
# =========================================================
example_user = (
    df_all
    .filter(F.col("first_rcid").isNotNull())
    .select("user_id")
    .limit(1)
    .collect()[0][0]
)
print(f"\n[INFO] Example user_id: {example_user}")
df_all.filter(df_all.user_id == example_user).orderBy("year").select("year", "first_rcid").show(20)

# =========================================================
# Missingness summary
# =========================================================
print("\n[INFO] Computing column-wise missingness...")
null_summary = (
    df_all.select([F.mean(F.col(c).isNull().cast("double")).alias(c) for c in df_all.columns])
    .toPandas().T
)
null_summary.columns = ["share_null"]
print(null_summary.sort_values("share_null"))

# =========================================================
# Done
# =========================================================
print("\n[INFO] Check completed successfully.")
spark.stop()

