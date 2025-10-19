#!/usr/bin/env python3
###############################################################################
# merge_inventor_year_shards.py
# Robust merge of inventor_year shards with consistent schema
# Author: Eugenio
###############################################################################

from pyspark.sql import SparkSession, functions as F
import os
from functools import reduce

# ==============================
# Spark setup
# ==============================
spark = (
    SparkSession.builder
    .appName("merge_inventor_year_global_fix")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.driver.memory", "32g")
    .config("spark.executor.memory", "32g")
    .config("spark.sql.files.maxPartitionBytes", "256m")
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ==============================
# Helper diagnostic
# ==============================
def check_snapshot(df, label, n=5):
    """Print duplicates, column names, and sample for debugging."""
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")
    row_count = df.count()
    print(f"[INFO] Total rows: {row_count:,}")

    has_year = "year" in df.columns
    if has_year:
        dupes = (
            df.groupBy("user_id", "year")
              .count()
              .filter(F.col("count") > 1)
        )
        dup_ct = dupes.count()
        print(f"[CHECK] {dup_ct:,} duplicate (user_id, year) pairs.")
        if dup_ct > 0:
            dupes.show(5, truncate=False)
    else:
        dupes = (
            df.groupBy("user_id")
              .count()
              .filter(F.col("count") > 1)
        )
        dup_ct = dupes.count()
        print(f"[CHECK] {dup_ct:,} duplicate user_id (no year column).")
        if dup_ct > 0:
            dupes.show(5, truncate=False)

    print("[SAMPLE]")
    df.show(n, truncate=False)

# ==============================
# CONFIG PATHS
# ==============================
in_dir = "/labs/khanna/linkedin_202507/processed/inventor_year_shards"
out_path = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"

print(f"[INFO] Reading all inventor_year_shard directories from {in_dir} ...")

# Collect all shard folders
shards = sorted(
    [os.path.join(in_dir, d) for d in os.listdir(in_dir) if d.startswith("inventor_year_shard=")]
)
if not shards:
    raise ValueError(f"No shard directories found in {in_dir}")

# ==============================
# 1. Read each shard and normalize schema
# ==============================
df_list = []
for s in shards:
    print(f"[DEBUG] Reading {s}")
    tmp = spark.read.parquet(s)
    tmp = (
        tmp.withColumn("user_id", F.col("user_id").cast("long"))
           .withColumn("year", F.col("year").cast("int"))
    )
    df_list.append(tmp)

print(f"[INFO] Unioning {len(df_list)} shards ...")
df = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), df_list)

check_snapshot(df, "After UNION of all shards")


# ==============================
# 2. Compute global time-invariant fields
# ==============================
print("[INFO] Computing global invariant fields per inventor...")

# Convert date-like fields safely
date_cols = ["first_startdate_pos", "first_startdate_edu", "first_enddate_edu"]
for c in date_cols:
    if c in df.columns:
        df = df.withColumn(c, F.to_date(F.col(c)))

agg_dates = (
    df.groupBy("user_id")
      .agg(
          F.min("first_startdate_pos").alias("global_first_startdate_pos"),
          F.min("first_startdate_edu").alias("global_first_startdate_edu"),
          F.max("first_enddate_edu").alias("global_last_enddate_edu")
      )
)

check_snapshot(agg_dates, "Aggregated invariant dates")

print("[INFO] Joining global invariant fields back to main DataFrame...")
df = (
    df.drop(*[c for c in date_cols if c in df.columns])
      .join(agg_dates, on="user_id", how="left")
      .withColumnRenamed("global_first_startdate_pos", "first_startdate_pos")
      .withColumnRenamed("global_first_startdate_edu", "first_startdate_edu")
      .withColumnRenamed("global_last_enddate_edu", "first_enddate_edu")
)

check_snapshot(df, "Before dropping duplicates")

# ==============================
# 3. Drop duplicates just in case
# ==============================
print("[INFO] Removing duplicate (user_id, year) pairs if any...")
df = df.dropDuplicates(["user_id", "year"])

check_snapshot(df, "After dropping duplicates")


# ==============================
# 4. Data Cleaning
# ==============================

print("[INFO] Cleaning city/country and position fields...")

## 4.1 Replace empty string city/country with NULL
for colname in ["first_city", "first_country"]:
    if colname in df.columns:
        df = df.withColumn(
            colname,
            F.when((F.col(colname) == "") | (F.trim(F.col(colname)) == "empty"), F.lit(None))
             .otherwise(F.col(colname))
        )

## 4.2 Forward-fill n_positions = 1 for missing years after first valid observation
if "n_positions" in df.columns:
    w = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
    # year of first non-null n_positions
    first_valid_year = (
        df.filter(F.col("n_positions").isNotNull())
          .groupBy("user_id")
          .agg(F.min("year").alias("first_pos_year"))
    )

    df = (
        df.join(first_valid_year, on="user_id", how="left")
          .withColumn(
              "n_positions",
              F.when(
                  (F.col("year") >= F.col("first_pos_year")) & F.col("n_positions").isNull(),
                  F.lit(1)
              ).otherwise(F.col("n_positions"))
          )
          .drop("first_pos_year")
    )

check_snapshot(df, "After cleaning city/country and n_positions")


# ==============================
# 4. Write merged dataset
# ==============================
print(f"[INFO] Writing merged dataset to {out_path} ...")
os.system(f"rm -rf {out_path}")
df.repartition(200).write.mode("overwrite").parquet(out_path)

print("[INFO] ✅ Final merged dataset with global invariants written successfully.")
spark.stop()
