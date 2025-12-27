#!/usr/bin/env python3
###############################################################################
# merge_inventor_year_shards.py
# Robust merge of inventor_year shards with consistent schema
# Author: Eugenio
###############################################################################

from pyspark.sql import SparkSession, functions as F
import os
from functools import reduce
from pyspark.sql import Window

# ==============================
# Spark setup
# ==============================
spark = (
    SparkSession.builder
    .appName("merge_inventor_year_global_fix")

    # Lower driver & executor memory → avoids GC lockups
    .config("spark.driver.memory", "32g")
    .config("spark.executor.memory", "32g")
    .config("spark.executor.cores", "4")

    # Avoid huge shuffle partitions → 100–200 is optimal
    .config("spark.sql.shuffle.partitions", "200")

    # Store temp data on fast SSD (VERY important)
    .config("spark.local.dir", "/labs/khanna/linkedin_202507/spark_tmp")

    # Avoid broadcast failures
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")

    # Reduce shuffle file sizes so GC is lighter
    .config("spark.sql.files.maxPartitionBytes", "128m")

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

    # Standardize core types
    tmp = (
        tmp.withColumn("user_id", F.col("user_id").cast("long"))
           .withColumn("year", F.col("year").cast("int"))
    )

    # Columns that must be LONG (IDs)
    required_cols_long = [
        "first_parent_rcid",
        "last_parent_rcid",
        "first_position_id"
    ]

    # Columns that must be STRING
    required_cols_string = [
        "first_university_country",
        "last_university_country"
    ]

    # Add missing long columns
    for c in required_cols_long:
        if c not in tmp.columns:
            tmp = tmp.withColumn(c, F.lit(None).cast("long"))

    # Add missing string columns
    for c in required_cols_string:
        if c not in tmp.columns:
            tmp = tmp.withColumn(c, F.lit(None).cast("string"))

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

# These are also treated as "invariant" and will be recomputed from agg_dates
uni_country_cols = ["first_university_country", "last_university_country"]

for c in date_cols:
    if c in df.columns:
        df = df.withColumn(c, F.to_date(F.col(c)))

agg_dates = (
    df.groupBy("user_id")
      .agg(
          F.min("first_startdate_pos").alias("global_first_startdate_pos"),
          F.min("first_startdate_edu").alias("global_first_startdate_edu"),
          F.max("first_enddate_edu").alias("global_last_enddate_edu"),
          F.first("first_university_country", ignorenulls=True)
              .alias("global_first_university_country"),
          F.first("last_university_country", ignorenulls=True)
              .alias("global_last_university_country")
      )
)

check_snapshot(agg_dates, "Aggregated invariant dates and locations")

print("[INFO] Joining global invariant fields back to main DataFrame...")

# 🔹 Drop old date + uni_country columns before join to avoid duplicates
drop_cols = date_cols + uni_country_cols
df = df.drop(*[c for c in drop_cols if c in df.columns])

df = (
    df.join(agg_dates, on="user_id", how="left")
      .withColumnRenamed("global_first_startdate_pos", "first_startdate_pos")
      .withColumnRenamed("global_first_startdate_edu", "first_startdate_edu")
      .withColumnRenamed("global_last_enddate_edu", "first_enddate_edu")
      .withColumnRenamed("global_first_university_country", "first_university_country")
      .withColumnRenamed("global_last_university_country", "last_university_country")
)

check_snapshot(df, "Before dropping duplicates")

# ==============================
# Fix invalid years BEFORE de-duplication
# ==============================
print("[INFO] Fixing invalid year values (capping to [1950, 2025]) ...")

df = df.withColumn(
    "year",
    F.when((F.col("year") < 1950) | (F.col("year") > 2025), None)
     .otherwise(F.col("year"))
)

# Drop rows with invalid years
df = df.filter(F.col("year").isNotNull())

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
# 4b. Ensure new fields exist (parent IDs, uni_country, position_id)
# ==============================
print("[INFO] Ensuring parent_rcid, university_country, and first_position_id columns exist ...")

# IDs should be long
for c in ["first_parent_rcid", "last_parent_rcid", "first_position_id"]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("long"))

# Uni-country should be strings (already present after join, but keep this as a guard)
for c in ["first_university_country", "last_university_country"]:
    if c not in df.columns:
        df = df.withColumn(c, F.lit(None).cast("string"))

print("[DEBUG] Final columns in df:", df.columns)

# ==============================
# 5. Write merged dataset
# ==============================
print(f"[INFO] Writing merged dataset to {out_path} ...")
os.system(f"rm -rf {out_path}")
df.repartition(200).write.mode("overwrite").parquet(out_path)

print("[INFO] ✅ Final merged dataset with global invariants written successfully.")
spark.stop()
