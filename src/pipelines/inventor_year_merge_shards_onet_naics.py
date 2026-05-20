#!/usr/bin/env python3
###############################################################################
# merge_inventor_year_shards_v2.py
#
# Robust merge of inventor-year v2 shards into:
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2
#
# Input shards are expected to be written by step4_inventor_year.py as:
#   /labs/khanna/linkedin_202507/processed/inventor_year_shards_v2/shard=00
#   /labs/khanna/linkedin_202507/processed/inventor_year_shards_v2/shard=01
#   ...
#
# What this does:
#   1. Reads all v2 shard folders.
#   2. Normalizes core column types.
#   3. Unions shards with allowMissingColumns=True.
#   4. Recomputes a few global inventor-level invariant aliases.
#   5. Cleans invalid years and empty location strings.
#   6. Removes duplicate user_id-year rows if any remain.
#   7. Writes the final merged v2 inventor-year panel.
#
# Important:
#   This script only merges columns already present in the v2 shards.
#   If O*NET / NAICS / occupation variables are not written by Step4,
#   this script cannot create them.
###############################################################################

import os
from functools import reduce

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


# =============================================================================
# CONFIG
# =============================================================================

IN_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_shards_v2_onet_naics"
OUT_PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2_onet_naics"

SPARK_TMP = "/labs/khanna/linkedin_202507/spark_tmp"
N_OUT_PARTITIONS = 200

VALID_YEAR_MIN = 1950
VALID_YEAR_MAX = 2025


# =============================================================================
# SPARK SETUP
# =============================================================================

spark = (
    SparkSession.builder
    .appName("merge_inventor_year_v2")
    .config("spark.driver.memory", "32g")
    .config("spark.executor.memory", "32g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "200")
    .config("spark.local.dir", SPARK_TMP)
    .config("spark.sql.autoBroadcastJoinThreshold", "-1")
    .config("spark.sql.files.maxPartitionBytes", "128m")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =============================================================================
# HELPERS
# =============================================================================

def check_snapshot(df, label, n=5, heavy=True):
    """
    Print useful diagnostics.
    Set heavy=False if you want to avoid count() actions.
    """
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")

    if not heavy:
        print("[INFO] Skipping row count and duplicate scan.")
        try:
            df.show(n, truncate=False)
        except Exception as e:
            print(f"[WARN] Sample show failed: {e}")
        return

    row_count = df.count()
    print(f"[INFO] Total rows: {row_count:,}")

    if "year" in df.columns:
        dupes = (
            df.groupBy("user_id", "year")
              .count()
              .filter(F.col("count") > 1)
        )
        dup_ct = dupes.count()
        print(f"[CHECK] {dup_ct:,} duplicate (user_id, year) pairs.")
        if dup_ct > 0:
            print("[SAMPLE] Duplicate pairs:")
            dupes.show(n, truncate=False)
    else:
        dupes = (
            df.groupBy("user_id")
              .count()
              .filter(F.col("count") > 1)
        )
        dup_ct = dupes.count()
        print(f"[CHECK] {dup_ct:,} duplicate user_id rows.")
        if dup_ct > 0:
            print("[SAMPLE] Duplicate user_ids:")
            dupes.show(n, truncate=False)

    print("[SAMPLE]")
    df.show(n, truncate=False)


def add_missing_column(df, colname, dtype):
    """Add a missing column with NULL cast to dtype."""
    if colname not in df.columns:
        df = df.withColumn(colname, F.lit(None).cast(dtype))
    return df


def cast_if_present(df, colname, dtype):
    """Cast column if present."""
    if colname in df.columns:
        df = df.withColumn(colname, F.col(colname).cast(dtype))
    return df


def first_nonnull_alias(df, source_cols, target_col):
    """
    Create target_col from the first non-null source column that exists.
    Does not overwrite target_col if it already exists.
    """
    if target_col in df.columns:
        return df

    existing = [c for c in source_cols if c in df.columns]
    if not existing:
        return df.withColumn(target_col, F.lit(None))

    return df.withColumn(target_col, F.coalesce(*[F.col(c) for c in existing]))


# =============================================================================
# 1. FIND V2 SHARDS
# =============================================================================

print(f"[INFO] Reading v2 inventor-year shards from: {IN_DIR}")

if not os.path.exists(IN_DIR):
    raise FileNotFoundError(f"Input directory does not exist: {IN_DIR}")

shards = sorted(
    os.path.join(IN_DIR, d)
    for d in os.listdir(IN_DIR)
    if d.startswith("shard=")
)

if not shards:
    raise ValueError(
        f"No v2 shard folders found in {IN_DIR}. "
        "Expected folders named shard=00, shard=01, ..."
    )

print(f"[INFO] Found {len(shards)} shard folders.")
for s in shards[:10]:
    print(f"  - {s}")
if len(shards) > 10:
    print(f"  ... plus {len(shards) - 10} more")


# =============================================================================
# 2. READ AND NORMALIZE EACH SHARD
# =============================================================================

df_list = []

required_long_cols = [
    "first_rcid",
    "last_rcid",
    "first_parent_rcid",
    "last_parent_rcid",
    "first_position_id",
]

required_string_cols = [
    "first_university_country",
    "last_university_country",
    "first_country",
    "last_country",
    "first_city",
    "last_city",
    "first_state",
    "last_state",
    "first_region",
    "last_region",
    "first_metro_area",
    "last_metro_area",
]

required_numeric_cols = [
    "n_patents",
    "n_applications",
    "n_first_inventor",
    "n_us_citations",
    "n_positions",
    "n_unique_companies",
    "avg_salary",
    "avg_total_comp",
    "avg_seniority",
    "pos_extrapolated",
    "n_educations",
]

for s in shards:
    print(f"[DEBUG] Reading shard: {s}")
    tmp = (
        spark.read
        .option("datetimeRebaseMode", "LEGACY")
        .option("int96RebaseMode", "LEGACY")
        .parquet(s)
    )

    # Core keys
    tmp = cast_if_present(tmp, "user_id", "long")
    tmp = cast_if_present(tmp, "year", "int")

    # Make sure key columns exist
    tmp = add_missing_column(tmp, "user_id", "long")
    tmp = add_missing_column(tmp, "year", "int")

    # Normalize common ID/string/numeric columns if present, add if missing
    for c in required_long_cols:
        tmp = add_missing_column(tmp, c, "long")
        tmp = cast_if_present(tmp, c, "long")

    for c in required_string_cols:
        tmp = add_missing_column(tmp, c, "string")
        tmp = cast_if_present(tmp, c, "string")

    for c in required_numeric_cols:
        tmp = add_missing_column(tmp, c, "double")
        tmp = cast_if_present(tmp, c, "double")

    df_list.append(tmp)


# =============================================================================
# 3. UNION SHARDS
# =============================================================================

print(f"[INFO] Unioning {len(df_list)} shards with allowMissingColumns=True ...")

df = reduce(
    lambda a, b: a.unionByName(b, allowMissingColumns=True),
    df_list
)

check_snapshot(df, "After UNION of all v2 shards", heavy=True)


# =============================================================================
# 4. CLEAN YEARS
# =============================================================================

print(f"[INFO] Keeping valid years in [{VALID_YEAR_MIN}, {VALID_YEAR_MAX}] ...")

df = df.withColumn(
    "year",
    F.when(
        (F.col("year") < VALID_YEAR_MIN) | (F.col("year") > VALID_YEAR_MAX),
        F.lit(None).cast("int")
    ).otherwise(F.col("year"))
)

df = df.filter(F.col("user_id").isNotNull() & F.col("year").isNotNull())

check_snapshot(df, "After dropping invalid user_id/year rows", heavy=True)


# =============================================================================
# 5. RECREATE / STANDARDIZE GLOBAL INVARIANT ALIASES
# =============================================================================

print("[INFO] Standardizing global inventor-level invariant aliases ...")

# v2 contains edu_first_* / edu_last_* plus old-style aliases in many cases.
# This block is conservative: it preserves original v2 columns and only
# creates/fixes old-style aliases used by older analysis code.

# Date conversions if present
date_cols = [
    "first_startdate_pos",
    "first_startdate_edu",
    "first_enddate_edu",
    "last_startdate_edu",
    "last_enddate_edu",
    "edu_first_startdate",
    "edu_first_enddate",
    "edu_last_startdate",
    "edu_last_enddate",
]

for c in date_cols:
    if c in df.columns:
        df = df.withColumn(c, F.to_date(F.col(c)))

# Create aliases if missing
df = first_nonnull_alias(
    df,
    ["edu_first_university_name", "first_university"],
    "first_university",
)
df = first_nonnull_alias(
    df,
    ["edu_last_university_name", "last_university"],
    "last_university",
)
df = first_nonnull_alias(
    df,
    ["edu_first_degree", "first_degree"],
    "first_degree",
)
df = first_nonnull_alias(
    df,
    ["edu_last_degree", "last_degree"],
    "last_degree",
)
df = first_nonnull_alias(
    df,
    ["edu_first_field", "first_field"],
    "first_field",
)
df = first_nonnull_alias(
    df,
    ["edu_last_field", "last_field"],
    "last_field",
)
df = first_nonnull_alias(
    df,
    ["edu_first_university_country", "first_university_country"],
    "first_university_country",
)
df = first_nonnull_alias(
    df,
    ["edu_last_university_country", "last_university_country"],
    "last_university_country",
)
df = first_nonnull_alias(
    df,
    ["edu_first_startdate", "first_startdate_edu"],
    "first_startdate_edu",
)
df = first_nonnull_alias(
    df,
    ["edu_first_enddate", "first_enddate_edu"],
    "first_enddate_edu",
)
df = first_nonnull_alias(
    df,
    ["edu_last_startdate", "last_startdate_edu"],
    "last_startdate_edu",
)
df = first_nonnull_alias(
    df,
    ["edu_last_enddate", "last_enddate_edu"],
    "last_enddate_edu",
)

# Recompute global invariant aliases across all rows for each inventor.
# This matters if an alias is missing in some years but present in others.
agg_exprs = []

if "first_startdate_pos" in df.columns:
    agg_exprs.append(F.min("first_startdate_pos").alias("global_first_startdate_pos"))

if "first_startdate_edu" in df.columns:
    agg_exprs.append(F.min("first_startdate_edu").alias("global_first_startdate_edu"))

if "first_enddate_edu" in df.columns:
    agg_exprs.append(F.first("first_enddate_edu", ignorenulls=True).alias("global_first_enddate_edu"))

if "last_startdate_edu" in df.columns:
    agg_exprs.append(F.first("last_startdate_edu", ignorenulls=True).alias("global_last_startdate_edu"))

if "last_enddate_edu" in df.columns:
    agg_exprs.append(F.max("last_enddate_edu").alias("global_last_enddate_edu"))

for c in [
    "first_university",
    "last_university",
    "first_degree",
    "last_degree",
    "first_field",
    "last_field",
    "first_university_country",
    "last_university_country",
]:
    if c in df.columns:
        agg_exprs.append(F.first(c, ignorenulls=True).alias(f"global_{c}"))

if agg_exprs:
    global_inv = df.groupBy("user_id").agg(*agg_exprs)

    # Drop old alias columns before replacing them with global versions
    replace_cols = [
        c.replace("global_", "")
        for c in global_inv.columns
        if c.startswith("global_")
    ]
    df = df.drop(*[c for c in replace_cols if c in df.columns])

    df = df.join(global_inv, on="user_id", how="left")

    for c in global_inv.columns:
        if c.startswith("global_"):
            df = df.withColumnRenamed(c, c.replace("global_", ""))

check_snapshot(df, "After standardizing global invariant aliases", heavy=True)


# =============================================================================
# 6. REMOVE DUPLICATE USER-YEAR ROWS
# =============================================================================

print("[INFO] Removing duplicate (user_id, year) rows if any ...")

# Duplicates should not exist because step4 used hash sharding.
# If they do, keep one row per user-year.
df = df.dropDuplicates(["user_id", "year"])

check_snapshot(df, "After duplicate removal", heavy=True)


# =============================================================================
# 7. BASIC CLEANING
# =============================================================================

print("[INFO] Cleaning empty strings in location fields ...")

location_cols = [
    "first_city",
    "last_city",
    "first_country",
    "last_country",
    "first_state",
    "last_state",
    "first_region",
    "last_region",
    "first_metro_area",
    "last_metro_area",
    "first_location_raw",
    "last_location_raw",
]

for c in location_cols:
    if c in df.columns:
        df = df.withColumn(
            c,
            F.when(
                (F.col(c).isNull()) |
                (F.trim(F.col(c)) == "") |
                (F.lower(F.trim(F.col(c))) == "empty"),
                F.lit(None).cast("string")
            ).otherwise(F.col(c))
        )

# Fill key counts after outer joins / extrapolation
fill_zero_cols = [
    "n_patents",
    "n_applications",
    "n_first_inventor",
    "n_us_citations",
    "pos_extrapolated",
    "n_educations",
]

fill_zero_map = {
    c: 0
    for c in fill_zero_cols
    if c in df.columns
}

if fill_zero_map:
    df = df.fillna(fill_zero_map)

# has_education guard
if "has_education" in df.columns:
    df = df.fillna({"has_education": False})
else:
    df = df.withColumn("has_education", F.col("n_educations") > 0)

# Forward-fill n_positions = 1 after first observed position if missing
if "n_positions" in df.columns:
    first_valid_pos_year = (
        df.filter(F.col("n_positions").isNotNull())
          .groupBy("user_id")
          .agg(F.min("year").alias("first_pos_year"))
    )

    df = (
        df.join(first_valid_pos_year, on="user_id", how="left")
          .withColumn(
              "n_positions",
              F.when(
                  (F.col("year") >= F.col("first_pos_year")) &
                  F.col("n_positions").isNull(),
                  F.lit(1.0)
              ).otherwise(F.col("n_positions"))
          )
          .drop("first_pos_year")
    )

check_snapshot(df, "After basic cleaning", heavy=True)


# =============================================================================
# 8. FINAL SCHEMA CHECKS
# =============================================================================

print("[INFO] Final schema check ...")

# Ensure commonly used columns exist
final_required_cols = {
    "user_id": "long",
    "year": "int",
    "first_parent_rcid": "long",
    "last_parent_rcid": "long",
    "first_position_id": "long",
    "first_university_country": "string",
    "last_university_country": "string",
    "n_patents": "double",
    "n_applications": "double",
    "n_us_citations": "double",
    "pos_extrapolated": "double",
}

for c, dtype in final_required_cols.items():
    df = add_missing_column(df, c, dtype)
    df = cast_if_present(df, c, dtype)

print("[INFO] Final number of columns:", len(df.columns))
print("[INFO] Final columns:")
for c in df.columns:
    print(f"  - {c}")


# =============================================================================
# 9. WRITE FINAL MERGED DATASET
# =============================================================================

print(f"[INFO] Writing merged v2 dataset to: {OUT_PATH}")

os.system(f"rm -rf {OUT_PATH}")

(
    df.repartition(N_OUT_PARTITIONS)
      .write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(OUT_PATH)
)

print("[INFO] ✅ Finished writing inventor_year_merged_v2 successfully.")
spark.stop()
