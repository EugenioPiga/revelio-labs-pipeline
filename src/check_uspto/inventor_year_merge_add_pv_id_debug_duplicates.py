#!/usr/bin/env python
# %%
import os
from pathlib import Path

try:
    from pyspark.sql import SparkSession, functions as F
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "PySpark is required to run this inspection script.\n"
        "Activate the project virtualenv (e.g. `source venv/bin/activate`) "
        "or install pyspark in your current environment."
    ) from exc

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# %%
# Configure input locations (edit as needed)
duplicates_path = Path("/home/gps-yuhei/revelio_labs/output/userid_pvid_correspondence_duplicates")

# %%
# Start Spark session
try:
    spark = (
        SparkSession.builder
        .master("local[*]")
        .appName("inspect_pvid_duplicates")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.warehouse.dir", str(Path("./spark-warehouse").resolve()))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
except Exception as exc:  # pragma: no cover
    raise SystemExit(
        "Failed to start a local Spark session. This environment may prohibit socket binding.\n"
        "Please rerun this script on a machine where Spark can start (e.g., the cluster login node).\n"
        f"Original error: {exc}"
    ) from exc

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
# Target user IDs to inspect
target_user_ids = [100815327, 116965573, 129204179, 130104932]

target_rows = duplicates_df.filter(F.col("user_id").isin(target_user_ids))

print("\n[INFO] Duplicate rows for selected user_id values")
target_rows.orderBy("user_id", "PV_INVENTOR_ID").show(truncate=False)

# %%
# Load inventor_year_merged and inspect the same user IDs
inventor_year_path = Path("/labs/khanna/linkedin_202507/processed/inventor_year_merged")
if inventor_year_path.exists():
    inventor_year_df = spark.read.parquet(str(inventor_year_path))
    inventor_year_df.cache()

    print(f"[INFO] Loaded inventor_year_merged with {inventor_year_df.count():,} rows")

    inventor_year_subset = (
        inventor_year_df
        .filter(F.col("user_id").isin(target_user_ids))
        .orderBy("user_id", "year")
    )

    print("\n[INFO] inventor_year_merged rows for selected user_id values")
    inventor_year_subset.show(50, truncate=False)
else:
    print(f"[WARN] inventor_year_merged path not found or inaccessible: {inventor_year_path}")

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
