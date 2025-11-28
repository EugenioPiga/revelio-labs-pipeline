#!/usr/bin/env python
# %%
import os
from pathlib import Path

try:
    from pyspark.sql import SparkSession, functions as F
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "PySpark is required for this script. Activate the project virtualenv "
        "(e.g. `source venv/bin/activate`) or install pyspark."
    ) from exc

os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")

# %%
# Config
inventor_year_path = Path("/labs/khanna/linkedin_202507/processed/inventor_year_merged")
target_user_ids = [100815327, 116965573, 129204179, 130104932]
select_columns = [
    "user_id",
    "year",
    "n_patents",
    "n_applications",
    "n_first_inventor",
    "n_positions",
    "n_unique_companies",
    "first_university",
    "first_degree",
    "last_university",
    "last_degree",
    "first_city",
    "last_city",
]

if not inventor_year_path.exists():
    raise SystemExit(f"Parquet path not found: {inventor_year_path}")

# %%
# Start Spark session
spark = (
    SparkSession.builder
    .master("local[*]")
    .appName("inspect_inventor_year_subset")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.shuffle.partitions", "16")
    .config("spark.sql.warehouse.dir", str(Path("./spark-warehouse").resolve()))
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# %%
# Load parquet and filter
df = spark.read.parquet(str(inventor_year_path))

available_cols = set(df.columns)
cols_to_use = [c for c in select_columns if c in available_cols]
if cols_to_use:
    df = df.select(*cols_to_use)

subset = (
    df.filter(F.col("user_id").isin(target_user_ids))
      .orderBy("user_id", "year")
)

# %%
# Display results
total_rows = subset.count()
print(f"[INFO] inventor_year_merged rows for targets: {total_rows:,}")

if total_rows == 0:
    print("[WARN] No rows matched the requested user IDs.")
else:
    subset.show(total_rows if total_rows <= 100 else 100, truncate=False)

    print("\n[INFO] Year ranges per user")
    subset.groupby("user_id").agg(
        F.count("year").alias("rows"),
        F.min("year").alias("min_year"),
        F.max("year").alias("max_year"),
    ).orderBy("user_id").show(truncate=False)

# %%
print("\n[READY] Spark session `spark` and DataFrame `subset` are available for interactive exploration.")
