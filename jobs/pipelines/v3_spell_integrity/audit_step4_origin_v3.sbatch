#!/usr/bin/env bash
#SBATCH --job-name=audit_step4_origin
#SBATCH --output=/home/epiga/revelio_labs/logs/audit_step4_origin_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/audit_step4_origin_%j.err
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=100G
#SBATCH --partition=general

set -euo pipefail

V3_DIR="/home/epiga/revelio_labs/pipeline_v3_spell_integrity"

cd "$V3_DIR"
source PATHS_TO_EDIT.env

export JOB_LABEL="audit_step4_origin"
export SPARK_DRIVER_MEMORY="80g"
export SPARK_EXECUTOR_MEMORY="80g"

source jobs/v3_runtime_env.sh

export NEW_STEP4_OUT="${STEP4_SHARDS_OUT%/}_origin_any_education"

echo "Auditing: $NEW_STEP4_OUT"

python - <<'PY'
import os

from pyspark.sql import SparkSession, functions as F

path = os.environ["NEW_STEP4_OUT"]

spark = (
    SparkSession.builder
    .master("local[8]")
    .appName("audit_corrected_step4_origin")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

df = spark.read.parquet(path)

required = [
    "user_id",
    "year",
    "immig_education_any_nonUS",
    "immig_job_first_nonUS",
    "immig_any_edu_or_first_job_nonUS",
    "immig_first_deg_or_job_nonUS",
    "first_observed_education_country",
    "first_nonUS_education_country",
    "first_observed_job_country",
    "country_of_origin",
    "country_of_origin_key",
    "country_of_origin_source",
    "country_of_origin_nonUS",
    "immigration_origin_observed",
]

missing = [c for c in required if c not in df.columns]

print("=" * 80)
print("SCHEMA CHECK")
print("=" * 80)
print("OUTPUT:", path)
print("NUMBER OF COLUMNS:", len(df.columns))
print("MISSING REQUIRED COLUMNS:", missing)

if missing:
    raise RuntimeError(f"Missing corrected columns: {missing}")

duplicate_keys = (
    df.groupBy("user_id", "year")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if duplicate_keys:
    raise RuntimeError("Duplicate user_id-year keys detected")

print("UNIQUE USER-YEAR KEYS: PASSED")

print()
print("=" * 80)
print("PERSON-LEVEL IMMIGRATION SUMMARY")
print("=" * 80)

persons = (
    df.select(
        "user_id",
        "immig_education_any_nonUS",
        "immig_job_first_nonUS",
        "immig_any_edu_or_first_job_nonUS",
        "country_of_origin",
        "country_of_origin_nonUS",
        "country_of_origin_source",
    )
    .dropDuplicates(["user_id"])
)

persons.agg(
    F.count("*").alias("n_users"),
    F.sum(
        F.col("immig_education_any_nonUS").cast("long")
    ).alias("users_any_education_abroad"),
    F.sum(
        F.col("immig_job_first_nonUS").cast("long")
    ).alias("users_first_job_abroad"),
    F.sum(
        F.col("immig_any_edu_or_first_job_nonUS").cast("long")
    ).alias("users_canonical_immigrant"),
    F.sum(
        F.col("country_of_origin").isNotNull().cast("long")
    ).alias("users_with_origin"),
).show(truncate=False)

print()
print("=" * 80)
print("COUNTRY-OF-ORIGIN SOURCES")
print("=" * 80)

(
    persons.groupBy("country_of_origin_source")
    .count()
    .orderBy(F.col("count").desc())
    .show(50, truncate=False)
)

print()
print("=" * 80)
print("MOST COMMON NON-US ORIGINS")
print("=" * 80)

(
    persons
    .filter(F.col("country_of_origin_nonUS") == 1)
    .groupBy("country_of_origin")
    .count()
    .orderBy(
        F.col("count").desc(),
        F.col("country_of_origin").asc(),
    )
    .show(30, truncate=False)
)

print()
print("CORRECTED STEP 4 AUDIT: PASSED")

spark.stop()
PY
