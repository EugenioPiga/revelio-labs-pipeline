#!/usr/bin/env bash
#SBATCH --job-name=audit_step6_origin
#SBATCH --output=/home/epiga/revelio_labs/logs/audit_step6_origin_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/audit_step6_origin_%j.err
#SBATCH --time=03:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=120G
#SBATCH --partition=general

set -euo pipefail

V3_DIR="/home/epiga/revelio_labs/pipeline_v3_spell_integrity"

cd "$V3_DIR"
source PATHS_TO_EDIT.env

export JOB_LABEL="audit_step6_origin"
export SPARK_DRIVER_MEMORY="100g"
export SPARK_EXECUTOR_MEMORY="100g"

source jobs/v3_runtime_env.sh

export STEP5_CORRECTED="${STEP5_MERGED_OUT%/}_origin_any_education"
export STEP6_CORRECTED="${STEP6_OUT%/}_origin_any_education"

python - <<'PY'
import os

from pyspark.sql import SparkSession, functions as F

step5_path = os.environ["STEP5_CORRECTED"]
step6_path = os.environ["STEP6_CORRECTED"]

spark = (
    SparkSession.builder
    .master("local[8]")
    .appName("audit_step6_origin")
    .config("spark.sql.shuffle.partitions", "400")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.parquet.enableVectorizedReader", "false")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

step5 = spark.read.parquet(step5_path)
step6 = spark.read.parquet(step6_path)

print("=" * 80)
print("PATHS")
print("=" * 80)
print("STEP5:", step5_path)
print("STEP6:", step6_path)

required_origin = [
    "immig_education_any_nonUS",
    "immig_job_first_nonUS",
    "immig_any_edu_or_first_job_nonUS",
    "country_of_origin",
    "country_of_origin_source",
    "country_of_origin_nonUS",
]

missing_origin = [c for c in required_origin if c not in step6.columns]

print()
print("STEP5 COLUMNS:", len(step5.columns))
print("STEP6 COLUMNS:", len(step6.columns))
print("MISSING ORIGIN COLUMNS:", missing_origin)

if missing_origin:
    raise RuntimeError(f"Origin variables lost in Step 6: {missing_origin}")

n5 = step5.count()
n6 = step6.count()

print()
print("STEP5 ROWS:", n5)
print("STEP6 ROWS:", n6)

if n5 != n6:
    raise RuntimeError(
        f"Row-count mismatch: Step5={n5}, Step6={n6}"
    )

dup = (
    step6.groupBy("user_id", "year")
    .count()
    .filter(F.col("count") > 1)
    .limit(1)
    .count()
)

if dup:
    raise RuntimeError("Duplicate user_id-year rows in Step 6")

print("UNIQUE USER-YEAR KEYS: PASSED")

origin_mismatch = (
    step5.select(
        "user_id",
        "year",
        *required_origin,
    )
    .alias("a")
    .join(
        step6.select(
            "user_id",
            "year",
            *required_origin,
        ).alias("b"),
        on=["user_id", "year"],
        how="full",
    )
)

condition = None
for c in required_origin:
    mismatch = ~F.col(f"a.{c}").eqNullSafe(F.col(f"b.{c}"))
    condition = mismatch if condition is None else (condition | mismatch)

bad_origin = origin_mismatch.filter(condition).limit(1).count()

if bad_origin:
    raise RuntimeError("Origin variables changed between Step 5 and Step 6")

print("ORIGIN VARIABLES PRESERVED: PASSED")

new_columns = sorted(set(step6.columns) - set(step5.columns))

print()
print("NEW STEP 6 COLUMNS:", len(new_columns))
for c in new_columns:
    print(c)

print()
print("STEP 6 ORIGIN AUDIT: PASSED")

spark.stop()
PY
