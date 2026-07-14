#!/bin/bash
#SBATCH --job-name=food722_origin_v4
#SBATCH --partition=general
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=192G
#SBATCH --output=/home/epiga/revelio_labs/logs/food722_origin_v4_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/food722_origin_v4_%j.err

set -eo pipefail

ROOT="/home/epiga/revelio_labs/founder_foodservice_722_v1"
OUTROOT="/home/epiga/revelio_labs/output/us_foodservice_722_histories_v1"

ORIGIN_V1="$OUTROOT/us_foodservice_722_people_origin_v1"
EDUCATION="/labs/khanna/linkedin_202507/academic_individual_user_education"

ORIGIN_V4="$OUTROOT/us_foodservice_722_people_origin_v4"
DIAGNOSTICS_V4="$OUTROOT/origin_country_diagnostics_v4"

test -f "$ORIGIN_V1/_SUCCESS"
test -d "$EDUCATION"

test ! -e "$ORIGIN_V4" || {
    echo "ERROR: refusing to overwrite $ORIGIN_V4"
    exit 9
}

test ! -e "$DIAGNOSTICS_V4" || {
    echo "ERROR: refusing to overwrite $DIAGNOSTICS_V4"
    exit 9
}

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/food722_origin_v4_${SLURM_JOB_ID}"

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"

mkdir -p "$TMPDIR" "$SCRATCH/warehouse"

trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 160g \
  --conf spark.sql.shuffle.partitions=2048 \
  --conf spark.default.parallelism=2048 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.sql.parquet.datetimeRebaseModeInRead=CORRECTED \
  --conf spark.sql.parquet.datetimeRebaseModeInWrite=CORRECTED \
  --conf spark.sql.legacy.parquet.datetimeRebaseModeInRead=CORRECTED \
  --conf spark.sql.legacy.parquet.datetimeRebaseModeInWrite=CORRECTED \
  --conf spark.local.dir="$SCRATCH" \
  "$ROOT/foodservice_722_origin_country_v4.py" \
    --origin-v1 "$ORIGIN_V1" \
    --education-path "$EDUCATION" \
    --output-dir "$ORIGIN_V4" \
    --diagnostics-dir "$DIAGNOSTICS_V4"
