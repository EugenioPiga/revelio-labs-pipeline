#!/bin/bash
#SBATCH --job-name=fs722_keys_geo
#SBATCH --partition=general
#SBATCH --time=1-00:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=256G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/keys_geo_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/keys_geo_%j.err

# Conda activation is not compatible with Bash nounset mode.
set -eo pipefail

ROOT="/home/epiga/revelio_labs/foodservice_722_backbone_match_v1"
BACKBONE="/home/epiga/revelio_labs/input/restaurant_backbone_enriched.parquet"
SPELLS="/home/epiga/revelio_labs/output/us_foodservice_722_histories_v1/us_foodservice_722_spells_v3"

: "${RUN_ID:?RUN_ID is required}"
: "${KEY_OUT:?KEY_OUT is required}"
: "${REPORT_OUT:?REPORT_OUT is required}"

test -f "$BACKBONE"
test -f "$SPELLS/_SUCCESS"

test ! -e "$KEY_OUT" || {
  echo "ERROR: refusing to overwrite existing key output: $KEY_OUT"
  exit 9
}

test ! -e "$REPORT_OUT" || {
  echo "ERROR: refusing to overwrite existing report output: $REPORT_OUT"
  exit 10
}

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_keys_geo_${SLURM_JOB_ID}"
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"

mkdir -p "$TMPDIR" "$SCRATCH/warehouse"
trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 220g \
  --conf spark.sql.shuffle.partitions=3072 \
  --conf spark.default.parallelism=3072 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.parquet.datetimeRebaseModeInRead=LEGACY \
  --conf spark.sql.parquet.int96RebaseModeInRead=LEGACY \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.local.dir="$SCRATCH" \
  "$ROOT/src/02_prepare_keys_and_geo_audit.py" \
    --backbone "$BACKBONE" \
    --spells "$SPELLS" \
    --key-out "$KEY_OUT" \
    --report-out "$REPORT_OUT" \
    --run-id "$RUN_ID"
