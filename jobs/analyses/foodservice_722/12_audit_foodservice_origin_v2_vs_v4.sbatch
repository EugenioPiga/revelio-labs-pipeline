#!/bin/bash
#SBATCH --job-name=food722_v2v4_audit
#SBATCH --partition=general
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=6
#SBATCH --mem=96G
#SBATCH --output=/home/epiga/revelio_labs/logs/food722_v2v4_audit_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/food722_v2v4_audit_%j.err

set -eo pipefail

ROOT="/home/epiga/revelio_labs/founder_foodservice_722_v1"
OUTROOT="/home/epiga/revelio_labs/output/us_foodservice_722_histories_v1"

V2="$OUTROOT/us_foodservice_722_people_origin_v2"
V4="$OUTROOT/us_foodservice_722_people_origin_v4"

: "${RUN_ID:?RUN_ID required}"

OUTDIR="$OUTROOT/origin_country_v2_v4_audit/$RUN_ID"

test -f "$V2/_SUCCESS"
test -f "$V4/_SUCCESS"

test ! -e "$OUTDIR" || {
    echo "ERROR: refusing to overwrite $OUTDIR"
    exit 9
}

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/food722_v2v4_audit_${SLURM_JOB_ID}"

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"

mkdir -p "$TMPDIR"

trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 78g \
  --conf spark.sql.shuffle.partitions=1024 \
  --conf spark.default.parallelism=1024 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.local.dir="$SCRATCH" \
  "$ROOT/audit_origin_v2_vs_v4.py" \
    --v2 "$V2" \
    --v4 "$V4" \
    --outdir "$OUTDIR"
