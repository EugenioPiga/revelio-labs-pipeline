#!/bin/bash
#SBATCH --job-name=fs722_bb_v2_pre
#SBATCH --partition=general
#SBATCH --time=08:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=128G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/backbone_v2_preflight_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/backbone_v2_preflight_%j.err

set -eo pipefail

ROOT="/home/epiga/revelio_labs/foodservice_722_backbone_match_v1"
OUTROOT="/home/epiga/revelio_labs/output/foodservice_722_backbone_match_v1"
BACKBONE="/home/epiga/revelio_labs/input/backbones/restaurant_backbone_enriched_20260710_083321.parquet"

: "${RUN_ID:?RUN_ID is required}"
OUTDIR="$OUTROOT/preflight/$RUN_ID"

test -f "$BACKBONE"
test ! -e "$OUTDIR" || {
  echo "ERROR: refusing to overwrite existing output: $OUTDIR"
  exit 9
}

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_bb_v2_pre_${SLURM_JOB_ID}"
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"

mkdir -p "$TMPDIR" "$SCRATCH/warehouse"
trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 100g \
  --conf spark.sql.shuffle.partitions=1024 \
  --conf spark.default.parallelism=1024 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.local.dir="$SCRATCH" \
  "$ROOT/src/05_preflight_updated_backbone_v2.py" \
    --backbone "$BACKBONE" \
    --outdir "$OUTDIR" \
    --run-id "$RUN_ID"
