#!/bin/bash
#SBATCH --job-name=fs722_bb2_hier
#SBATCH --partition=general
#SBATCH --time=1-00:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=256G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/backbone_v2_hierarchical_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/backbone_v2_hierarchical_%j.err

set -eo pipefail

ROOT="/home/epiga/revelio_labs/foodservice_722_backbone_match_v1"
OUTROOT="/home/epiga/revelio_labs/output/foodservice_722_backbone_match_v1"

BACKBONE="/home/epiga/revelio_labs/input/backbones/restaurant_backbone_enriched_20260710_083321.parquet"
REVELIO_KEYS="/home/epiga/revelio_labs/output/foodservice_722_backbone_match_v1/candidate_pairs/keys_geo_20260709_001620/revelio_entity_location_keys"
GEOGRAPHY_KEYS="/home/epiga/revelio_labs/output/foodservice_722_backbone_match_v1/updated_backbone_v2_audit/backbone_v2_focused_audit_20260709_235006/backbone_geography_keys"

: "${RUN_ID:?RUN_ID is required}"

OUTDIR="$OUTROOT/updated_backbone_v2_hierarchical_exact/$RUN_ID"

test -f "$BACKBONE"
test -f "$REVELIO_KEYS/_SUCCESS"
test -f "$GEOGRAPHY_KEYS/_SUCCESS"

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

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_bb2_hier_${SLURM_JOB_ID}"

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
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.local.dir="$SCRATCH" \
  "$ROOT/src/10_updated_backbone_hierarchical_exact.py" \
    --backbone "$BACKBONE" \
    --revelio-keys "$REVELIO_KEYS" \
    --geography-keys "$GEOGRAPHY_KEYS" \
    --outdir "$OUTDIR" \
    --run-id "$RUN_ID"
