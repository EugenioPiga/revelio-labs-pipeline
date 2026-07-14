#!/bin/bash
#SBATCH --job-name=fs722_complete_pre
#SBATCH --partition=general
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=6
#SBATCH --mem=96G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_complete_pre_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_complete_pre_%j.err

set -eo pipefail

BASE="/home/epiga/revelio_labs"
PROJ="$BASE/foodservice_722_backbone_match_v1"

: "${RUN_ID:?RUN_ID required}"
: "${HIER_OUTDIR:?HIER_OUTDIR required}"

OLD_CROSSWALK="$BASE/output/foodservice_722_backbone_match_v1/accepted_crosswalk/exact_tier12_20260709_030722/accepted_crosswalk"

BACKBONE="$BASE/input/backbones/restaurant_backbone_enriched_20260710_083321.parquet"

SPELLS="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_spells_v3"

ORIGIN_V4="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_people_origin_v4"

HIER_ACCEPTED="$HIER_OUTDIR/accepted_crosswalk/accepted_crosswalk"
HIER_AMBIGUOUS="$HIER_OUTDIR/ambiguous_crosswalk/ambiguous_crosswalk"
HIER_UNMATCHED="$HIER_OUTDIR/unmatched_revelio_keys/unmatched_revelio_keys"

OUTDIR="$BASE/output/foodservice_722_backbone_match_v1/completion_preflight/$RUN_ID"

test ! -e "$OUTDIR"
test -d "$OLD_CROSSWALK"
test -d "$HIER_ACCEPTED"
test -d "$HIER_AMBIGUOUS"
test -d "$HIER_UNMATCHED"
test -f "$SPELLS/_SUCCESS"
test -f "$ORIGIN_V4/_SUCCESS"

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"

export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_complete_pre_${SLURM_JOB_ID}"

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
  "$PROJ/src/11_completion_preflight_and_reconcile.py" \
    --old-crosswalk "$OLD_CROSSWALK" \
    --hier-accepted "$HIER_ACCEPTED" \
    --hier-ambiguous "$HIER_AMBIGUOUS" \
    --hier-unmatched "$HIER_UNMATCHED" \
    --backbone "$BACKBONE" \
    --spells "$SPELLS" \
    --origin-v4 "$ORIGIN_V4" \
    --outdir "$OUTDIR"
