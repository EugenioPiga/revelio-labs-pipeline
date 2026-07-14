#!/bin/bash
#SBATCH --job-name=fs722_fuzzy
#SBATCH --partition=general
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=180G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_fuzzy_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_fuzzy_%j.err

set -eo pipefail

BASE="/home/epiga/revelio_labs"
PROJ="$BASE/foodservice_722_backbone_match_v1"

: "${RUN_ID:?RUN_ID required}"
: "${HIER_OUTDIR:?HIER_OUTDIR required}"

ACCEPTED="$HIER_OUTDIR/accepted_crosswalk/accepted_crosswalk"
AMBIGUOUS="$HIER_OUTDIR/ambiguous_crosswalk/ambiguous_crosswalk"
UNMATCHED="$HIER_OUTDIR/unmatched_revelio_keys/unmatched_revelio_keys"

BACKBONE="$BASE/input/backbones/restaurant_backbone_enriched_20260710_083321.parquet"

OUTDIR="$BASE/output/foodservice_722_backbone_match_v1/restricted_fuzzy/$RUN_ID"

test -d "$ACCEPTED"
test -d "$AMBIGUOUS"
test -d "$UNMATCHED"
test -e "$BACKBONE"
test ! -e "$OUTDIR"

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"

export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_fuzzy_${SLURM_JOB_ID}"

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"

mkdir -p "$TMPDIR"

trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 150g \
  --conf spark.sql.shuffle.partitions=2048 \
  --conf spark.default.parallelism=2048 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.local.dir="$SCRATCH" \
  "$PROJ/src/12_restricted_fuzzy_match.py" \
    --accepted "$ACCEPTED" \
    --ambiguous "$AMBIGUOUS" \
    --unmatched "$UNMATCHED" \
    --backbone "$BACKBONE" \
    --outdir "$OUTDIR" \
    --max-distance-ratio 0.10 \
    --minimum-margin 0.03 \
    --minimum-name-length 6
