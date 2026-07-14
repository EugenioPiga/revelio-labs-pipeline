#!/bin/bash
#SBATCH --job-name=fs722_finalize
#SBATCH --partition=general
#SBATCH --time=16:00:00
#SBATCH --cpus-per-task=12
#SBATCH --mem=180G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_finalize_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_backbone_match_v1/logs/fs722_finalize_%j.err

set -eo pipefail

BASE="/home/epiga/revelio_labs"
PROJ="$BASE/foodservice_722_backbone_match_v1"

: "${RUN_ID:?RUN_ID required}"
: "${EXACT_CROSSWALK:?EXACT_CROSSWALK required}"
: "${FUZZY_CROSSWALK:?FUZZY_CROSSWALK required}"

BACKBONE="$BASE/input/backbones/restaurant_backbone_enriched_20260710_083321.parquet"

SPELLS="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_spells_v3"

ORIGIN_V4="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_people_origin_v4"

OUTDIR="$BASE/output/foodservice_722_backbone_match_v1/final_linkage/$RUN_ID"

test -d "$EXACT_CROSSWALK"
test -d "$FUZZY_CROSSWALK"
test -e "$BACKBONE"
test -f "$SPELLS/_SUCCESS"
test -f "$ORIGIN_V4/_SUCCESS"
test ! -e "$OUTDIR"

source "$HOME/miniconda/bin/activate" revelio

PYTHON="$HOME/miniconda/envs/revelio/bin/python"

export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_finalize_${SLURM_JOB_ID}"

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
  "$PROJ/src/13_finalize_foodservice_linkage.py" \
    --exact-crosswalk "$EXACT_CROSSWALK" \
    --fuzzy-crosswalk "$FUZZY_CROSSWALK" \
    --backbone "$BACKBONE" \
    --spells "$SPELLS" \
    --origin-v4 "$ORIGIN_V4" \
    --outdir "$OUTDIR" \
    --chain-threshold 100
