#!/bin/bash
#SBATCH --job-name=fs722_controls_v2
#SBATCH --partition=general
#SBATCH --time=1-00:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=512G
#SBATCH --output=/home/epiga/revelio_labs/foodservice_722_controls_v2/logs/fs722_controls_v2_%j.out
#SBATCH --error=/home/epiga/revelio_labs/foodservice_722_controls_v2/logs/fs722_controls_v2_%j.err

set -euo pipefail

BASE="/home/epiga/revelio_labs"
PROJECT="$BASE/foodservice_722_controls_v2"
RUN_ID="owner_backbone_refresh_20260715_021137"

FINAL_ROOT="$BASE/output/foodservice_722_owner_backbone_refresh_v1/final_linkage/$RUN_ID"
ORIGIN_V4="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_people_origin_v4"
FULL_HISTORIES="$BASE/output/us_foodservice_722_histories_v1/us_foodservice_722_full_histories_v3"
EDUCATION="/labs/khanna/linkedin_202507/academic_individual_user_education"
USERS="/labs/khanna/linkedin_202507/academic_individual_user"
OUTPUT_ROOT="$BASE/output/foodservice_722_controls_v2/${RUN_ID}_controls_v2"

PYTHON="$HOME/miniconda/envs/revelio/bin/python"
SCRATCH="/labs/khanna/linkedin_202507/scratch/fs722_controls_v2_${SLURM_JOB_ID}"

source "$HOME/miniconda/bin/activate" revelio
export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"

mkdir -p "$TMPDIR" "$SCRATCH/warehouse" "$PROJECT/logs"
trap 'rm -rf "$SCRATCH" 2>/dev/null || true' EXIT

for path in \
  "$FINAL_ROOT/exact_baseline/people/_SUCCESS" \
  "$FINAL_ROOT/exact_baseline/spells/_SUCCESS" \
  "$FINAL_ROOT/exact_plus_fuzzy/people/_SUCCESS" \
  "$FINAL_ROOT/exact_plus_fuzzy/spells/_SUCCESS" \
  "$ORIGIN_V4/_SUCCESS" \
  "$FULL_HISTORIES/_SUCCESS"
do
  test -f "$path"
done

test -d "$EDUCATION"
test -d "$USERS"

if [[ -e "$OUTPUT_ROOT" ]]; then
  echo "ERROR: output already exists: $OUTPUT_ROOT" >&2
  exit 20
fi

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 450g \
  --conf spark.sql.shuffle.partitions=2048 \
  --conf spark.default.parallelism=2048 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.parquet.enableVectorizedReader=false \
  --conf spark.sql.parquet.datetimeRebaseModeInRead=LEGACY \
  --conf spark.sql.parquet.int96RebaseModeInRead=LEGACY \
  --conf spark.sql.files.maxPartitionBytes=67108864 \
  --conf spark.local.dir="$SCRATCH" \
  "$PROJECT/build_foodservice_controls_v2.py" \
    --final-root "$FINAL_ROOT" \
    --origin-v4 "$ORIGIN_V4" \
    --full-histories "$FULL_HISTORIES" \
    --education "$EDUCATION" \
    --users "$USERS" \
    --output-root "$OUTPUT_ROOT" \
    --output-partitions 512 \
    --threads "$SLURM_CPUS_PER_TASK" \
    --tmpdir "$SCRATCH"
