#!/bin/bash
#SBATCH --job-name=restaurant_select
#SBATCH --partition=general
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=600G
#SBATCH --output=/home/epiga/innovation_linkedin/logs/restaurant_select_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/restaurant_select_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT
#SBATCH --mail-user=epiga@ucsd.edu

set -eo pipefail
source "$HOME/miniconda/bin/activate" revelio

ROOT="$HOME/innovation_linkedin"
OUT="/home/epiga/revelio_labs/output/us_restaurant_histories_v3"
SCRATCH="/labs/khanna/linkedin_202507/scratch/restaurant_select_${SLURM_JOB_ID}"

export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
mkdir -p "$TMPDIR" "$SCRATCH/warehouse"

cleanup() {
  for _ in 1 2 3; do rm -rf "$SCRATCH" 2>/dev/null && return 0; sleep 2; done
  echo "[WARN] Scratch retained after cleanup: $SCRATCH" >&2
  return 0
}
trap cleanup EXIT

spark-submit \
  --master "local[${SLURM_CPUS_PER_TASK}]" \
  --driver-memory 540g \
  --conf spark.sql.shuffle.partitions=4000 \
  --conf spark.sql.adaptive.enabled=true \
  --conf spark.sql.autoBroadcastJoinThreshold=-1 \
  "$ROOT/src/pipelines/extract_us_restaurant_histories_v3.py" \
  --mode select_restaurant_population \
  --out-dir "$OUT" \
  --restaurant-naics-prefix 7225 \
  --threads "$SLURM_CPUS_PER_TASK" \
  --shuffle-partitions 4000 \
  --coalesce 400 \
  --tmpdir "$SCRATCH"
