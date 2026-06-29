#!/bin/bash
#SBATCH --job-name=founder_shard_v3
#SBATCH --partition=general
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=24
#SBATCH --mem=240G
#SBATCH --array=0-23%3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/founder_shard_v3_%A_%a.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/founder_shard_v3_%A_%a.err
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT
#SBATCH --mail-user=epiga@ucsd.edu

set -eo pipefail
source "$HOME/miniconda/bin/activate" revelio

ROOT="$HOME/innovation_linkedin"
OUT="/home/epiga/revelio_labs/output/founder_firm_outcomes_v3"
SCRATCH="/labs/khanna/linkedin_202507/scratch/founder_shard_v3_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}"

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
  --driver-memory 205g \
  --conf spark.sql.shuffle.partitions=1600 \
  --conf spark.sql.adaptive.enabled=true \
  "$ROOT/src/pipelines/founder_firm_outcomes_v3.py" \
  --mode founder_shard \
  --out-dir "$OUT" \
  --shards 24 \
  --shard-idx "$SLURM_ARRAY_TASK_ID" \
  --link-definition core \
  --threads "$SLURM_CPUS_PER_TASK" \
  --shuffle-partitions 1600 \
  --coalesce 96 \
  --tmpdir "$SCRATCH"
