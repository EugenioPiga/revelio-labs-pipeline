#!/bin/bash
#SBATCH --job-name=founder_firmyr_v3
#SBATCH --partition=general
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=350G
#SBATCH --output=/home/epiga/innovation_linkedin/logs/founder_firmyr_v3_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/founder_firmyr_v3_%j.err
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT
#SBATCH --mail-user=epiga@ucsd.edu

set -eo pipefail
source "$HOME/miniconda/bin/activate" revelio

ROOT="$HOME/innovation_linkedin"
OUT="/home/epiga/revelio_labs/output/founder_firm_outcomes_v3"
SCRATCH="/labs/khanna/linkedin_202507/scratch/founder_firmyr_v3_${SLURM_JOB_ID}"

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
  --driver-memory 300g \
  --conf spark.sql.shuffle.partitions=2400 \
  --conf spark.sql.adaptive.enabled=true \
  "$ROOT/src/pipelines/founder_firm_outcomes_v3.py" \
  --mode firm_year \
  --out-dir "$OUT" \
  --threads "$SLURM_CPUS_PER_TASK" \
  --shuffle-partitions 2400 \
  --coalesce 200 \
  --tmpdir "$SCRATCH"
