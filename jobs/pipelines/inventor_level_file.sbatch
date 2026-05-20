#!/usr/bin/env bash
#SBATCH --job-name=inventor_level
#SBATCH --output=logs/inventor_level_%A_%a.out
#SBATCH --error=logs/inventor_level_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=200G
#SBATCH --partition=general
#SBATCH --array=0-15       # run 16 shards (0..15)
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL

set -euo pipefail
mkdir -p logs

# --- activate venv
source "$HOME/.venvs/revelio/bin/activate"

# --- ensure Java 17
export JAVA_HOME=$HOME/jdk-17
export PATH=$JAVA_HOME/bin:$PATH
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- define a safe tmpdir
TMPDIR="$HOME/spark_tmp"
mkdir -p "$TMPDIR"

# --- Spark tuning (now with tmpdir override)
export SPARK_DRIVER_MEMORY=120g
export SPARK_EXECUTOR_MEMORY=120g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"

# --- run inventor-level job with sharding
python -u inventor_level_file.py \
  --input output/inventors_matched_education_enriched \
  --out-dir output/inventor_level_file \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 4000 \
  --coalesce 200 \
  --shards 16 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}" \
  --tmpdir "$TMPDIR"
