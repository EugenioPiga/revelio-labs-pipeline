#!/usr/bin/env bash
#SBATCH --job-name=step2_inv_pos_spark
#SBATCH --output=logs/step2_inv_pos_spark_%A_%a.out
#SBATCH --error=logs/step2_inv_pos_spark_%A_%a.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --array=0-23         # 24 shards
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# --- 1) Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# --- 2) Ensure Java 17
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 3) Scratch
SCRATCH_BASE="${SCRATCH_BASE:-$HOME/.spark_scratch}"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# --- 4) JVM + Spark tuning
export SPARK_DRIVER_MEMORY=200g
export SPARK_EXECUTOR_MEMORY=200g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 5) Run one shard
python -u step2_inventors_matched_positions_spark.py \
  --step1-dir "output/inventors_matched_users" \
  --positions-dir "academic_individual_position" \
  --out-dir "output/inventors_matched_positions_spark" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "400" \
  --coalesce "200" \
  --tmpdir "$SCRATCH" \
  --shards 24 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}"

# --- 6) Cleanup
rm -rf "$SCRATCH" || true

