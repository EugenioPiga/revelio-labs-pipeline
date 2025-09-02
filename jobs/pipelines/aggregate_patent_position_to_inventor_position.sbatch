#!/usr/bin/env bash
#SBATCH --job-name=inventor_position
#SBATCH --output=logs/inventor_position_%j.out
#SBATCH --error=logs/inventor_position_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=200G
#SBATCH --partition=general
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

# --- 3) JVM + Spark tuning
export SPARK_DRIVER_MEMORY=120g
export SPARK_EXECUTOR_MEMORY=120g
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run aggregation script (no sharding here)
python -u aggregate_patent_position_to_inventor_position.py \
  --input "output/inventors_matched_positions_spark" \
  --out-dir "output/inventor_position_file" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 400 \
  --coalesce 200
