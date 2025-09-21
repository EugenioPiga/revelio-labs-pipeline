#!/usr/bin/env bash
#SBATCH --job-name=check_inv_pos_edu
#SBATCH --output=logs/check_inv_pos_edu_%j.out
#SBATCH --error=logs/check_inv_pos_edu_%j.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=END,FAIL

set -euo pipefail
mkdir -p logs

# --- 1) Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# --- 2) Ensure Java 17
JAVA_DIR="$HOME/jdk-17"
export JAVA_HOME="$JAVA_DIR"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 3) Scratch
SCRATCH_BASE="${SCRATCH_BASE:-$HOME/.spark_scratch}"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM tuning
export SPARK_DRIVER_MEMORY="32g"
export SPARK_EXECUTOR_MEMORY="32g"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run check
python -u check_inventor_position_with_education_dask.py

# --- 5) Cleanup
rm -rf "$SCRATCH" || true
