#!/usr/bin/env bash
#SBATCH --job-name=step1_inv_users_spark
#SBATCH --output=logs/step1_inv_users_spark_%j.out
#SBATCH --error=logs/step1_inv_users_spark_%j.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# 1) Activate env (your standard)
source "$HOME/miniconda/bin/activate" revelio
echo "[INFO] Using python: $(which python)"
python -V

# 2) Ensure Java 17 (same as step2)
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# 3) Scratch
SCRATCH_BASE="${SCRATCH_BASE:-$HOME/.spark_scratch}"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# 4) Spark tuning
export SPARK_DRIVER_MEMORY=200g
export SPARK_EXECUTOR_MEMORY=200g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# 5) Paths
USERS_DIR="/labs/khanna/linkedin_202507/academic_individual_user"
INVENTORS_DIR="/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
OUT_DIR="/labs/khanna/linkedin_202507/processed/step1_output/inventors_matched_users"

# 6) Run
python -u step1_inventors_matched_users.py \
  --users-dir "$USERS_DIR" \
  --inventors-dir "$INVENTORS_DIR" \
  --out-dir "$OUT_DIR" \
  --shuffle-partitions 400 \
  --coalesce 200 \
  --tmpdir "$SCRATCH" \
  --prefix-user-cols

# 7) Cleanup
rm -rf "$SCRATCH" || true
echo "[INFO] Done."
