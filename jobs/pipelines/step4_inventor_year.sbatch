#!/usr/bin/env bash
#SBATCH --job-name=step4_inv_year
#SBATCH --output=logs/step4_inv_year_%A_%a.out
#SBATCH --error=logs/step4_inv_year_%A_%a.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --array=0-23
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

echo "[INFO] Job started on $(hostname) at $(date)"

# ============================================================
# 1) Environment
# ============================================================
source "$HOME/miniconda/bin/activate" revelio
echo "[INFO] Using Python from: $(which python)"
python --version

# ============================================================
# 2) Java 17
# ============================================================
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# ============================================================
# 3) Scratch ON /labs (avoid home quota)
# ============================================================
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_inv_year"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM tuning
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# ============================================================
# 4) Paths (NEW pipeline outputs)
# ============================================================
STEP1_DIR="/labs/khanna/linkedin_202507/processed/step1_output/inventors_matched_users"
STEP2_DIR="/labs/khanna/linkedin_202507/processed/step2_output/inventors_matched_positions_spark"
STEP3_DIR="/labs/khanna/linkedin_202507/processed/step3_output/inventors_matched_education_spark"
USPTO_DIR="/labs/khanna/linkedin_202507/processed/uspto_aux_202507"

OUT_DIR="/labs/khanna/linkedin_202507/processed/inventor_year_shards_v2"
mkdir -p "$OUT_DIR"

# ============================================================
# 5) Run shard
# ============================================================
python -u step4_inventor_year.py \
  --shards 24 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}" \
  --step1-dir "$STEP1_DIR" \
  --step2-dir "$STEP2_DIR" \
  --step3-dir "$STEP3_DIR" \
  --uspto-aux-dir "$USPTO_DIR" \
  --out-dir "$OUT_DIR" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "1200" \
  --coalesce "8" \
  --tmpdir "$SCRATCH"

# ============================================================
# 6) Cleanup
# ============================================================
rm -rf "$SCRATCH" || true
echo "[INFO] Done."
