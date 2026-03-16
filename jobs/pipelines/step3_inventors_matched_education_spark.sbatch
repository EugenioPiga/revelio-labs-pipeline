#!/usr/bin/env bash
#SBATCH --job-name=step3_inv_pos_edu
#SBATCH --output=logs/step3_inv_pos_edu_%A_%a.out
#SBATCH --error=logs/step3_inv_pos_edu_%A_%a.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --array=0-23
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# 1) Conda env
source "$HOME/miniconda/bin/activate" revelio
echo "[INFO] Using python: $(which python)"
python -V

# 2) Java 17
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# 3) Scratch ON /labs (avoid home quota)
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_step3"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# 4) JVM/Spark tuning
export SPARK_DRIVER_MEMORY=200g
export SPARK_EXECUTOR_MEMORY=200g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# 5) Paths (NEW PATHS CONSISTENT WITH STEP2)
STEP2_DIR="/labs/khanna/linkedin_202507/processed/step2_output/inventors_matched_positions_spark"
EDU_DIR="/labs/khanna/linkedin_202507/academic_individual_user_education"
OUT_DIR="/labs/khanna/linkedin_202507/processed/step3_output/inventors_matched_education_spark"
mkdir -p "$OUT_DIR"

# 6) Run one shard
python -u step3_inventors_matched_education_spark.py \
  --step2-dir "$STEP2_DIR" \
  --education-dir "$EDU_DIR" \
  --out-dir "$OUT_DIR" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "1200" \
  --coalesce "200" \
  --tmpdir "$SCRATCH" \
  --shards 24 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}"

# 7) Cleanup
rm -rf "$SCRATCH" || true
