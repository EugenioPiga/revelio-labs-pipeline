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

echo "============================================================"
echo "[INFO] Step 3: inventor-position + education"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] Array task: ${SLURM_ARRAY_TASK_ID}"
echo "[INFO] Node: ${SLURMD_NODENAME:-unknown}"
echo "[INFO] Started at: $(date)"
echo "============================================================"

# ============================================================
# 1) Conda env
# ============================================================
# Temporarily disable nounset around conda activation.
set +u
source "$HOME/miniconda/bin/activate" revelio
set -u
echo "[INFO] Using python: $(which python)"
python -V

# ============================================================
# 2) Java 17
# ============================================================
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# ============================================================
# 3) Scratch on /labs
# ============================================================
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_step3_onet_naics"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# ============================================================
# 4) JVM/Spark tuning
# ============================================================
export SPARK_DRIVER_MEMORY=200g
export SPARK_EXECUTOR_MEMORY=200g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# ============================================================
# 5) Paths
# ============================================================
CODE_DIR="$HOME/innovation_linkedin/src/pipelines"

# This is the modified Step 2 output you already created.
STEP2_DIR="/labs/khanna/linkedin_202507/processed/step2_output/inventors_matched_positions_spark"

EDU_DIR="/labs/khanna/linkedin_202507/academic_individual_user_education"

# New Step 3 output, so we do not mix old and new runs.
OUT_DIR="/labs/khanna/linkedin_202507/processed/step3_output/inventors_matched_education_spark_onet_naics"

mkdir -p "$OUT_DIR"

echo "[PATH] CODE_DIR  = $CODE_DIR"
echo "[PATH] STEP2_DIR = $STEP2_DIR"
echo "[PATH] EDU_DIR   = $EDU_DIR"
echo "[PATH] OUT_DIR   = $OUT_DIR"
echo "[PATH] SCRATCH   = $SCRATCH"

# ============================================================
# 6) Pre-checks
# ============================================================
if [ ! -d "$CODE_DIR" ]; then
  echo "[ERROR] CODE_DIR does not exist: $CODE_DIR"
  exit 1
fi

if [ ! -f "$CODE_DIR/step3_inventors_matched_education_spark.py" ]; then
  echo "[ERROR] Step 3 script not found:"
  echo "        $CODE_DIR/step3_inventors_matched_education_spark.py"
  exit 1
fi

if [ ! -d "$STEP2_DIR" ]; then
  echo "[ERROR] STEP2_DIR does not exist: $STEP2_DIR"
  exit 1
fi

if [ ! -d "$EDU_DIR" ]; then
  echo "[ERROR] EDU_DIR does not exist: $EDU_DIR"
  exit 1
fi

N_STEP2_SHARDS=$(find "$STEP2_DIR" -maxdepth 1 -type d -name "shard=*" | wc -l)
echo "[CHECK] Step 2 shard folders found: $N_STEP2_SHARDS"

if [ "$N_STEP2_SHARDS" -ne 24 ]; then
  echo "[ERROR] Expected 24 Step 2 shard folders but found $N_STEP2_SHARDS"
  find "$STEP2_DIR" -maxdepth 1 -type d -name "shard=*" | sort
  exit 1
fi

cd "$CODE_DIR"

# ============================================================
# 7) Run one shard
# ============================================================
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

# ============================================================
# 8) Cleanup
# ============================================================
rm -rf "$SCRATCH" || true

echo "============================================================"
echo "[INFO] Finished Step 3 shard ${SLURM_ARRAY_TASK_ID} at: $(date)"
echo "[INFO] Output shard: $OUT_DIR/shard=$(printf '%02d' "${SLURM_ARRAY_TASK_ID}")"
echo "============================================================"
