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

echo "============================================================"
echo "[INFO] Step 4: inventor-year panel with O*NET / NAICS / roles"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] Array task: ${SLURM_ARRAY_TASK_ID}"
echo "[INFO] Node: ${SLURMD_NODENAME:-unknown}"
echo "[INFO] Started at: $(date)"
echo "============================================================"

# ============================================================
# 1) Environment
# ============================================================
# Temporarily disable nounset around conda activation.
set +u
source "$HOME/miniconda/bin/activate" revelio
set -u
echo "[INFO] Using Python from: $(which python)"
python --version

# ============================================================
# 2) Java 17
# ============================================================
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# ============================================================
# 3) Scratch ON /labs
# ============================================================
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_inv_year_onet_naics"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM / Spark tuning
export SPARK_DRIVER_MEMORY=420g
export SPARK_EXECUTOR_MEMORY=420g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# ============================================================
# 4) Paths
# ============================================================
CODE_DIR="$HOME/innovation_linkedin/src/pipelines"

STEP1_DIR="/labs/khanna/linkedin_202507/processed/step1_output/inventors_matched_users"

# Modified Step 2 output.
STEP2_DIR="/labs/khanna/linkedin_202507/processed/step2_output/inventors_matched_positions_spark"

# New Step 3 output.
STEP3_DIR="/labs/khanna/linkedin_202507/processed/step3_output/inventors_matched_education_spark_onet_naics"

USPTO_DIR="/labs/khanna/linkedin_202507/processed/uspto_aux_202507"

# New Step 4 output, so we do not overwrite old v2 shards.
OUT_DIR="/labs/khanna/linkedin_202507/processed/inventor_year_shards_v2_onet_naics"

mkdir -p "$OUT_DIR"

echo "[PATH] CODE_DIR  = $CODE_DIR"
echo "[PATH] STEP1_DIR = $STEP1_DIR"
echo "[PATH] STEP2_DIR = $STEP2_DIR"
echo "[PATH] STEP3_DIR = $STEP3_DIR"
echo "[PATH] USPTO_DIR = $USPTO_DIR"
echo "[PATH] OUT_DIR   = $OUT_DIR"
echo "[PATH] SCRATCH   = $SCRATCH"

# ============================================================
# 5) Pre-checks
# ============================================================
if [ ! -d "$CODE_DIR" ]; then
  echo "[ERROR] CODE_DIR does not exist: $CODE_DIR"
  exit 1
fi

if [ ! -f "$CODE_DIR/step4_inventor_year.py" ]; then
  echo "[ERROR] Step 4 script not found:"
  echo "        $CODE_DIR/step4_inventor_year.py"
  exit 1
fi

if [ ! -d "$STEP1_DIR" ]; then
  echo "[ERROR] STEP1_DIR does not exist: $STEP1_DIR"
  exit 1
fi

if [ ! -d "$STEP2_DIR" ]; then
  echo "[ERROR] STEP2_DIR does not exist: $STEP2_DIR"
  exit 1
fi

if [ ! -d "$STEP3_DIR" ]; then
  echo "[ERROR] STEP3_DIR does not exist: $STEP3_DIR"
  exit 1
fi

if [ ! -d "$USPTO_DIR" ]; then
  echo "[ERROR] USPTO_DIR does not exist: $USPTO_DIR"
  exit 1
fi

N_STEP2_SHARDS=$(find "$STEP2_DIR" -maxdepth 1 -type d -name "shard=*" | wc -l)
N_STEP3_SHARDS=$(find "$STEP3_DIR" -maxdepth 1 -type d -name "shard=*" | wc -l)

echo "[CHECK] Step 2 shard folders found: $N_STEP2_SHARDS"
echo "[CHECK] Step 3 shard folders found: $N_STEP3_SHARDS"

if [ "$N_STEP2_SHARDS" -ne 24 ]; then
  echo "[ERROR] Expected 24 Step 2 shard folders but found $N_STEP2_SHARDS"
  find "$STEP2_DIR" -maxdepth 1 -type d -name "shard=*" | sort
  exit 1
fi

if [ "$N_STEP3_SHARDS" -ne 24 ]; then
  echo "[ERROR] Expected 24 Step 3 shard folders but found $N_STEP3_SHARDS"
  find "$STEP3_DIR" -maxdepth 1 -type d -name "shard=*" | sort
  exit 1
fi

cd "$CODE_DIR"

# ============================================================
# 6) Run shard
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
# 7) Cleanup
# ============================================================
rm -rf "$SCRATCH" || true

echo "============================================================"
echo "[INFO] Finished Step 4 shard ${SLURM_ARRAY_TASK_ID} at: $(date)"
echo "[INFO] Output shard: $OUT_DIR/shard=$(printf '%02d' "${SLURM_ARRAY_TASK_ID}")"
echo "============================================================"
