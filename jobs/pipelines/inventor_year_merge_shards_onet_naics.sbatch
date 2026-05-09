#!/usr/bin/env bash
#SBATCH --job-name=merge_inv_year
#SBATCH --output=logs/merge_inv_year_%j.out
#SBATCH --error=logs/merge_inv_year_%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

mkdir -p logs

echo "============================================================"
echo "[INFO] Merge inventor-year shards with O*NET / NAICS"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] Node: ${SLURMD_NODENAME:-unknown}"
echo "[INFO] Started at: $(date)"
echo "============================================================"

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
# 3) Scratch on /labs
# ============================================================
SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_merge_inv_year_onet_naics"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local,warehouse}

export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

export SPARK_DRIVER_MEMORY=420g
export SPARK_EXECUTOR_MEMORY=420g
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# ============================================================
# 4) Paths
# ============================================================
CODE_DIR="$HOME/revelio_labs"
MERGE_SCRIPT="$CODE_DIR/inventor_year_merge_shards.py"

IN_DIR="/labs/khanna/linkedin_202507/processed/inventor_year_shards_v2_onet_naics"
OUT_DIR="/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2_onet_naics"

echo "[PATH] CODE_DIR     = $CODE_DIR"
echo "[PATH] MERGE_SCRIPT = $MERGE_SCRIPT"
echo "[PATH] IN_DIR       = $IN_DIR"
echo "[PATH] OUT_DIR      = $OUT_DIR"
echo "[PATH] SCRATCH      = $SCRATCH"

# ============================================================
# 5) Pre-checks
# ============================================================
if [ ! -f "$MERGE_SCRIPT" ]; then
  echo "[ERROR] Merge script not found: $MERGE_SCRIPT"
  exit 1
fi

if [ ! -d "$IN_DIR" ]; then
  echo "[ERROR] Input shard directory not found: $IN_DIR"
  exit 1
fi

N_SHARDS=$(find "$IN_DIR" -maxdepth 1 -type d -name "shard=*" | wc -l)
echo "[CHECK] Number of shard folders found: $N_SHARDS"

if [ "$N_SHARDS" -ne 24 ]; then
  echo "[ERROR] Expected 24 shard folders but found $N_SHARDS"
  find "$IN_DIR" -maxdepth 1 -type d -name "shard=*" | sort
  exit 1
fi

cd "$CODE_DIR"

# ============================================================
# 6) Run merge
# ============================================================
echo "[INFO] Running merge script..."

python -u "$MERGE_SCRIPT"


# ============================================================
# 7) Cleanup
# ============================================================
rm -rf "$SCRATCH" || true

echo "============================================================"
echo "[INFO] Finished merge job at: $(date)"
echo "[INFO] Final output:"
echo "       $OUT_DIR"
echo "============================================================"
