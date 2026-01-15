#!/usr/bin/env bash
#SBATCH --job-name=build_public_firms
#SBATCH --output=logs/build_public_firms_%A_%a.out
#SBATCH --error=logs/build_public_firms_%A_%a.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --array=0-99
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] SLURM_JOB_ID=${SLURM_JOB_ID} SLURM_ARRAY_TASK_ID=${SLURM_ARRAY_TASK_ID}"
echo "[INFO] CPUs=${SLURM_CPUS_PER_TASK} MEM=${SLURM_MEM_PER_NODE:-NA}"

# =========================================================
# Environment
# =========================================================
source ~/miniconda/bin/activate revelio
echo "[INFO] Using Python from: $(which python)"
python --version

# --- Ensure Java 17 
JAVA_DIR="$HOME/jdk-17"
if [ ! -x "$JAVA_DIR/bin/java" ]; then
  echo "[JAVA] Java 17 not found in $JAVA_DIR. Downloading..."
  cd "$HOME"
  wget -q https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.14%2B7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  tar -xzf OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  mv jdk-17.0.14+7 jdk-17
fi
export JAVA_HOME="$JAVA_DIR"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- Extra Spark memory tuning 
export SPARK_DRIVER_MEMORY=450g
export SPARK_EXECUTOR_MEMORY=450g

# --- Keep thread-heavy libs from oversubscribing
export OMP_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export MKL_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export OPENBLAS_NUM_THREADS=${SLURM_CPUS_PER_TASK}
export NUMEXPR_NUM_THREADS=${SLURM_CPUS_PER_TASK}

# =========================================================
# Scratch dirs (recommended for spill/checkpoints)
# =========================================================
SCRATCH_BASE="${SCRATCH:-/labs/khanna/linkedin_202507/scratch}/${USER}/inv_pat_pub/${SLURM_JOB_ID}/${SLURM_ARRAY_TASK_ID}"
mkdir -p "${SCRATCH_BASE}/spark_local" "${SCRATCH_BASE}/spark_tmp" "${SCRATCH_BASE}/spark_checkpoints"

export SPARK_LOCAL_DIRS="${SCRATCH_BASE}/spark_local"
export TMPDIR="${SCRATCH_BASE}/spark_tmp"
export SPARK_CHECKPOINT_DIR="${SCRATCH_BASE}/spark_checkpoints"

echo "[INFO] SCRATCH_BASE=${SCRATCH_BASE}"

# =========================================================
# Run shard task
# =========================================================
python -u "$HOME/revelio_labs/build_inventor_patent_public_firms.py" "${SLURM_ARRAY_TASK_ID}"

echo "[INFO] Done at $(date)"
