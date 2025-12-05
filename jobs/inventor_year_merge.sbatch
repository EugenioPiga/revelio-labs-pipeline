#!/usr/bin/env bash
#SBATCH --job-name=inv_year_merge
#SBATCH --output=logs/inv_year_merge_%A_%a.out
#SBATCH --error=logs/inv_year_merge_%A_%a.err
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

# =========================================================
# Environment
# =========================================================
source ~/miniconda/bin/activate revelio
echo "[INFO] Using Python from: $(which python)"
python --version

# --- 2) Ensure Java 17
JAVA_DIR="$HOME/jdk-17"
if [ ! -x "$JAVA_DIR/bin/java" ]; then
  echo "[JAVA] Java 17 not found in $JAVA_DIR. Downloading..."
  cd $HOME
  wget -q https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.14+7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  tar -xzf OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  mv jdk-17.0.14+7 jdk-17
fi

export JAVA_HOME="$JAVA_DIR"
export PATH="$JAVA_HOME/bin:$PATH"

# --- 3) Extra Spark memory tuning
export SPARK_DRIVER_MEMORY=450g
export SPARK_EXECUTOR_MEMORY=450g

echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 4) Run shard task
python "$HOME/revelio_labs/inventor_year_merge.py" $SLURM_ARRAY_TASK_ID
