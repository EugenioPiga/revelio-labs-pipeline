#!/usr/bin/env bash
#SBATCH --job-name=merge_pos_edu_spark
#SBATCH --output=logs/merge_pos_edu_spark_%j.out
#SBATCH --error=logs/merge_pos_edu_spark_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# --- 1) Activate venv
source "$HOME/.venvs/revelio/bin/activate"

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
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 3) Local scratch directory
SCRATCH="$HOME/scratch/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM tuning
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-90g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-90g}"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run Spark merge job
python -u merge_position_education.py \
  --pos-dir "/labs/khanna/linkedin_202507/processed/inventor_position_file" \
  --edu-dir "/labs/khanna/linkedin_202507/processed/inventors_matched_education_enriched" \
  --out-dir "/labs/khanna/linkedin_202507/processed/inventor_position_education" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "200" \
  --coalesce "80" \
  --tmpdir "$SCRATCH"

# --- 5) Cleanup
rm -rf "$SCRATCH" || true
