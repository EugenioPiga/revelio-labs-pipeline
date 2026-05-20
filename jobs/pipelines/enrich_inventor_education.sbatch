#!/usr/bin/env bash
#SBATCH --job-name=enrich_inventor_education
#SBATCH --output=logs/enrich_inventor_education_%j.out
#SBATCH --error=logs/enrich_inventor_education_%j.err
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

# --- 2) Ensure Java 17 is available in $HOME
JAVA_DIR="$HOME/jdk-17"
if [ ! -x "$JAVA_DIR/bin/java" ]; then
  echo "[JAVA] Java 17 not found in $JAVA_DIR. Downloading..."
  cd $HOME
  wget -q https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.14+7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  tar -xzf OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  mv jdk-17.0.14+7 jdk-17
fi

# Force Spark to use Java 17
export JAVA_HOME="$JAVA_DIR"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 3) Scratch directory
SCRATCH_BASE="${SCRATCH_BASE:-$HOME/.spark_scratch}"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM tuning
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-90g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-90g}"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run enrichment
python -u enrich_inventor_education.py \
  --matched-dir "output/inventors_matched_education_spark" \
  --education-dir "academic_individual_user_education" \
  --out-dir "output/inventors_enriched_education" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "200" \
  --coalesce "80" \
  --tmpdir "$SCRATCH"

# --- 5) Cleanup
rm -rf "$SCRATCH" || true
