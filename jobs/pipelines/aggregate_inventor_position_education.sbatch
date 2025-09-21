#!/usr/bin/env bash
#SBATCH --job-name=agg_pos_with_edu
#SBATCH --output=logs/agg_pos_with_edu_%j.out
#SBATCH --error=logs/agg_pos_with_edu_%j.err
#SBATCH --time=24:00:00              # try shorter runtime first; extend if needed
#SBATCH --cpus-per-task=64           # nodes now have 128 cores, request half
#SBATCH --mem=500G                   # nodes have 2.25TB, so 500GB is safe & available
#SBATCH --partition=general          # only partition available now
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

# --- 3) Scratch directory in your home (quota-aware)
SCRATCH_BASE="$HOME/.spark_scratch"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# JVM tuning (adapted to new big nodes)
export SPARK_DRIVER_MEMORY="200g"
export SPARK_EXECUTOR_MEMORY="200g"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run aggregation
python -u aggregate_inventor_position_education.py \
  --in-dir "/labs/khanna/linkedin_202507/processed/inventor_position_education" \
  --out-dir "/labs/khanna/linkedin_202507/processed/inventor_position_with_education"

# --- 5) Cleanup
rm -rf "$SCRATCH" || true

