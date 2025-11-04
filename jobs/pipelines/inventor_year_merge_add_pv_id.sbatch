#!/usr/bin/env bash
#SBATCH --job-name=inv_year_add_pvid
#SBATCH --output=logs/inv_year_add_pvid_%j.out
#SBATCH --error=logs/inv_year_add_pvid_%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=48
#SBATCH --mem=400G
#SBATCH --partition=general
#SBATCH --mail-user=yuhei.miyauchi@gmail.com
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

echo "[INFO] Job started on $(hostname) at $(date)"

# --- 1) Move to submission directory and activate environment
cd "$SLURM_SUBMIT_DIR"
source "$SLURM_SUBMIT_DIR/venv/bin/activate"
echo "[INFO] Using Python from: $(which python)"
python --version

# --- 2) Ensure Java 17 is available
JAVA_DIR="$HOME/jdk-17"
if [ ! -x "$JAVA_DIR/bin/java" ]; then
  echo "[JAVA] Java 17 not found in $JAVA_DIR. Downloading..."
  cd "$HOME"
  wget -q https://github.com/adoptium/temurin17-binaries/releases/download/jdk-17.0.14+7/OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  tar -xzf OpenJDK17U-jdk_x64_linux_hotspot_17.0.14_7.tar.gz
  mv jdk-17.0.14+7 jdk-17
fi
export JAVA_HOME="$JAVA_DIR"
export PATH="$JAVA_HOME/bin:$PATH"
echo "[JAVA] Using $(java -version 2>&1 | head -n 1)"

# --- 3) Configure Spark scratch + memory
SCRATCH_BASE="$HOME/.spark_scratch"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"
export SPARK_DRIVER_MEMORY="250g"
export SPARK_EXECUTOR_MEMORY="250g"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 4) Run enrichment script
REPO_DIR="$SLURM_SUBMIT_DIR"
python -u "$REPO_DIR/src/pipeline/inventor_year_merge_add_pv_id.py" \
  --matches-path "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches" \
  --inventor-year-path "/labs/khanna/linkedin_202507/processed/inventor_year_merged" \
  --correspondence-path "/labs/khanna/linkedin_202507/processed/userid_pvid_correspondence" \
  --output-path "/labs/khanna/linkedin_202507/processed/inventor_year_merged_with_pvid" \
  --shuffle-partitions 400

# --- 5) Cleanup scratch
rm -rf "$SCRATCH" || true

echo "[INFO] Job finished at $(date)"
