#!/usr/bin/env bash
#SBATCH --job-name=step2_inv_pos_spark
#SBATCH --output=logs/step2_inv_pos_spark_%A_%a.out
#SBATCH --error=logs/step2_inv_pos_spark_%A_%a.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --array=0-7
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# --- 1) Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# --- 2) Make "module" available (if your cluster uses Lmod/Environment Modules)
if [ -f /etc/profile.d/modules.sh ]; then
  source /etc/profile.d/modules.sh || true
fi

# --- 3) Ensure Java exists (try common modules, then system java)
have_java=0
if command -v module >/dev/null 2>&1; then
  module reset || true
  module load java/11 2>/dev/null || module load openjdk/11 2>/dev/null || module load openjdk/17 2>/dev/null || true
fi
if command -v java >/dev/null 2>&1; then
  have_java=1
fi
if [ "$have_java" -ne 1 ]; then
  echo "ERROR: No 'java' found on this node. Ask admins which Java module to load (e.g. 'module load java/11')."
  exit 90
fi

# Derive JAVA_HOME if not set
if [ -z "${JAVA_HOME:-}" ]; then
  JAVA_BIN="$(readlink -f "$(command -v java)")"
  export JAVA_HOME="$(dirname "$(dirname "$JAVA_BIN")")"
fi
echo "[JAVA] JAVA_HOME=$JAVA_HOME"
java -version || { echo "Java not runnable"; exit 91; }

# --- 4) Scratch on a larger filesystem (avoid /tmp)
SCRATCH_BASE="${SCRATCH_BASE:-$HOME/.spark_scratch}"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"

# quick space check
need_gb=15
free_k=$(df -Pk "$SCRATCH" | awk 'NR==2{print $4}')
free_g=$(( free_k / 1024 / 1024 ))
echo "[SCRATCH] $SCRATCH free ~${free_g}G"
if [ "$free_g" -lt "$need_gb" ]; then
  echo "ERROR: Not enough free space in $SCRATCH (need ${need_gb}G)."
  exit 99
fi

# JVM tuning within Slurm mem
export SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-90g}"
export SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-90g}"
export _JAVA_OPTIONS="-XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_EVENTLOG_ENABLED="false"

# --- 5) Run one shard (same PySpark script as before)
python -u step2_inventors_matched_positions_spark.py \
  --step1-dir "output/inventors_matched_users" \
  --positions-dir "academic_individual_position" \
  --out-dir "output/inventors_matched_positions_spark" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions "200" \
  --coalesce "80" \
  --tmpdir "$SCRATCH" \
  --shards 8 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}"

# --- 6) Cleanup
rm -rf "$SCRATCH" || true
