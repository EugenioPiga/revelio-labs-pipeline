#!/usr/bin/env bash
#SBATCH --job-name=step3_inv_edu_spark
#SBATCH --partition=general
#SBATCH --time=72:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=200G
#SBATCH --array=0-7%2
#SBATCH --output=logs/step3_inv_edu_spark_%A_%a.out
#SBATCH --error=logs/step3_inv_edu_spark_%A_%a.err
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# --- venv ---
source "$HOME/.venvs/revelio/bin/activate"

# --- Find Java (11/17) and put on PATH ---
if [[ -z "${JAVA_HOME:-}" ]]; then
  for CAND in \
    /usr/lib/jvm/java-17-openjdk-amd64 \
    /usr/lib/jvm/java-11-openjdk-amd64 \
    /usr/lib/jvm/temurin-17-jdk-amd64 \
    /usr/lib/jvm/temurin-11-jdk-amd64; do
    if [[ -x "$CAND/bin/java" ]]; then
      export JAVA_HOME="$CAND"
      break
    fi
  done
fi
export PATH="${JAVA_HOME:+$JAVA_HOME/bin:}$PATH"
echo "[JAVA] JAVA_HOME=${JAVA_HOME:-unset}"
java -version

# --- Scratch/temp (fixes 'No space left on device' & tmp warnings) ---
SCRATCH="${SLURM_TMPDIR:-$HOME/.spark_scratch/${SLURM_JOB_ID}_${SLURM_ARRAY_TASK_ID}}"
mkdir -p "$SCRATCH"
export TMPDIR="$SCRATCH"
export SPARK_LOCAL_DIRS="$SCRATCH"
# Tell Java to use SCRATCH for its tmp
export _JAVA_OPTIONS="-Djava.io.tmpdir=$SCRATCH ${_JAVA_OPTIONS:-}"
export JAVA_TOOL_OPTIONS="-Djava.io.tmpdir=$SCRATCH ${JAVA_TOOL_OPTIONS:-}"
export PYSPARK_PYTHON="$(which python)"
export SPARK_DRIVER_MEMORY="140g"   # headroom below --mem=200G

echo "[ENV] SCRATCH=$SCRATCH"
echo "[ENV] SPARK_LOCAL_DIRS=$SPARK_LOCAL_DIRS"
echo "[ENV] TMPDIR=$TMPDIR"
echo "[ENV] SLURM_CPUS_PER_TASK=${SLURM_CPUS_PER_TASK}"

# --- Inputs/Outputs ---
STEP2_DIR="output/inventors_matched_positions_spark"
EDU_DIR="academic_individual_user_education"
OUT_DIR="output/inventors_matched_education_spark"
mkdir -p "$OUT_DIR"

# --- Run shard (NO stray quotes; each arg is properly quoted) ---
python -u step3_inventors_matched_education_spark.py \
  --step2-dir "$STEP2_DIR" \
  --education-dir "$EDU_DIR" \
  --out-dir "$OUT_DIR" \
  --threads "$SLURM_CPUS_PER_TASK" \
  --shuffle-partitions 600 \
  --tmpdir "$SPARK_LOCAL_DIRS" \
  --shards 8 \
  --shard-index "${SLURM_ARRAY_TASK_ID}" \
  --coalesce 64
