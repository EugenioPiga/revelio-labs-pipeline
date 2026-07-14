#!/bin/bash
#SBATCH --job-name=fqV3Audit
#SBATCH --output=/home/epiga/revelio_labs/logs/founder_quality_v1/phase0_v3/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/founder_quality_v1/phase0_v3/%x_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=120G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general

# V3-only founder-quality audit. It is read-only outside its dedicated output root.
# Deliberately no `set -u`: the established conda activation hook can use optional variables.
set -eo pipefail

ROOT=/home/epiga/revelio_labs
LOG_DIR="$ROOT/logs/founder_quality_v1/phase0_v3"
OUT_ROOT="$ROOT/output/founder_quality_v1/audit_v3"
mkdir -p "$LOG_DIR" "$OUT_ROOT"

# Established SSRDE project environment.
source /home/epiga/miniconda/bin/activate revelio

export PYSPARK_PYTHON="$(command -v python)"
export SPARK_LOCAL_DIRS="${TMPDIR:-/tmp}/${USER}/founder_quality_phase0_v3_${SLURM_JOB_ID}"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
mkdir -p "$SPARK_LOCAL_DIRS"
trap 'rm -rf "$SPARK_LOCAL_DIRS"' EXIT

cd "$ROOT"
echo "[INFO] $(date --iso-8601=seconds) job=${SLURM_JOB_ID} host=$(hostname)"
echo "[INFO] python=$(command -v python)"
python --version
python - <<'PY'
import pyspark
print(f"[INFO] pyspark={pyspark.__version__}")
PY

python src/founder_quality_v1/audit_founder_quality_v3.py \
  --project-root "$ROOT" \
  --output-root "$OUT_ROOT" \
  --threads "${SLURM_CPUS_PER_TASK}"
