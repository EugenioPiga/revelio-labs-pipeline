#!/bin/bash
#SBATCH --job-name=fqV3Phase1
#SBATCH --partition=general
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=80G
#SBATCH --output=/home/epiga/revelio_labs/logs/founder_quality_v1/phase1_v3/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/founder_quality_v1/phase1_v3/%x_%j.err

set -eo pipefail

PROJECT=/home/epiga/revelio_labs
mkdir -p "$PROJECT/logs/founder_quality_v1/phase1_v3"
mkdir -p "$PROJECT/output/founder_quality_v1/phase1_v3"

source /home/epiga/miniconda/bin/activate revelio

VALIDATION_RUN=$(ls -dt "$PROJECT"/output/founder_quality_v1/validation_v3/run_* 2>/dev/null | head -1)
if [ -z "$VALIDATION_RUN" ] || [ ! -f "$VALIDATION_RUN/VALIDATION_REPORT.md" ]; then
  echo "[ERROR] No completed V3 validation run found." >&2
  exit 2
fi

echo "[INFO] $(date --iso-8601=seconds) job=${SLURM_JOB_ID:-unknown} host=$(hostname)"
echo "[INFO] python=$(which python)"
python --version
echo "[INFO] validation_run=$VALIDATION_RUN"

exec python "$PROJECT/src/founder_quality_v1/analyze_founder_quality_phase1_v3.py" \
  --validation-run "$VALIDATION_RUN" \
  --out-root "$PROJECT/output/founder_quality_v1/phase1_v3" \
  --shuffle-partitions "320"
