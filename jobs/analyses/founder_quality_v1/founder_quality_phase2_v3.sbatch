#!/bin/bash
#SBATCH --job-name=fqV3Phase2
#SBATCH --partition=general
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=48G
#SBATCH --output=/home/epiga/revelio_labs/logs/founder_quality_v1/phase2_v3/fqV3Phase2_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/founder_quality_v1/phase2_v3/fqV3Phase2_%j.err

set -eo pipefail
mkdir -p /home/epiga/revelio_labs/logs/founder_quality_v1/phase2_v3
mkdir -p /home/epiga/revelio_labs/output/founder_quality_v1/phase2_v3
source /home/epiga/miniconda/bin/activate revelio

VALIDATION_RUN=$(ls -dt /home/epiga/revelio_labs/output/founder_quality_v1/validation_v3/run_* | head -1)
if [ -z "$VALIDATION_RUN" ]; then
  echo "ERROR: no V3 validation run found" >&2
  exit 2
fi

echo "[INFO] $(date -Is) job=${SLURM_JOB_ID:-NA} host=$(hostname)"
echo "[INFO] python=$(which python)"
python --version
printf '[INFO] validation_run=%s\n' "$VALIDATION_RUN"

exec python /home/epiga/revelio_labs/src/founder_quality_v1/analyze_founder_quality_phase2_v3.py \
  --validation-run "$VALIDATION_RUN" \
  --out-root /home/epiga/revelio_labs/output/founder_quality_v1/phase2_v3 \
  --shuffle-partitions 320
