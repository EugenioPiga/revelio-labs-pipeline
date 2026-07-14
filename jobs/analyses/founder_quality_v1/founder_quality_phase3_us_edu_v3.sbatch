#!/bin/bash
#SBATCH --job-name=fqV3USEdu
#SBATCH --partition=general
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=120G
#SBATCH --output=/home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3/fqV3USEdu_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3/fqV3USEdu_%j.err

set -eo pipefail
cd /home/epiga/revelio_labs
mkdir -p /home/epiga/revelio_labs/logs/founder_quality_v1/phase3_us_edu_v3
source /home/epiga/miniconda/bin/activate revelio

VALIDATION_RUN=$(ls -dt /home/epiga/revelio_labs/output/founder_quality_v1/validation_v3/run_* | head -1)
if [ -z "${VALIDATION_RUN}" ]; then
  echo "[ERROR] No completed validation run found" >&2
  exit 1
fi

python /home/epiga/revelio_labs/src/founder_quality_v1/analyze_founder_quality_phase3_us_edu_v3.py   --validation-run "${VALIDATION_RUN}"   --out-root /home/epiga/revelio_labs/output/founder_quality_v1/phase3_us_edu_v3   --shuffle-partitions 320
