#!/usr/bin/env bash
#SBATCH --job-name=step5_origin_v3
#SBATCH --output=/home/epiga/revelio_labs/logs/step5_origin_v3_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/step5_origin_v3_%j.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=300G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/revelio_labs/pipeline_v3_spell_integrity"

export JOB_LABEL="step5_origin_v3"
export SPARK_DRIVER_MEMORY="240g"
export SPARK_EXECUTOR_MEMORY="240g"

source "${V3_DIR}/jobs/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

CORRECTED_STEP4="${STEP4_SHARDS_OUT%/}_origin_any_education"
CORRECTED_STEP5="${STEP5_MERGED_OUT%/}_origin_any_education"

echo "============================================================"
echo "CORRECTED STEP 5 MERGE"
echo "============================================================"
echo "Input:  ${CORRECTED_STEP4}"
echo "Output: ${CORRECTED_STEP5}"
echo "============================================================"

if [[ ! -d "${CORRECTED_STEP4}" ]]; then
    echo "ERROR: Corrected Step 4 input not found: ${CORRECTED_STEP4}" >&2
    exit 1
fi

success_count="$(
    find "${CORRECTED_STEP4}" \
        -mindepth 2 \
        -maxdepth 2 \
        -type f \
        -name '_SUCCESS' \
        | wc -l
)"

if [[ "${success_count}" -ne 24 ]]; then
    echo "ERROR: Expected 24 Step 4 _SUCCESS markers; found ${success_count}" >&2
    exit 1
fi

if [[ -e "${CORRECTED_STEP5}" ]]; then
    echo "ERROR: Corrected Step 5 output already exists:" >&2
    echo "${CORRECTED_STEP5}" >&2
    exit 1
fi

python -u "${V3_DIR}/step5_merge_inventor_year_shards_v3.py" \
  --input-shards-dir "${CORRECTED_STEP4}" \
  --out-dir "${CORRECTED_STEP5}" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 3000 \
  --out-partitions 300 \
  --tmpdir "${SCRATCH}"
