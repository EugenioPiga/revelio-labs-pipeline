#!/usr/bin/env bash
#SBATCH --job-name=step6_origin_v3
#SBATCH --output=/home/epiga/revelio_labs/logs/step6_origin_v3_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/step6_origin_v3_%j.err
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=450G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/revelio_labs/pipeline_v3_spell_integrity"

export JOB_LABEL="step6_origin_v3"
export SPARK_DRIVER_MEMORY="360g"
export SPARK_EXECUTOR_MEMORY="360g"

source "${V3_DIR}/jobs/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

CORRECTED_STEP5="${STEP5_MERGED_OUT%/}_origin_any_education"
CORRECTED_STEP6="${STEP6_OUT%/}_origin_any_education"

echo "============================================================"
echo "CORRECTED STEP 6"
echo "============================================================"
echo "Input panel: ${CORRECTED_STEP5}"
echo "Output:      ${CORRECTED_STEP6}"
echo "Link mode:   strict_current"
echo "============================================================"

if [[ ! -f "${CORRECTED_STEP5}/_SUCCESS" ]]; then
    echo "ERROR: Corrected Step 5 input is incomplete:" >&2
    echo "${CORRECTED_STEP5}" >&2
    exit 1
fi

if [[ -e "${CORRECTED_STEP6}" ]]; then
    echo "ERROR: Corrected Step 6 output already exists:" >&2
    echo "${CORRECTED_STEP6}" >&2
    exit 1
fi

python -u \
  "${V3_DIR}/step6_colleague_citation_collaboration_measures_v3.py" \
  --input-panel "${CORRECTED_STEP5}" \
  --patent-matches "${PATENT_MATCHES}" \
  --citations-tsv "/labs/khanna/USPTO_202507/g_us_patent_citation.tsv" \
  --out-dir "${CORRECTED_STEP6}" \
  --employment-link-mode strict_current \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 4000 \
  --out-partitions 300 \
  --tmpdir "${SCRATCH}"
