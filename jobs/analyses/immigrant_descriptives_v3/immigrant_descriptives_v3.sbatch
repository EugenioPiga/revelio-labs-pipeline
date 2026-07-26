#!/usr/bin/env bash
#SBATCH --job-name=immig_desc_v3
#SBATCH --output=/home/epiga/revelio_labs/logs/immig_desc_v3_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/immig_desc_v3_%j.err
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=350G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,REQUEUE,TIME_LIMIT

set -euo pipefail

ROOT="/home/epiga/revelio_labs"
PIPELINE_DIR="${ROOT}/pipeline_v3_spell_integrity"
PROJECT_DIR="${ROOT}/immigrant_descriptives_v3"
SCRIPT="${PROJECT_DIR}/immigrant_descriptives_v3.py"

INPUT_PANEL="/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged_colleague_origin_any_education"
POSITIONS_DIR="/labs/khanna/linkedin_202507/processed/v3_spell_integrity/step2_inventors_matched_positions"
OUT_BASE="${ROOT}/output/immigrant_descriptives_v3_usyears_lastUSeducation"
RUN_OUT="${OUT_BASE}/run_${SLURM_JOB_ID}"

export JOB_LABEL="immig_desc_v3"
export SPARK_DRIVER_MEMORY="300g"
export SPARK_EXECUTOR_MEMORY="300g"
export MPLBACKEND="Agg"
export OMP_NUM_THREADS="1"
export OPENBLAS_NUM_THREADS="1"
export MKL_NUM_THREADS="1"
export NUMEXPR_NUM_THREADS="1"

mkdir -p "${ROOT}/logs" "${PROJECT_DIR}" "${OUT_BASE}"

source "${PIPELINE_DIR}/jobs/v3_runtime_env.sh"

if [[ ! -f "${SCRIPT}" ]]; then
    echo "ERROR: Python script not found: ${SCRIPT}" >&2
    exit 1
fi

if [[ ! -f "${INPUT_PANEL}/_SUCCESS" ]]; then
    echo "ERROR: Corrected Step 6 input is missing _SUCCESS: ${INPUT_PANEL}" >&2
    exit 1
fi

if [[ ! -d "${POSITIONS_DIR}" ]]; then
    echo "ERROR: Canonical positions directory not found: ${POSITIONS_DIR}" >&2
    exit 1
fi

if [[ -e "${RUN_OUT}" ]]; then
    echo "ERROR: Run output already exists: ${RUN_OUT}" >&2
    exit 1
fi

echo "================================================================================"
echo "CORRECTED V3 IMMIGRANT DESCRIPTIVES"
echo "================================================================================"
echo "Job ID:          ${SLURM_JOB_ID}"
echo "Host:            $(hostname)"
echo "Input panel:     ${INPUT_PANEL}"
echo "Positions:       ${POSITIONS_DIR}"
echo "Output:          ${RUN_OUT}"
echo "Years:           2000-2024"
echo "Citation window: through 2019 for mature-year summaries"
echo "Restrictions:    current U.S. person-years; last observed education in U.S."
echo "Samples:         all inventors; ever-founders"
echo "================================================================================"

python -u "${SCRIPT}" \
  --input-panel "${INPUT_PANEL}" \
  --positions-dir "${POSITIONS_DIR}" \
  --out-dir "${RUN_OUT}" \
  --year-start 2000 \
  --year-end 2024 \
  --citation-end-year 2019 \
  --tenure-max 50 \
  --tenure-bin-width 5 \
  --min-cluster 10 \
  --decomp-min-cluster 50 \
  --top-origins 20 \
  --top-categories 20 \
  --founder-event-pre 5 \
  --founder-event-post 10 \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 3000 \
  --tmpdir "${SCRATCH}"

test -f "${RUN_OUT}/_DESCRIPTIVES_COMPLETE"

echo "${RUN_OUT}" > "${OUT_BASE}/latest_completed_run.txt"

echo "[DONE] Detailed immigrant descriptives completed: ${RUN_OUT}"
