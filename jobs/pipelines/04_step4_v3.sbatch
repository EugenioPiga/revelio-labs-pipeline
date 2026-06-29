#!/usr/bin/env bash
#SBATCH --job-name=step4_invyear_v3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/step4_v3_%A_%a.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/step4_v3_%A_%a.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --array=0-23%3
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/innovation_linkedin"
export JOB_LABEL="step4_v3"
export SPARK_DRIVER_MEMORY="200g"
export SPARK_EXECUTOR_MEMORY="200g"

source "${V3_DIR}/jobs/pipelines/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

if [[ -z "${USPTO_AUX_DIR:-}" || "${USPTO_AUX_DIR}" == "/PATH/TO/EXISTING/USPTO_AUX_DIR" ]]; then
    echo "ERROR: Set USPTO_AUX_DIR in ${V3_DIR}/PATHS_TO_EDIT.env" >&2
    exit 1
fi

python -u "${V3_DIR}/src/pipelines/step4_inventor_year_v3.py" \
  --step1-dir "${STEP1_OUT}" \
  --step2-dir "${STEP2_OUT}" \
  --step3-dir "${STEP3_OUT}" \
  --company-ref "${COMPANY_REF}" \
  --uspto-aux-dir "${USPTO_AUX_DIR}" \
  --out-dir "${STEP4_SHARDS_OUT}" \
  --shards 24 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}" \
  --panel-max-year 2025 \
  --main-position-rule longest_overlap \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 1600 \
  --coalesce 8 \
  --tmpdir "${SCRATCH}"
