#!/usr/bin/env bash
#SBATCH --job-name=step3_inv_edu_v3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/step3_v3_%A_%a.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/step3_v3_%A_%a.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=200G
#SBATCH --partition=general
#SBATCH --array=0-23%4
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/innovation_linkedin"
export JOB_LABEL="step3_v3"
export SPARK_DRIVER_MEMORY="160g"
export SPARK_EXECUTOR_MEMORY="160g"

source "${V3_DIR}/jobs/pipelines/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

python -u "${V3_DIR}/src/pipelines/step3_inventors_matched_positions_education_spark_v3.py" \
  --step2-dir "${STEP2_OUT}" \
  --education-dir "${EDUCATION_DIR}" \
  --out-dir "${STEP3_OUT}" \
  --shards 24 \
  --shard-idx "${SLURM_ARRAY_TASK_ID}" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 1200 \
  --coalesce 100 \
  --tmpdir "${SCRATCH}"
