#!/usr/bin/env bash
#SBATCH --job-name=step1_inv_users_v3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/step1_v3_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/step1_v3_%j.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=40
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/innovation_linkedin"
export JOB_LABEL="step1_v3"
export SPARK_DRIVER_MEMORY="200g"
export SPARK_EXECUTOR_MEMORY="200g"

source "${V3_DIR}/jobs/pipelines/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

python -u "${V3_DIR}/src/pipelines/step1_inventors_matched_users_spark_v3.py" \
  --users-dir "${USERS_DIR}" \
  --inventors-dir "${PATENT_MATCHES}" \
  --out-dir "${STEP1_OUT}" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 1200 \
  --coalesce 200 \
  --tmpdir "${SCRATCH}"
