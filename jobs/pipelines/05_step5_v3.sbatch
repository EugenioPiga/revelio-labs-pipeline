#!/usr/bin/env bash
#SBATCH --job-name=step5_merge_v3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/step5_v3_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/step5_v3_%j.err
#SBATCH --time=70:00:00
#SBATCH --cpus-per-task=32
#SBATCH --mem=300G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/innovation_linkedin"
export JOB_LABEL="step5_v3"
export SPARK_DRIVER_MEMORY="240g"
export SPARK_EXECUTOR_MEMORY="240g"

source "${V3_DIR}/jobs/pipelines/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

python -u "${V3_DIR}/src/pipelines/step5_merge_inventor_year_shards_v3.py" \
  --input-shards-dir "${STEP4_SHARDS_OUT}" \
  --out-dir "${STEP5_MERGED_OUT}" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 3000 \
  --out-partitions 300 \
  --tmpdir "${SCRATCH}"
