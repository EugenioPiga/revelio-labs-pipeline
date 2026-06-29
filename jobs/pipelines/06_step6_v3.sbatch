#!/usr/bin/env bash
#SBATCH --job-name=step6_colleagues_v3
#SBATCH --output=/home/epiga/innovation_linkedin/logs/step6_v3_%j.out
#SBATCH --error=/home/epiga/innovation_linkedin/logs/step6_v3_%j.err
#SBATCH --time=7-00:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=450G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

V3_DIR="/home/epiga/innovation_linkedin"
export JOB_LABEL="step6_v3"
export SPARK_DRIVER_MEMORY="360g"
export SPARK_EXECUTOR_MEMORY="360g"

source "${V3_DIR}/jobs/pipelines/v3_runtime_env.sh"
source "${V3_DIR}/PATHS_TO_EDIT.env"

python -u "${V3_DIR}/src/pipelines/step6_colleague_citation_collaboration_measures_v3.py" \
  --input-panel "${STEP5_MERGED_OUT}" \
  --patent-matches "${PATENT_MATCHES}" \
  --citations-tsv "/labs/khanna/USPTO_202507/g_us_patent_citation.tsv" \
  --out-dir "${STEP6_OUT}" \
  --employment-link-mode strict_current \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 4000 \
  --out-partitions 300 \
  --tmpdir "${SCRATCH}"
