#!/bin/bash
#SBATCH --job-name=v2logicMain00
#SBATCH --output=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/tenure_no_seniority/2000_2019/%x_%j.out
#SBATCH --error=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/tenure_no_seniority/2000_2019/%x_%j.err
#SBATCH --time=168:00:00
#SBATCH --mem=180G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --array=1-5%2

set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
source "$ROOT/00_config/v3_env.sh"
export REVELIO_V3_OUTPUT_DIR="$HOME/revelio_labs/analyses_original_v2_logic_on_v3/output/tenure_no_seniority/2000_2019"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/tenure_no_seniority/2000_2019" "$REVELIO_V3_OUTPUT_DIR"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export VECLIB_MAXIMUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
# Original task decomposition; only concurrency is capped at 2.
export WINDOW_TAGS="2000_2019"
export RUN_NO_USER_FE="0"
export RUN_COHORTS="0"
case "$SLURM_ARRAY_TASK_ID" in
  1) export CHUNK_MODE="career"; export RUN_FE_DECOMP="0" ;;
  2) export CHUNK_MODE="metro_size"; export RUN_FE_DECOMP="0" ;;
  3) export CHUNK_MODE="metro_prod"; export RUN_FE_DECOMP="0" ;;
  4) export CHUNK_MODE="density"; export RUN_FE_DECOMP="0" ;;
  5) export CHUNK_MODE="fe_decomp"; export RUN_FE_DECOMP="1" ;;
  *) echo "Unknown array task"; exit 1 ;;
esac
echo "[INFO] $(date) | input=$REVELIO_V3_INPUT | output=$REVELIO_V3_OUTPUT_DIR"
Rscript "$ROOT/01_tenure/no_seniority/ppml_tenure_ladder.R"
