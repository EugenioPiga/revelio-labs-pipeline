#!/bin/bash
#SBATCH --job-name=v2logicMoveExt
#SBATCH --output=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/mobility/churn_mobility_extended/%x_%j.out
#SBATCH --error=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/mobility/churn_mobility_extended/%x_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=400G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general

set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
source "$ROOT/00_config/v3_env.sh"
export REVELIO_V3_OUTPUT_DIR="$HOME/revelio_labs/analyses_original_v2_logic_on_v3/output/mobility/churn_mobility_extended"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/mobility/churn_mobility_extended" "$REVELIO_V3_OUTPUT_DIR"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export VECLIB_MAXIMUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

echo "[INFO] $(date) | input=$REVELIO_V3_INPUT | output=$REVELIO_V3_OUTPUT_DIR"
Rscript "$ROOT/04_mobility/_components/churn_mobility_extended.R"
