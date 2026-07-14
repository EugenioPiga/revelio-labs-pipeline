#!/bin/bash
#SBATCH --job-name=v2logicPreflight
#SBATCH --output=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/preflight/%x_%j.out
#SBATCH --error=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/preflight/%x_%j.err
#SBATCH --time=2:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general

set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
source "$ROOT/00_config/v3_env.sh"
export REVELIO_V3_OUTPUT_DIR="$HOME/revelio_labs/analyses_original_v2_logic_on_v3/output/preflight"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/preflight" "$REVELIO_V3_OUTPUT_DIR"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export VECLIB_MAXIMUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

echo "[INFO] $(date) | input=$REVELIO_V3_INPUT | output=$REVELIO_V3_OUTPUT_DIR"
Rscript "$ROOT/00_preflight/preflight_original_v2_logic_on_v3.R"
