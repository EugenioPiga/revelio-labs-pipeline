#!/bin/bash
#SBATCH --job-name=v2logicMetro
#SBATCH --output=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/metro_parent_productivity/%x_%j.out
#SBATCH --error=$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/metro_parent_productivity/%x_%j.err
#SBATCH --time=48:00:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general

set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
source "$ROOT/00_config/v3_env.sh"
export REVELIO_V3_OUTPUT_DIR="$HOME/revelio_labs/analyses_original_v2_logic_on_v3/output/metro_parent_productivity"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$HOME/revelio_labs/analyses_original_v2_logic_on_v3/logs/metro_parent_productivity" "$REVELIO_V3_OUTPUT_DIR"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export VECLIB_MAXIMUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

echo "[INFO] $(date) | input=$REVELIO_V3_INPUT | output=$REVELIO_V3_OUTPUT_DIR"
Rscript "$ROOT/05_metro_parent_productivity/metro_parent_productivity_v3.R"
