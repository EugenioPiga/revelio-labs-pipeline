#!/bin/bash
#SBATCH --job-name=v3preflight
#SBATCH --output=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/preflight/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/preflight/%x_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=16G
#SBATCH --cpus-per-task=2
#SBATCH --partition=general

set -euo pipefail
ROOT="${REVELIO_ANALYSES_ROOT:-$HOME/revelio_labs/analyses_v3_reviewed}"
source "$ROOT/00_config/v3_env.sh"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
mkdir -p "$ROOT/logs/preflight"
cd "$ROOT/logs/preflight"
Rscript "$ROOT/00_preflight/preflight_v3.R"
