#!/bin/bash
#SBATCH --job-name=mobilityV3
#SBATCH --output=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/mobility/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/mobility/%x_%j.err
#SBATCH --time=30:00:00
#SBATCH --mem=220G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail
ROOT="${REVELIO_ANALYSES_ROOT:-$HOME/revelio_labs/analyses_v3_reviewed}"
source "$ROOT/00_config/v3_env.sh"
module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MOBILITY_YEAR_MIN="${MOBILITY_YEAR_MIN:-2010}"
export MOBILITY_YEAR_MAX="${MOBILITY_YEAR_MAX:-2019}"
export MOBILITY_WRITE_PANEL="${MOBILITY_WRITE_PANEL:-0}"
export MOBILITY_RUN_EVENT_PROFILE="${MOBILITY_RUN_EVENT_PROFILE:-0}"
export MOBILITY_RUN_FE_LPM="${MOBILITY_RUN_FE_LPM:-1}"
mkdir -p "$ROOT/logs/mobility"
cd "$ROOT/logs/mobility"
Rscript "$ROOT/04_mobility/mobility_v3_reviewed.R"
