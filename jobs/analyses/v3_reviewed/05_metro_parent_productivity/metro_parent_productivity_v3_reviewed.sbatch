#!/bin/bash
#SBATCH --job-name=metroProdV3
#SBATCH --output=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/metro_parent_productivity/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/metro_parent_productivity/%x_%j.err
#SBATCH --time=48:00:00
#SBATCH --mem=260G
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
export METRO_YEAR_MIN="${METRO_YEAR_MIN:-2010}"
export METRO_YEAR_MAX="${METRO_YEAR_MAX:-2019}"
# Dynamic PPML with lagged patenting is a sensitivity analysis because of
# dynamic fixed-effect bias; leave it off for the main run.
export METRO_RUN_DYNAMIC="${METRO_RUN_DYNAMIC:-0}"
mkdir -p "$ROOT/logs/metro_parent_productivity"
cd "$ROOT/logs/metro_parent_productivity"
Rscript "$ROOT/05_metro_parent_productivity/metro_parent_productivity_v3_reviewed.R"
