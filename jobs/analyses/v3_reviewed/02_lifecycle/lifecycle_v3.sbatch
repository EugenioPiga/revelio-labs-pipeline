#!/bin/bash
#SBATCH --job-name=lifecycleV3
#SBATCH --output=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/lifecycle/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/lifecycle/%x_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=160G
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
export LIFECYCLE_YEAR_MIN="${LIFECYCLE_YEAR_MIN:-2010}"
export LIFECYCLE_YEAR_MAX="${LIFECYCLE_YEAR_MAX:-2019}"
mkdir -p "$ROOT/logs/lifecycle"
cd "$ROOT/logs/lifecycle"
Rscript "$ROOT/02_lifecycle/lifecycle_v3.R"
