#!/bin/bash
#SBATCH --job-name=tenureV3_2000_2019
#SBATCH --output=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/tenure/%x_%j.out
#SBATCH --error=/home/epiga/revelio_labs/analyses_v3_reviewed/logs/tenure/%x_%j.err
#SBATCH --time=72:00:00
#SBATCH --mem=180G
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

# Core calculations only. Add metro_size,metro_prod,density only for the
# supplementary split package after the main profiles have completed.
export TENURE_WINDOW="2000_2019"
export TENURE_VARIANTS="base,seniority"
export TENURE_SPLITS="${TENURE_SPLITS:-career}"
export TENURE_FE_SET="${TENURE_FE_SET:-core}"
export TENURE_CLUSTER_MODE="${TENURE_CLUSTER_MODE:-two_way}"

mkdir -p "$ROOT/logs/tenure"
cd "$ROOT/logs/tenure"
Rscript "$ROOT/01_tenure/tenure_suite_v3.R"
