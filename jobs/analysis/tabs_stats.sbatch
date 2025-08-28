#!/usr/bin/env bash
#SBATCH --job-name=tabs_stats_ex
#SBATCH --output=logs/tabs_stats_%j.out
#SBATCH --error=logs/tabs_stats_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

source "$HOME/.venvs/revelio/bin/activate"

# Run only one dataset at a time (example: company_ref)
python -u tabs_stats.py \
    --base . \
    --out report/tabs_company_ref.parquet \
    --datasets academic_company_ref
