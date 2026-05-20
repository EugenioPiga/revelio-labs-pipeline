#!/usr/bin/env bash
#SBATCH --job-name=desc_stats_imm
#SBATCH --output=logs/desc_stats_imm_%j.out
#SBATCH --error=logs/desc_stats_imm_%j.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=END,FAIL

set -euo pipefail
mkdir -p logs

# 1. Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# 2. Run immigrant descriptive stats
python -u descriptive_statistics_immigrants.py \
  --file /labs/khanna/linkedin_202507/processed/inventor_position_with_education \
  --start_year 2010
