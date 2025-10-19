#!/usr/bin/env bash
#SBATCH --job-name=desc_stats_country
#SBATCH --output=logs/desc_stats_country_%j.out
#SBATCH --error=logs/desc_stats_country_%j.err
#SBATCH --time=04:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=64G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=END,FAIL

set -euo pipefail
mkdir -p logs

# 1. Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# 2. Run the script
python -u descriptive_statistics_country.py \
  --file /labs/khanna/linkedin_202507/processed/inventor_position_with_education \
  --start_year 2010
