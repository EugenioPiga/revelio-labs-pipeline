#!/usr/bin/env bash
#SBATCH --job-name=tabs_stats
#SBATCH --output=logs/tabs_stats_%j.out
#SBATCH --error=logs/tabs_stats_%j.err
#SBATCH --time=24:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]

set -euo pipefail
mkdir -p logs
source "$HOME/.venvs/revelio/bin/activate"

# Run from repo root, write to ./output
python -u tabs_stats.py --base . --out output/tabs_stats
