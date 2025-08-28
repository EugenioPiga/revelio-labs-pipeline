#!/usr/bin/env bash
#SBATCH --job-name=patent_assignee_analysis
#SBATCH --partition=general
#SBATCH --time=24:00:00             # adjust as needed
#SBATCH --cpus-per-task=36          # balanced CPU count
#SBATCH --mem=180G                  # plenty of RAM
#SBATCH --output=logs/patent_assignee_analysis_%j.out
#SBATCH --error=logs/patent_assignee_analysis_%j.err
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail

# Activate your Python environment
source "$HOME/.venvs/revelio/bin/activate"
export PYTHONUNBUFFERED=1

# Scratch / temp dir (use node-local scratch if available)
export TMPDIR="${SLURM_TMPDIR:-/tmp}"
export DASK_TEMPORARY_DIRECTORY="$TMPDIR/dask-tmp"
mkdir -p "$DASK_TEMPORARY_DIRECTORY"

# Run the analysis
python -u patent_assignee_analysis.py
