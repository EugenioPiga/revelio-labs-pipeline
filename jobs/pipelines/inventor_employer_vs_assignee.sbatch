#!/usr/bin/env bash
#SBATCH --job-name=inv_assg_match
#SBATCH --output=logs/inventor_assignee_match_%j.out
#SBATCH --error=logs/inventor_assignee_match_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# Activate your virtual environment
source "$HOME/.venvs/revelio/bin/activate"

# Run the analysis script
python -u inventor_employer_vs_assignee.py
