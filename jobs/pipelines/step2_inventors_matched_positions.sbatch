#!/usr/bin/env bash
#SBATCH --job-name=step2_inv_pos
#SBATCH --output=logs/step2_inv_pos_%j.out
#SBATCH --error=logs/step2_inv_pos_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=56
#SBATCH --mem=250G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# Activate venv
source "$HOME/.venvs/revelio/bin/activate"

# Define inputs
STEP1_DIR="output/inventors_matched_users"
POSITIONS_DIR="academic_individual_position/*.parquet"

# Output directory (change if using scratch)
export OUTDIR="output/inventors_matched_positions"
export PARTITION_SIZE="256MB"

# Run job
python -u step2_inventors_matched_positions.py \
  --step1-dir "$STEP1_DIR" \
  --positions-dir "$POSITIONS_DIR" \
  --part-size "$PARTITION_SIZE"
