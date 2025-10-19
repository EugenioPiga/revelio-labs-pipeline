#!/bin/bash
#SBATCH --job-name=check_dup_pos_years
#SBATCH --output=/home/epiga/revelio_labs/logs/check_duplicate_positions_and_years_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_duplicate_positions_and_years_%j.err
#SBATCH --time=10:00:00
#SBATCH --mem=120G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

# =======================================
# Environment setup
# =======================================
echo "[INFO] Job started on $(hostname) at $(date)"

module purge
module load python/3.10.12  # or whichever version matches your Miniconda env

# Activate Miniconda environment
source ~/miniconda/bin/activate revelio

# =======================================
# Run diagnostic
# =======================================
echo "[INFO] Running duplicate diagnostics..."
python /home/epiga/revelio_labs/check_duplicates_positions_years.py

echo "[INFO] Job finished at $(date)"
