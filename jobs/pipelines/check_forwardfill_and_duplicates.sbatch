#!/bin/bash
#SBATCH --job-name=check_forwardfill_and_duplicates
#SBATCH --output=/home/epiga/revelio_labs/logs/check_forwardfill_and_duplicates_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_forwardfill_and_duplicates_%j.err
#SBATCH --time=00:20:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

echo "[INFO] Job started on $(hostname) at $(date)"

# =========================================================
# Activate environment
# =========================================================
source ~/miniconda/bin/activate revelio

echo "[INFO] Using Python from: $(which python)"
python --version

# =========================================================
# Run the diagnostic script
# =========================================================
python /home/epiga/revelio_labs/check_forwardfill_and_duplicates.py

echo "[INFO] Job finished at $(date)"
