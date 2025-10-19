#!/bin/bash
#SBATCH --job-name=check_shard_inventor_year
#SBATCH --output=/home/epiga/revelio_labs/logs/check_shard_inventor_year_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_shard_inventor_year_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

# =========================================================
# Environment setup
# =========================================================
echo "[INFO] Job started on $(hostname) at $(date)"

source ~/miniconda/bin/activate revelio

# =========================================================
# Run the check script
# =========================================================
python /home/epiga/revelio_labs/check_shard_inventor_year.py

echo "[INFO] Job finished at $(date)"
