#!/bin/bash
#SBATCH --job-name=test_inv_year_fix
#SBATCH --output=/home/epiga/revelio_labs/logs/test_inv_year_fix_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/test_inv_year_fix_%j.err
#SBATCH --time=01:30:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

echo "[INFO] Job started on $(hostname) at $(date)"

# =========================================================
# Environment
# =========================================================
source ~/miniconda/bin/activate revelio
echo "[INFO] Using Python from: $(which python)"
python --version

# =========================================================
# Run the modified merge script for one shard only
# =========================================================
SHARD_INDEX=0
python /home/epiga/revelio_labs/inventor_year_merge.py $SHARD_INDEX

echo "[INFO] Test job finished at $(date)"
