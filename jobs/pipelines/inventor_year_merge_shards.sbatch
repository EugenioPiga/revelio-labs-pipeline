#!/bin/bash
#SBATCH --job-name=merge_inv_year
#SBATCH --output=logs/merge_inv_year_%j.out
#SBATCH --error=logs/merge_inv_year_%j.err
#SBATCH --time=01:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

source ~/miniconda/bin/activate revelio

echo "[INFO] Starting merge job on $(hostname) at $(date)"
python /home/epiga/revelio_labs/inventor_year_merge_shards.py
echo "[INFO] Finished job at $(date)"
