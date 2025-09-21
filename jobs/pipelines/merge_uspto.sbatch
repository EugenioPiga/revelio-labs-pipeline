#!/bin/bash
#SBATCH --job-name=merge_uspto
#SBATCH --output=logs/merge_uspto_%j.out
#SBATCH --error=logs/merge_uspto_%j.err
#SBATCH --time=2-00:00:00          # 2 days
#SBATCH --partition=general
#SBATCH --nodes=1
#SBATCH --cpus-per-task=16
#SBATCH --mem=128G
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL

module load python/3.10
source /home/epiga/.venvs/revelio/bin/activate

echo "Starting merge job at $(date)"

python /home/epiga/revelio_labs/merge_uspto.py

echo "Job finished at $(date)"
