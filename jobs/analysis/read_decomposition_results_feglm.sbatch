#!/bin/bash
#SBATCH --job-name=read_decomposition_all_feglm
#SBATCH --output=/home/epiga/revelio_labs/logs/decomposition_feglm_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/decomposition_feglm_%j.err
#SBATCH --partition=general
#SBATCH --mem=64G
#SBATCH --time=4:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

# ============================================
# Environment setup
# ============================================
echo "[INFO] Job started on $(hostname) at $(date)"
module purge
module load R/4.5.1

export R_LIBS_USER=~/R/library
mkdir -p /home/epiga/revelio_labs/logs

# ============================================
# Run decomposition analysis
# ============================================
Rscript /home/epiga/revelio_labs/read_decomposition_results_feglm.R

# ============================================
# End
# ============================================
echo "[INFO] Job finished at $(date)"
