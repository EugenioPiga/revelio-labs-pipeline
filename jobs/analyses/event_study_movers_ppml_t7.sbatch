#!/bin/bash
#SBATCH --job-name=event_ppml_sharded_t7
#SBATCH --partition=general
#SBATCH --mem=400G
#SBATCH --time=3-00:00:00
#SBATCH --array=0-19
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --output=logs/event_ppml_t7_%A_%a.out
#SBATCH --error=logs/event_ppml_t7_%A_%a.err

# --- Load R module ---
module load R/4.5.1

# --- Print environment info ---
echo "[INFO] Job ID: ${SLURM_JOB_ID}, Array Task: ${SLURM_ARRAY_TASK_ID}"
echo "[INFO] Node: $(hostname)"
echo "[INFO] Starting at: $(date)"

# --- Run R script ---
Rscript /home/epiga/revelio_labs/event_study_movers_ppml_t7.R $SLURM_ARRAY_TASK_ID

echo "[INFO] Finished at: $(date)"

