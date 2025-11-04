#!/bin/bash
#SBATCH --job-name=check_date_format
#SBATCH --output=/home/epiga/revelio_labs/logs/check_date_format_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_date_format_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] Running tenure-normalized PPML with alpaca::feglm"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

R_SCRIPT="/home/epiga/revelio_labs/check_date_format.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/regressions"
mkdir -p "$LOG_DIR" "$OUT_DIR"

Rscript "$R_SCRIPT" > "$LOG_DIR/check_date_format_${SLURM_JOB_ID}.log" 2>&1

status=$?
if [ $status -eq 0 ]; then
    echo "[INFO] Job completed successfully at $(date)"
else
    echo "[ERROR] Job failed with exit code $status at $(date)"
fi
