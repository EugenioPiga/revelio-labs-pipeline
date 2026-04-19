#!/bin/bash
#SBATCH --job-name=ppmlTenureMerge
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlTenureMerge_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlTenureMerge_%j.err
#SBATCH --time=12:00:00
#SBATCH --mem=32G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Merge job started on $(hostname) at $(date)"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

R_SCRIPT="/home/epiga/revelio_labs/ppml_tenure_ladder_merge_chunks.R"
LOG_DIR="/home/epiga/revelio_labs/logs/ppml_tenure_chunked"
OUT_DIR="/home/epiga/revelio_labs/output/ppml_tenure_ladder_extended"

mkdir -p "$LOG_DIR" "$OUT_DIR"

if [ ! -f "$R_SCRIPT" ]; then
  echo "[ERROR] Merge R script not found: $R_SCRIPT"
  exit 1
fi

echo "[INFO] Running merge script: $R_SCRIPT"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Merge failed with exit code $status"
  exit 1
fi

echo "[INFO] Merge completed successfully."
echo "[INFO] Job finished at $(date)"
exit 0
