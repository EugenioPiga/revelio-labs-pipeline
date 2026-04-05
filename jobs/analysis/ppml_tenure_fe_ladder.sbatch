#!/bin/bash
#SBATCH --job-name=ppmlTenure
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_tenure/ppmlTenure_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_tenure/ppmlTenure_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=180G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE:-NA}MB | JobID: $SLURM_JOB_ID"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

# Keep BLAS/OpenMP in sync with the script's single-thread design
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

R_SCRIPT="/home/epiga/revelio_labs/ppml_tenure_fe_ladder.R"
LOG_DIR="/home/epiga/revelio_labs/logs/ppml_tenure"
OUT_DIR="/home/epiga/revelio_labs/output/ppml_tenure_fe_ladder"

mkdir -p "$LOG_DIR" "$OUT_DIR"

if [ ! -f "$R_SCRIPT" ]; then
  echo "[ERROR] R script not found: $R_SCRIPT"
  exit 1
fi

echo "[INFO] Running R script: $R_SCRIPT"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Script failed with exit code $status"
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/ppmlTenure_${SLURM_JOB_ID}.out"
  echo "        $LOG_DIR/ppmlTenure_${SLURM_JOB_ID}.err"
  exit 1
else
  echo "[INFO] Script completed successfully."
fi

echo "[INFO] Main output directory: $OUT_DIR"
echo "[INFO] Job finished at $(date)"
exit 0
