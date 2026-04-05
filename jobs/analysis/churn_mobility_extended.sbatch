#!/bin/bash
#SBATCH --job-name=moveExt
#SBATCH --output=/home/epiga/revelio_labs/logs/mobility/moveExt_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/mobility/moveExt_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=350G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE}MB | JobID: $SLURM_JOB_ID"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export OPENBLAS_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK
export VECLIB_MAXIMUM_THREADS=$SLURM_CPUS_PER_TASK
export NUMEXPR_NUM_THREADS=$SLURM_CPUS_PER_TASK

R_SCRIPT="/home/epiga/revelio_labs/churn_mobility_extended.R"
LOG_DIR="/home/epiga/revelio_labs/logs/mobility"
OUT_DIR="/home/epiga/revelio_labs/output/mobility_churn_extended"

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
  echo "        $LOG_DIR/moveExt_${SLURM_JOB_ID}.out"
  echo "        $LOG_DIR/moveExt_${SLURM_JOB_ID}.err"
  exit 1
else
  echo "[INFO] Script completed successfully."
fi

echo "[INFO] Job finished at $(date)"
exit 0
