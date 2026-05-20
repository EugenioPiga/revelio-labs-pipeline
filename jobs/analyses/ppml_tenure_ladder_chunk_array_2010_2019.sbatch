#!/bin/bash
#SBATCH --job-name=ppmlMain2010
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlMain2010_%A_%a.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlMain2010_%A_%a.err
#SBATCH --time=168:00:00
#SBATCH --mem=180G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --array=1-5%5
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE:-NA}MB | JobID: $SLURM_JOB_ID | ArrayTask: $SLURM_ARRAY_TASK_ID"

set +u
source /etc/profile
set -u

module purge
module load R/4.5.1

if ! command -v Rscript >/dev/null 2>&1; then
  echo "[ERROR] Rscript not found after module load"
  exit 1
fi

echo "[INFO] Using Rscript at: $(which Rscript)"
echo "[INFO] Using R version: $(Rscript --version 2>&1 | head -n 1)"

export R_LIBS_USER=~/R/library
export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

R_SCRIPT="/home/epiga/revelio_labs/ppml_tenure_ladder.R"
LOG_DIR="/home/epiga/revelio_labs/logs/ppml_tenure_chunked"

mkdir -p "$LOG_DIR"

if [ ! -f "$R_SCRIPT" ]; then
  echo "[ERROR] R script not found: $R_SCRIPT"
  exit 1
fi

export WINDOW_TAGS="2010_2019"
export RUN_NO_USER_FE="0"
export RUN_COHORTS="0"

case "$SLURM_ARRAY_TASK_ID" in
  1)
    export CHUNK_MODE="career"
    export RUN_FE_DECOMP="0"
    ;;
  2)
    export CHUNK_MODE="metro_size"
    export RUN_FE_DECOMP="0"
    ;;
  3)
    export CHUNK_MODE="metro_prod"
    export RUN_FE_DECOMP="0"
    ;;
  4)
    export CHUNK_MODE="density"
    export RUN_FE_DECOMP="0"
    ;;
  5)
    export CHUNK_MODE="fe_decomp"
    export RUN_FE_DECOMP="1"
    ;;
  *)
    echo "[ERROR] Unknown array task id: $SLURM_ARRAY_TASK_ID"
    exit 1
    ;;
esac

echo "[INFO] Running chunk mode: $CHUNK_MODE | windows: $WINDOW_TAGS"
echo "[INFO] RUN_NO_USER_FE=$RUN_NO_USER_FE | RUN_COHORTS=$RUN_COHORTS | RUN_FE_DECOMP=$RUN_FE_DECOMP"

if ! Rscript "$R_SCRIPT"; then
  echo "[ERROR] Script failed."
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/ppmlMain2010_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"
  echo "        $LOG_DIR/ppmlMain2010_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.err"
  exit 1
fi

echo "[INFO] Chunk completed successfully."
echo "[INFO] Job finished at $(date)"
exit 0
