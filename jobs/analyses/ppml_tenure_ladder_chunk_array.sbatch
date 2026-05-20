#!/bin/bash
#SBATCH --job-name=ppmlTenureChunk
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlTenureChunk_%A_%a.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_tenure_chunked/ppmlTenureChunk_%A_%a.err
#SBATCH --time=168:00:00
#SBATCH --mem=180G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --array=1-8%4
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE:-NA}MB | JobID: $SLURM_JOB_ID | ArrayTask: $SLURM_ARRAY_TASK_ID"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

R_SCRIPT="/home/epiga/revelio_labs/ppml_tenure_ladder_chunked.R"
LOG_DIR="/home/epiga/revelio_labs/logs/ppml_tenure_chunked"
OUT_DIR="/home/epiga/revelio_labs/output/ppml_tenure_ladder_extended"

mkdir -p "$LOG_DIR" "$OUT_DIR"

if [ ! -f "$R_SCRIPT" ]; then
  echo "[ERROR] R script not found: $R_SCRIPT"
  exit 1
fi

# Toggle immigrant outputs here if you want to postpone them.
export RUN_IMMIGRANT_SPLITS="0"
export RUN_IMMIGRANT_INTERACTIONS="0"

case "$SLURM_ARRAY_TASK_ID" in
  1)
    export WINDOW_TAGS="2010_2019"
    export CHUNK_MODE="core"
    ;;
  2)
    export WINDOW_TAGS="2010_2019"
    export CHUNK_MODE="ethnicity"
    ;;
  3)
    export WINDOW_TAGS="2010_2019"
    export CHUNK_MODE="heterogeneity"
    ;;
  4)
    export WINDOW_TAGS="2010_2019"
    export CHUNK_MODE="interactions"
    ;;
  5)
    export WINDOW_TAGS="2000_2019"
    export CHUNK_MODE="core"
    ;;
  6)
    export WINDOW_TAGS="2000_2019"
    export CHUNK_MODE="ethnicity"
    ;;
  7)
    export WINDOW_TAGS="2000_2019"
    export CHUNK_MODE="heterogeneity"
    ;;
  8)
    export WINDOW_TAGS="2000_2019"
    export CHUNK_MODE="interactions"
    ;;
  *)
    echo "[ERROR] Unknown array task id: $SLURM_ARRAY_TASK_ID"
    exit 1
    ;;
esac

echo "[INFO] Running chunk mode: $CHUNK_MODE | windows: $WINDOW_TAGS"
echo "[INFO] Immigrant splits: $RUN_IMMIGRANT_SPLITS | immigrant interactions: $RUN_IMMIGRANT_INTERACTIONS"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Script failed with exit code $status"
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/ppmlTenureChunk_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.out"
  echo "        $LOG_DIR/ppmlTenureChunk_${SLURM_ARRAY_JOB_ID}_${SLURM_ARRAY_TASK_ID}.err"
  exit 1
else
  echo "[INFO] Chunk completed successfully."
fi

echo "[INFO] Main output directory: $OUT_DIR"
echo "[INFO] Job finished at $(date)"
exit 0
