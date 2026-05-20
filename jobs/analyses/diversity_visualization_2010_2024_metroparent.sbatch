#!/bin/bash
#SBATCH --job-name=divViz_metroparent
#SBATCH --output=/home/epiga/revelio_labs/logs/diversity_visualizations/divViz_metroparent_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/diversity_visualizations/divViz_metroparent_%j.err
#SBATCH --time=36:00:00
#SBATCH --mem=350G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

# ===========================================
# Environment setup
# ===========================================
echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE}MB | JobID: $SLURM_JOB_ID"

module purge
module load R/4.5.1

export R_LIBS_USER=~/R/library

# Threading hygiene
export OMP_NUM_THREADS=$SLURM_CPUS_PER_TASK
export OPENBLAS_NUM_THREADS=$SLURM_CPUS_PER_TASK
export MKL_NUM_THREADS=$SLURM_CPUS_PER_TASK
export VECLIB_MAXIMUM_THREADS=$SLURM_CPUS_PER_TASK
export NUMEXPR_NUM_THREADS=$SLURM_CPUS_PER_TASK

# ===========================================
# Paths
# ===========================================
R_SCRIPT="/home/epiga/revelio_labs/diversity_visualization_2010_2024_metroparent.R"

LOG_DIR="/home/epiga/revelio_labs/logs/diversity_visualization"
OUT_DIR="/home/epiga/revelio_labs/output/diversity_visualization_2010_2024_metroparent"

mkdir -p "$LOG_DIR" "$OUT_DIR"

if [ ! -f "$R_SCRIPT" ]; then
  echo "[ERROR] R script not found: $R_SCRIPT"
  exit 1
fi

# ===========================================
# Run script
# ===========================================
echo "[INFO] Running R script: $R_SCRIPT"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Script failed with exit code $status"
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/divViz_metroparent_${SLURM_JOB_ID}.out"
  echo "        $LOG_DIR/divViz_metroparent_${SLURM_JOB_ID}.err"
  exit 1
else
  echo "[INFO] Script completed successfully."
fi

echo "[INFO] Job finished at $(date)"
exit 0

