#!/bin/bash
#SBATCH --job-name=divTI10y_PARENT_countrySens
#SBATCH --output=/home/epiga/revelio_labs/logs/timeinvariant_country_sensitivity/divTI10y_PARENT_countrySens_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/timeinvariant_country_sensitivity/divTI10y_PARENT_countrySens_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=500G
#SBATCH --cpus-per-task=16
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

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
R_SCRIPT="/home/epiga/revelio_labs/diversity_timeinvariant10y_PARENT_country_sensitivity.R"

LOG_DIR="/home/epiga/revelio_labs/logs/timeinvariant_country_sensitivity"
OUT_DIR="/home/epiga/revelio_labs/output/diversity_timeinvariant10y_PARENT_country_sensitivity"

mkdir -p "$LOG_DIR" "$OUT_DIR"

# ===========================================
# Run script
# ===========================================
echo "[INFO] Running R script: $R_SCRIPT"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Script failed with exit code $status"
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/divTI10y_PARENT_countrySens_${SLURM_JOB_ID}.out"
  echo "        $LOG_DIR/divTI10y_PARENT_countrySens_${SLURM_JOB_ID}.err"
  exit 1
else
  echo "[INFO] Script completed successfully."
fi

echo "[INFO] Job finished at $(date)"
exit 0
