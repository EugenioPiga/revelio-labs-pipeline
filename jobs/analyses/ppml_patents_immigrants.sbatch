#!/bin/bash
#SBATCH --job-name=ppml_immigrants
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_immigrants_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_immigrants_%j.err
#SBATCH --time=48:00:00
#SBATCH --mem=600G
#SBATCH --cpus-per-task=32
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

###############################################
# ENVIRONMENT SETUP
###############################################

echo "[INFO] ========================================"
echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] SLURM job ID: $SLURM_JOB_ID"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK"
echo "[INFO] Memory: $SLURM_MEM_PER_NODE"
echo "[INFO] ========================================"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

# ----------------------------
# Paths (update script name if needed)
# ----------------------------
R_SCRIPT="/home/epiga/revelio_labs/ppml_patents_immigrants.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/regressions/ppml_immigrants"

# Ensure directories exist
mkdir -p "$LOG_DIR" "$OUT_DIR"

echo "[INFO] Using Rscript: $R_SCRIPT"
echo "[INFO] Output directory: $OUT_DIR"
echo "[INFO] Logs in: $LOG_DIR"

###############################################
# RUN IMMIGRANT PPML SCRIPT
###############################################

echo "[INFO] Starting immigrant PPML grid:"
echo "       - 2 immigrant definitions (job-first-nonUS, degree-first-nonUS)"
echo "       - 2 groups per definition (immigrant vs non-immigrant)"
echo "       - 9 FE/geo specs per group (3 geos × 3 firm-FE options)"
echo "       => total up to 36 PPML runs"

Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
    echo "[FATAL] Immigrant PPML unified script FAILED with exit code $status."
    echo "[FATAL] Check logs:"
    echo "        $LOG_DIR/ppml_immigrants_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/ppml_immigrants_${SLURM_JOB_ID}.err"
    exit $status
else
    echo "[INFO] All immigrant PPML specifications completed successfully."
fi

###############################################
# SUCCESS SUMMARY
###############################################

echo "[INFO] ========================================"
echo "[INFO] Immigrant PPML grid finished successfully at $(date)"
echo "[INFO] Job ID: $SLURM_JOB_ID"
echo "[INFO] Output located in: $OUT_DIR"
echo "[INFO] ========================================"

exit 0
