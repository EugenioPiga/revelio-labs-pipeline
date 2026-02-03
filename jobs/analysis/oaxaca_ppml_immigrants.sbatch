#!/bin/bash
#SBATCH --job-name=oaxaca_immigrants
#SBATCH --output=/home/epiga/revelio_labs/logs/oaxaca_immigrants_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/oaxaca_immigrants_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=400G
#SBATCH --cpus-per-task=16
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
R_SCRIPT="/home/epiga/revelio_labs/oaxaca_ppml_immigrants.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/regressions/oaxaca_immigrants"

# Ensure directories exist
mkdir -p "$LOG_DIR" "$OUT_DIR"

echo "[INFO] Using Rscript: $R_SCRIPT"
echo "[INFO] Output directory: $OUT_DIR"
echo "[INFO] Logs in: $LOG_DIR"

###############################################
# RUN OAXACA SCRIPT
###############################################

echo "[INFO] Starting Oaxaca decomposition grid:"
echo "       - 3 immigrant definitions"
echo "       - 9 FE/geo specs (3 geos × 3 firm-FE options)"
echo "       - Loads PPML models from: /home/epiga/revelio_labs/output/regressions/ppml_immigrants"
echo "       => total up to 27 Oaxaca runs"

Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
    echo "[FATAL] Oaxaca script FAILED with exit code $status."
    echo "[FATAL] Check logs:"
    echo "        $LOG_DIR/oaxaca_immigrants_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/oaxaca_immigrants_${SLURM_JOB_ID}.err"
    exit $status
else
    echo "[INFO] Oaxaca decomposition completed successfully."
fi

###############################################
# SUCCESS SUMMARY
###############################################

echo "[INFO] ========================================"
echo "[INFO] Oaxaca job finished successfully at $(date)"
echo "[INFO] Job ID: $SLURM_JOB_ID"
echo "[INFO] Output located in: $OUT_DIR"
echo "[INFO] ========================================"

exit 0
