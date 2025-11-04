#!/bin/bash
#SBATCH --job-name=event_ppml_feglm_tenure_window
#SBATCH --partition=general
#SBATCH --mem=450G
#SBATCH --time=20:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --output=/home/epiga/revelio_labs/logs/event_ppml_feglm_tenure_window_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/event_ppml_feglm_tenure_window_%j.err

################################################################################
# HOW TO USE THIS SCRIPT
#
# Run one event window (e.g., ±2 years) manually:
#       WINDOW=2 sbatch event_tenure_ppml_feglm_window.sbatch
#
# Run multiple windows in parallel (recommended):
#       for W in 1 2 3 7; do sbatch --export=WINDOW=$W event_tenure_ppml_feglm_window.sbatch; done
#
# Each submission runs a separate job for ±1, ±2, ±3, ±7, producing outputs:
#   coefficients_city_tenure_t1.csv, coefficients_city_tenure_t2.csv, etc.
#
# Notes:
# - You do NOT need to modify this file each time.
# - The WINDOW variable is passed to the R script as the event-study sample size.
# - Logs and outputs are stored separately in tenure-specific folders.
################################################################################

# ============================
# Environment Setup
# ============================
echo "[INFO] Starting Event Study with TENURE controls (alpaca::feglm)"
echo "[INFO] Node: $(hostname)"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] CPUs: ${SLURM_CPUS_PER_TASK}"
echo "[INFO] Memory: ${SLURM_MEM_PER_NODE:-${SLURM_MEM_PER_CPU}}"
echo "[INFO] Start time: $(date)"
echo "-------------------------------------------"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

# ============================
# Parameters
# ============================
# WINDOW can be provided externally using --export=WINDOW=<num>
# If not provided, defaults to 2 (±2 window)
WINDOW=${WINDOW:-2}

R_SCRIPT="/home/epiga/revelio_labs/event_tenure_ppml_feglm_window.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/eventstudy_tenure_ppml_feglm"

mkdir -p "$LOG_DIR" "$OUT_DIR"

# ============================
# Run Event Study
# ============================
echo "[INFO] Running script: $R_SCRIPT"
echo "[INFO] Using ±${WINDOW}-year sample"

Rscript "$R_SCRIPT" "${WINDOW}"
status=$?

if [ $status -ne 0 ]; then
    echo "[ERROR] Event study (tenure) failed with exit code $status"
    echo "[ERROR] Check logs at:"
    echo "        $LOG_DIR/event_ppml_feglm_tenure_window_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/event_ppml_feglm_tenure_window_${SLURM_JOB_ID}.err"
    exit 1
else
    echo "[INFO] ✅ Event study with TENURE controls (±${WINDOW}) completed successfully"
fi

echo "[INFO] End time: $(date)"
exit 0
