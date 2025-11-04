#!/bin/bash
#SBATCH --job-name=event_ppml_feglm_baseline
#SBATCH --partition=general
#SBATCH --mem=450G
#SBATCH --time=2-00:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --output=/home/epiga/revelio_labs/logs/event_ppml_feglm_baseline_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/event_ppml_feglm_baseline_%j.err

# ============================
# Environment Setup
# ============================
echo "[INFO] Starting Event Study Baseline (alpaca::feglm)"
echo "[INFO] Node: $(hostname)"
echo "[INFO] Job ID: ${SLURM_JOB_ID}"
echo "[INFO] Using ${SLURM_CPUS_PER_TASK} CPUs and ${SLURM_MEM_PER_NODE:-${SLURM_MEM_PER_CPU}} memory"
echo "[INFO] Start time: $(date)"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

R_SCRIPT="/home/epiga/revelio_labs/event_study_movers_ppml_t2_feglm.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/eventstudy_ppml_feglm"

mkdir -p "$LOG_DIR" "$OUT_DIR"

# ============================
# Run Event Study
# ============================
echo "[INFO] Running script: $R_SCRIPT"
Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
    echo "[ERROR] Event study baseline failed with exit code $status"
    echo "[ERROR] Check logs at:"
    echo "        $LOG_DIR/event_ppml_feglm_baseline_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/event_ppml_feglm_baseline_${SLURM_JOB_ID}.err"
    exit 1
else
    echo "[INFO] ✅ Event study baseline completed successfully"
fi

echo "[INFO] End time: $(date)"
exit 0
