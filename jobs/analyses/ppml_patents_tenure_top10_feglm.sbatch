#!/bin/bash
#SBATCH --job-name=ppml_tenure_top10_feglm
#SBATCH --output=/home/epiga/revelio_labs/logs/ppml_tenure_top10_feglm_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/ppml_tenure_top10_feglm_%j.err
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
echo "[INFO] Running on $SLURM_CPUS_PER_TASK CPUs with $SLURM_MEM_PER_NODE memory"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library

R_SCRIPT="/home/epiga/revelio_labs/ppml_patents_tenure_top10_feglm.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/regressions"

# Ensure directories exist
mkdir -p "$LOG_DIR" "$OUT_DIR"

# ===========================================
# Run PPML (Top 10% inventors, alpaca::feglm)
# ===========================================
echo "[INFO] Starting PPML model for Top 10% inventors (alpaca::feglm)..."
echo "[INFO] Script: $R_SCRIPT"

Rscript "$R_SCRIPT"
status_top10=$?

if [ $status_top10 -ne 0 ]; then
    echo "[ERROR] PPML Top10 model failed with exit code $status_top10."
    echo "[ERROR] Check logs at:"
    echo "        $LOG_DIR/ppml_tenure_top10_feglm_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/ppml_tenure_top10_feglm_${SLURM_JOB_ID}.err"
    echo "[FATAL] Job failed. Exiting..."
    exit 1
else
    echo "[INFO] PPML Top10 model completed successfully."
fi

# ===========================================
# Success summary
# ===========================================
echo "[INFO] PPML Top10 job finished successfully at $(date)"
exit 0
