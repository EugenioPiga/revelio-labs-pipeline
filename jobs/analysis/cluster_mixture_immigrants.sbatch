#!/bin/bash
#SBATCH --job-name=cluster_decomp
#SBATCH --output=/home/epiga/revelio_labs/logs/cluster_decomp_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/cluster_decomp_%j.err
#SBATCH --time=12:00:00
#SBATCH --mem=300G
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
R_SCRIPT="/home/epiga/revelio_labs/cluster_mixture_immigrants.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/descriptives_immigrants/cluster_decomposition"

# Ensure directories exist
mkdir -p "$LOG_DIR" "$OUT_DIR"

echo "[INFO] Using Rscript: $R_SCRIPT"
echo "[INFO] Output directory: $OUT_DIR"
echo "[INFO] Logs in: $LOG_DIR"

###############################################
# RUN CLUSTER (MIXTURE) DECOMPOSITION
###############################################

echo "[INFO] Starting cluster (mixture) decomposition:"
echo "       - 3 immigrant definitions"
echo "       - 3 geographies (state/city/metro)"
echo "       - Computes cluster-year means (ybar, yI, yN, Lambda, beta)"
echo "       - Produces change decomposition: d_ybar = d_eps + beta_bar*d_Lambda + Lambda_bar*d_beta"
echo "       => outputs per-combo panels + a master summary table"

Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
    echo "[FATAL] Cluster decomposition script FAILED with exit code $status."
    echo "[FATAL] Check logs:"
    echo "        $LOG_DIR/cluster_decomp_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/cluster_decomp_${SLURM_JOB_ID}.err"
    exit $status
else
    echo "[INFO] Cluster decomposition completed successfully."
fi

###############################################
# SUCCESS SUMMARY
###############################################

echo "[INFO] ========================================"
echo "[INFO] Cluster decomposition finished successfully at $(date)"
echo "[INFO] Job ID: $SLURM_JOB_ID"
echo "[INFO] Output located in: $OUT_DIR"
echo "[INFO] ========================================"

exit 0
