#!/bin/bash
#SBATCH --partition=general
#SBATCH --mem=250G
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=8
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --output=/home/epiga/revelio_labs/logs/event_tenure_symmetry_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/event_tenure_symmetry_%j.err

################################################################################
# HOW TO USE THIS SCRIPT
#
# Submit with:
#       sbatch event_tenure_symmetry_profiles.sbatch
#
# This job performs two complementary symmetry analyses:
#   (1) Card–Heining–Kline (2013)-style mean n_patents profiles around job moves
#       by origin/destination coworker-productivity quartiles (±8-year window).
#   (2) Scatter test of Δ n_patents (post–pre) vs Δ ψ (dest–origin mean innovation)
#       across firm, city, and firm-within-city movers.
#
# Output directory:
#   /home/epiga/revelio_labs/output/eventstudy_tenure_symmetry/
#     symmetry_origin_q{1–4}_{mover}.png        # Quartile-based profiles
#     symmetry_scatter_{mover}.png               # Δ n_patents vs Δ ψ scatterplots
#
# Logs:
#   /home/epiga/revelio_labs/logs/event_tenure_symmetry_<JOBID>.out / .err
################################################################################

# ============================
# Environment Setup
# ============================
echo "[INFO] Starting SYMMETRY analyses (quartile profiles + Δ scatter)"
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
R_SCRIPT="/home/epiga/revelio_labs/event_tenure_symmetry_profiles2.R"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output/eventstudy_tenure_symmetry"

mkdir -p "$LOG_DIR" "$OUT_DIR"

# ============================
# Run Analyses
# ============================
echo "[INFO] Running script: $R_SCRIPT"
echo "[INFO] Using fixed ±8-year event window and ≥2-year tenure each side"
echo "[INFO] Will generate both quartile-profile and Δψ scatter figures"

Rscript "$R_SCRIPT"
status=$?

if [ $status -ne 0 ]; then
    echo "[ERROR] Symmetry analyses failed with exit code $status"
    echo "[ERROR] Check logs at:"
    echo "        $LOG_DIR/event_tenure_symmetry_${SLURM_JOB_ID}.out"
    echo "        $LOG_DIR/event_tenure_symmetry_${SLURM_JOB_ID}.err"
    exit 1
else
    echo "[INFO] ✅ Symmetry analyses (quartile + scatter) completed successfully"
fi

echo "[INFO] End time: $(date)"
exit 0























