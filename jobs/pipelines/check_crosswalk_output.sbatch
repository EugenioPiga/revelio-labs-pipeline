#!/bin/bash
#SBATCH --job-name=inspect_crosswalk
#SBATCH --output=/home/epiga/revelio_labs/logs/check_crosswalk_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_crosswalk_%j.err
#SBATCH --time=01:00:00
#SBATCH --mem=64G
#SBATCH --cpus-per-task=4
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

# ==========================================
# Environment setup
# ==========================================
echo "[INFO] Job started on $(hostname) at $(date)"
source ~/.venvs/revelio/bin/activate || echo "[WARN] No venv found, using system Python"

module purge
module load python/3.12

SCRIPT="/home/epiga/revelio_labs/check_crosswalk_output.py"
LOG_DIR="/home/epiga/revelio_labs/logs"
OUT_DIR="/home/epiga/revelio_labs/output"

mkdir -p "$LOG_DIR" "$OUT_DIR"

# ==========================================
# Run inspection
# ==========================================
echo "[INFO] Starting inspection script..."
python "$SCRIPT" > "$LOG_DIR/cross_crosswalk_${SLURM_JOB_ID}.log" 2>&1
status=$?

if [ $status -ne 0 ]; then
    echo "[ERROR] Inspection script failed with exit code $status"
    echo "        Check log: $LOG_DIR/check_crosswalk_${SLURM_JOB_ID}.log"
else
    echo "[INFO] Inspection script completed successfully."
fi

echo "[INFO] Job finished at $(date)"
