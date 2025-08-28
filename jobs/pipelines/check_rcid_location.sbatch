#!/usr/bin/env bash
#SBATCH --job-name=rcid_location_check
#SBATCH --partition=general
#SBATCH --time=06:00:00
#SBATCH --cpus-per-task=16
#SBATCH --mem=64G
#SBATCH --output=logs/rcid_location_%j.out
#SBATCH --error=logs/rcid_location_%j.err
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=END,FAIL

set -euo pipefail

WORKDIR="$HOME/revelio_labs"
VENV_ACTIVATE="$HOME/.venvs/revelio/bin/activate"
SCRIPT="check_rcid_location.py"

# POINT THIS to a directory or a glob (same style as your working step2 code)
# Examples:
#   COMPANY_DIR="$HOME/revelio_labs/academic_company_ref"
COMPANY_DIR="${COMPANY_DIR:-$HOME/revelio_labs/academic_company_ref}"
LIMIT_FILES="${LIMIT_FILES:-1}"      # default: just one shard, bump if you want more
N_EXAMPLES="${N_EXAMPLES:-12}"
MAX_PER_NAME="${MAX_PER_NAME:-10}"
OUTCSV="${OUTCSV:-rcid_location_examples.csv}"

mkdir -p "$WORKDIR/logs"
cd "$WORKDIR"

export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK:-1}"

[[ -f "$VENV_ACTIVATE" ]] && source "$VENV_ACTIVATE" || echo "[WARN] no venv at $VENV_ACTIVATE"

[[ -f "$SCRIPT" ]] || { echo "[ERROR] Missing $SCRIPT in $PWD"; exit 2; }

echo "[INFO] Using COMPANY_DIR: $COMPANY_DIR (limit=$LIMIT_FILES)"
srun python "$SCRIPT" \
  --company-dir "$COMPANY_DIR" \
  --limit-files "$LIMIT_FILES" \
  --n-examples "$N_EXAMPLES" \
  --max-per-name "$MAX_PER_NAME" \
  --output-csv "$OUTCSV"

echo "[OK] Done. CSV: $OUTCSV"
