#!/bin/bash
#SBATCH --job-name=studyColleagueUS
#SBATCH --output=/home/epiga/revelio_labs/logs/colleague_links_us/studyColleagueUS_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/colleague_links_us/studyColleagueUS_%j.err
#SBATCH --time=12:00:00
#SBATCH --mem=120G
#SBATCH --cpus-per-task=1
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

set -euo pipefail

echo "[INFO] Job started on $(hostname) at $(date)"
echo "[INFO] CPUs: $SLURM_CPUS_PER_TASK | Mem: ${SLURM_MEM_PER_NODE:-NA}MB | JobID: $SLURM_JOB_ID"

module purge
module load R/4.5.1
export R_LIBS_USER=~/R/library
mkdir -p "$R_LIBS_USER"

export OMP_NUM_THREADS=1
export OPENBLAS_NUM_THREADS=1
export MKL_NUM_THREADS=1
export VECLIB_MAXIMUM_THREADS=1
export NUMEXPR_NUM_THREADS=1

TMP_BASE="/home/epiga/revelio_labs/tmp/${SLURM_JOB_ID}"
mkdir -p "$TMP_BASE"

export TMPDIR="$TMP_BASE"
export TMP="$TMP_BASE"
export TEMP="$TMP_BASE"

R_SCRIPT="$HOME/innovation_linkedin/src/analyses/study_colleague_links_1990_2019_us.R"
LOG_DIR="/home/epiga/revelio_labs/logs/colleague_links_us"
OUT_DIR="/home/epiga/revelio_labs/output/colleague_links_1990_2019_us"

mkdir -p "$LOG_DIR" "$OUT_DIR"

echo "[INFO] Rscript path: $(which Rscript)"
Rscript --version
echo "[INFO] R_LIBS_USER=$R_LIBS_USER"

echo "[INFO] Checking required R packages..."
Rscript - <<'RSCRIPT'
required <- c(
  "arrow",
  "dplyr",
  "tidyr",
  "ggplot2",
  "readr",
  "scales",
  "stringr",
  "rlang"
)

lib <- Sys.getenv("R_LIBS_USER")
dir.create(lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(lib, .libPaths()))

missing <- required[!vapply(required, requireNamespace, logical(1), quietly = TRUE)]

if (length(missing) > 0) {
  message("[INFO] Installing missing packages: ", paste(missing, collapse = ", "))
  install.packages(missing, repos = "https://cloud.r-project.org")
} else {
  message("[INFO] All required packages already installed.")
}

for (pkg in required) {
  suppressPackageStartupMessages(library(pkg, character.only = TRUE))
  message("[OK] Loaded ", pkg)
}
RSCRIPT

echo "[INFO] Running R script: $R_SCRIPT"
Rscript "$R_SCRIPT"

status=$?

if [ $status -ne 0 ]; then
  echo "[ERROR] Script failed with exit code $status"
  echo "[ERROR] Check logs:"
  echo "        $LOG_DIR/studyColleagueUS_${SLURM_JOB_ID}.out"
  echo "        $LOG_DIR/studyColleagueUS_${SLURM_JOB_ID}.err"
  exit 1
else
  echo "[INFO] Script completed successfully."
fi

echo "[INFO] Main output directory: $OUT_DIR"
echo "[INFO] Job finished at $(date)"

rm -rf "$TMP_BASE" || true
exit 0
