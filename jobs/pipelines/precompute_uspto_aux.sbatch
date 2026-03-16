#!/usr/bin/env bash
#SBATCH --job-name=uspto_aux
#SBATCH --output=logs/uspto_aux_%j.out
#SBATCH --error=logs/uspto_aux_%j.err
#SBATCH --time=48:00:00
#SBATCH --cpus-per-task=64
#SBATCH --mem=500G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

source "$HOME/miniconda/bin/activate" revelio
export JAVA_HOME="$HOME/jdk-17"
export PATH="$JAVA_HOME/bin:$PATH"

SCRATCH_BASE="/labs/khanna/linkedin_202507/scratch/spark_uspto_aux"
SCRATCH="$SCRATCH_BASE/${SLURM_JOB_ID}"
mkdir -p "$SCRATCH"/{tmp,local}
export TMPDIR="$SCRATCH/tmp"
export SPARK_LOCAL_DIRS="$SCRATCH/local"
export _JAVA_OPTIONS="-Djava.io.tmpdir=$TMPDIR -XX:+UseG1GC -XX:MaxGCPauseMillis=200 -XX:+ExitOnOutOfMemoryError"
export SPARK_DRIVER_MEMORY=350g
export SPARK_EXECUTOR_MEMORY=350g

USPTO_DIR="/labs/khanna/USPTO_202507"
OUT_DIR="/labs/khanna/linkedin_202507/processed/uspto_aux_202507"

python -u precompute_uspto_aux.py \
  --us-cit-path "$USPTO_DIR/g_us_patent_citation.tsv" \
  --cpc-path    "$USPTO_DIR/g_cpc_current.tsv" \
  --out-dir     "$OUT_DIR" \
  --threads "${SLURM_CPUS_PER_TASK}" \
  --shuffle-partitions 4000 \
  --coalesce 800 \
  --tmpdir "$SCRATCH"

rm -rf "$SCRATCH" || true
