#!/bin/bash
#SBATCH --job-name=mgrFE_rules
#SBATCH --partition=general
#SBATCH --time=02:00:00
#SBATCH --cpus-per-task=6
#SBATCH --mem=96G
#SBATCH --output=/home/epiga/revelio_labs/manager_fe_preflight_20260724/logs/mgrFE_rules_%j.out
#SBATCH --error=/home/epiga/revelio_labs/manager_fe_preflight_20260724/logs/mgrFE_rules_%j.err
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=FAIL

set -euo pipefail

ROOT="$HOME/revelio_labs/manager_fe_preflight_20260724"
PYTHON="$HOME/miniconda/envs/revelio/bin/python"
RUN_DIR="$ROOT/runs/stage0e_${SLURM_JOB_ID}"

set +u
source "$HOME/miniconda/bin/activate" revelio
set -u

export PYSPARK_PYTHON="$PYTHON"
export PYSPARK_DRIVER_PYTHON="$PYTHON"

TMP_BASE="/tmp/${USER}/manager_fe_stage0e_${SLURM_JOB_ID}"
mkdir -p "$TMP_BASE"

export TMPDIR="$TMP_BASE"
export SPARK_LOCAL_DIRS="$TMP_BASE"

cleanup() {
    rm -rf "$TMP_BASE" 2>/dev/null || true
}
trap cleanup EXIT

echo "Host:       $(hostname)"
echo "Job ID:     $SLURM_JOB_ID"
echo "Run folder: $RUN_DIR"

spark-submit \
    --master "local[${SLURM_CPUS_PER_TASK}]" \
    --driver-memory 82g \
    --conf spark.local.dir="$TMP_BASE" \
    --conf spark.sql.shuffle.partitions=192 \
    "$ROOT/code/stage0e_rule_comparison.py" \
    --out-dir "$RUN_DIR"
