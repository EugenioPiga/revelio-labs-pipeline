#!/usr/bin/env bash
#SBATCH --job-name=merge_master
#SBATCH --output=logs/master_%j.out
#SBATCH --error=logs/master_%j.err
#SBATCH --time=12:00:00                # shorter helps backfill; raise if needed
#SBATCH --cpus-per-task=4
#SBATCH --mem=128G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]

set -euo pipefail
mkdir -p logs
source "$HOME/.venvs/revelio/bin/activate"

export PYTHONUNBUFFERED=1
export PYTHONNOUSERSITE=1
export TMPDIR="${SLURM_TMPDIR:-/tmp}"
export DASK_TEMPORARY_DIRECTORY="$TMPDIR/dask-tmp"
mkdir -p "$DASK_TEMPORARY_DIRECTORY"

export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET="0.60"
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL="0.70"
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE="0.95"
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export PYARROW_THREAD_POOL_SIZE="${SLURM_CPUS_PER_TASK}"

echo "Host: $(hostname)"; echo "Date: $(date)"; python -V

python -u master_merge_pipeline.py \
  --positions-dir academic_individual_position \
  --education-dir academic_individual_user_education \
  --part-size-1 256MB --part-size-2 256MB --part-size-3 256MB
