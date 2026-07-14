#!/bin/bash
#SBATCH --job-name=fqV3Validate
#SBATCH --output=/home/epiga/revelio_labs/logs/founder_quality_v1/validation_v3/fqV3Validate_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/founder_quality_v1/validation_v3/fqV3Validate_%j.err
#SBATCH --time=24:00:00
#SBATCH --mem=120G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general

# Read-only V3 validation. It writes only to output/founder_quality_v1/validation_v3/.
# Do not use "set -u": the project's Conda activation hook references optional variables.
set -eo pipefail

cd /home/epiga/revelio_labs
source /home/epiga/miniconda/bin/activate revelio

export PYTHONUNBUFFERED=1
export OMP_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export OPENBLAS_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export MKL_NUM_THREADS="${SLURM_CPUS_PER_TASK}"
export NUMEXPR_NUM_THREADS="${SLURM_CPUS_PER_TASK}"

echo "[INFO] $(date --iso-8601=seconds) job=${SLURM_JOB_ID} host=$(hostname)"
echo "[INFO] python=$(which python)"
python --version
python - <<'PY'
import pyspark
print(f"[INFO] pyspark={pyspark.__version__}")
PY

python src/founder_quality_v1/validate_founder_quality_v3.py \
  --out-root /home/epiga/revelio_labs/output/founder_quality_v1/validation_v3 \
  --shuffle-partitions 320
