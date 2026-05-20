#!/usr/bin/env bash
# submit_split_pipeline.sbatch
# Launcher: submits three child jobs with dependencies: step1 -> step2 -> step3

#SBATCH --job-name=split_launcher
#SBATCH --output=logs/launcher_%j.out
#SBATCH --error=logs/launcher_%j.err
#SBATCH --time=00:10:00
#SBATCH --cpus-per-task=1
#SBATCH --mem=1G
#SBATCH --partition=general
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
mkdir -p logs

# ---------- configurable knobs ----------
PARTITION="general"
TIME="72:00:00"                              # more headroom for heavy merges
CPUS=56
MEM="250G"
EXCLUDE_NODES="ssrde-c-[208,210,501-502]"

POSITIONS_DIR="academic_individual_position"
EDUCATION_DIR="academic_individual_user_education"

PART_SIZE1="128MB"
PART_SIZE2="128MB"
PART_SIZE3="128MB"

VENV_ACTIVATE="$HOME/.venvs/revelio/bin/activate"
# ----------------------------------------

write_child_header () {
  local name="$1"
  cat <<EOF
#!/usr/bin/env bash
#SBATCH --job-name=$name
#SBATCH --partition=$PARTITION
#SBATCH --time=$TIME
#SBATCH --cpus-per-task=$CPUS
#SBATCH --mem=$MEM
#SBATCH --exclude=$EXCLUDE_NODES
#SBATCH --output=logs/${name}_%j.out
#SBATCH --error=logs/${name}_%j.err
#SBATCH --mail-user=epiga@ucsd.edu
#SBATCH --mail-type=BEGIN,END,FAIL,TIME_LIMIT

set -euo pipefail
source "$VENV_ACTIVATE"
export PYTHONUNBUFFERED=1
export PYTHONNOUSERSITE=1
export DASK_DISTRIBUTED__SHUFFLE__METHOD=p2p

export TMPDIR="\${SLURM_TMPDIR:-/tmp}"
export DASK_TEMPORARY_DIRECTORY="\$TMPDIR/dask-tmp"
mkdir -p "\$DASK_TEMPORARY_DIRECTORY"

export DASK_DISTRIBUTED__WORKER__MEMORY__TARGET="0.60"
export DASK_DISTRIBUTED__WORKER__MEMORY__SPILL="0.70"
export DASK_DISTRIBUTED__WORKER__MEMORY__TERMINATE="0.95"

export OMP_NUM_THREADS="\${SLURM_CPUS_PER_TASK}"
export MKL_NUM_THREADS="\${SLURM_CPUS_PER_TASK}"
export OPENBLAS_NUM_THREADS="\${SLURM_CPUS_PER_TASK}"
export PYARROW_THREAD_POOL_SIZE="\${SLURM_CPUS_PER_TASK}"

echo "Host: \$(hostname)"; echo "Date: \$(date)"; python -V
EOF
}

STEP1_FILE="logs/step1_merge_child.sbatch"
STEP2_FILE="logs/step2_merge_child.sbatch"
STEP3_FILE="logs/step3_merge_child.sbatch"

# Step 1
write_child_header "step1_merge" > "$STEP1_FILE"
cat >> "$STEP1_FILE" <<EOF
python -u master_merge_pipeline.py \
  --positions-dir "$POSITIONS_DIR" \
  --education-dir "$EDUCATION_DIR" \
  --part-size-1 "$PART_SIZE1" \
  --part-size-2 "$PART_SIZE2" \
  --part-size-3 "$PART_SIZE3" \
  --skip-2 --skip-3
EOF

# Step 2
write_child_header "step2_merge" > "$STEP2_FILE"
cat >> "$STEP2_FILE" <<EOF
python -u master_merge_pipeline.py \
  --positions-dir "$POSITIONS_DIR" \
  --education-dir "$EDUCATION_DIR" \
  --part-size-1 "$PART_SIZE1" \
  --part-size-2 "$PART_SIZE2" \
  --part-size-3 "$PART_SIZE3" \
  --skip-1 --skip-3
EOF

# Step 3
write_child_header "step3_merge" > "$STEP3_FILE"
cat >> "$STEP3_FILE" <<EOF
python -u master_merge_pipeline.py \
  --positions-dir "$POSITIONS_DIR" \
  --education-dir "$EDUCATION_DIR" \
  --part-size-1 "$PART_SIZE1" \
  --part-size-2 "$PART_SIZE2" \
  --part-size-3 "$PART_SIZE3" \
  --skip-1 --skip-2
EOF

# Submit with dependencies
jid1=$(sbatch "$STEP1_FILE" | awk '{print $4}')
echo "Submitted step1 as JobID: $jid1"

jid2=$(sbatch --dependency=afterok:$jid1 "$STEP2_FILE" | awk '{print $4}')
echo "Submitted step2 as JobID: $jid2 (afterok:$jid1)"

jid3=$(sbatch --dependency=afterok:$jid2 "$STEP3_FILE" | awk '{print $4}')
echo "Submitted step3 as JobID: $jid3 (afterok:$jid2)"

echo "Track with: squeue -u $USER"
