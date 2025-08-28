#!/usr/bin/env bash
# submit_steps_2_3_only.sbatch
# Launcher: submit Step 2 immediately, then Step 3 after Step 2 succeeds.

#SBATCH --job-name=split_launcher_2_3
#SBATCH --output=logs/launcher_2_3_%j.out
#SBATCH --error=logs/launcher_2_3_%j.err
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
TIME="72:00:00"
CPUS=72
MEM="240G"
EXCLUDE_NODES=""

POSITIONS_DIR="academic_individual_position"
EDUCATION_DIR="academic_individual_user_education"

PART_SIZE1="128MB"
PART_SIZE2="1GB"  
PART_SIZE3="128MB"

VENV_ACTIVATE="$HOME/.venvs/revelio/bin/activate"
# ----------------------------------------

# Require Step-1 output to exist
if [ ! -d "output/inventors_matched_users" ]; then
  echo "ERROR: Missing Step-1 output directory: output/inventors_matched_users"
  echo "       Aborting (we only run Step 2 and Step 3)."
  exit 1
fi

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

# minimal hardening for many files
ulimit -n 65536

echo "Host: \$(hostname)"; echo "Date: \$(date)"; python -V
EOF
}

STEP2_FILE="logs/step2_merge_child.sbatch"
STEP3_FILE="logs/step3_merge_child.sbatch"

# ---------------- Step 2 ----------------
write_child_header "step2_merge" > "$STEP2_FILE"
cat >> "$STEP2_FILE" <<'EOF'
# Write to node-local scratch to avoid NFS Remote I/O; publish only on success
SCRATCH="${SLURM_TMPDIR:-${TMPDIR:-/tmp}}"
OUT_LOCAL="$SCRATCH/step2_out"
mkdir -p "$OUT_LOCAL"

# Let Python honor these (requires the tiny env hook in step2 script)
export OUTDIR="$OUT_LOCAL"
export PARTITION_SIZE="${PARTITION_SIZE:-1GB}"

python -u master_merge_pipeline.py \
  --positions-dir "academic_individual_position" \
  --education-dir "academic_individual_user_education" \
  --part-size-1 "128MB" \
  --part-size-2 "1GB" \
  --part-size-3 "128MB" \
  --skip-1 --skip-3

# Publish only if the step created a success sentinel in OUT_LOCAL
if [ -f "$OUT_LOCAL/_SUCCESS" ]; then
  ts="$(date +%Y%m%d_%H%M)"
  DST="output/inventors_matched_positions_${ts}"
  mkdir -p output
  rsync -av "$OUT_LOCAL/" "$DST/"
  ln -sfn "$(basename "$DST")" output/inventors_matched_positions
  echo "Step 2 OK → $DST (symlink updated)."
else
  echo "WARNING: Step 2 did not create _SUCCESS in \$OUT_LOCAL; not publishing."
  exit 99
fi
EOF

# ---------------- Step 3 ----------------
write_child_header "step3_merge" > "$STEP3_FILE"
cat >> "$STEP3_FILE" <<'EOF'
python -u master_merge_pipeline.py \
  --positions-dir "academic_individual_position" \
  --education-dir "academic_individual_user_education" \
  --part-size-1 "128MB" \
  --part-size-2 "1GB" \
  --part-size-3 "128MB" \
  --skip-1 --skip-2
EOF

# Submit: Step 2 now; Step 3 after Step 2 succeeds
jid2=$(sbatch "$STEP2_FILE" | awk '{print $4}')
echo "Submitted step2 as JobID: $jid2"

jid3=$(sbatch --dependency=afterok:$jid2 "$STEP3_FILE" | awk '{print $4}')
echo "Submitted step3 as JobID: $jid3 (afterok:$jid2)"

echo "Track with: squeue -u $USER"
