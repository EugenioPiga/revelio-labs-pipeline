#!/usr/bin/env bash
#SBATCH --job-name=copy_li_202507
#SBATCH --output=logs/copy_li_202507_%j.out
#SBATCH --error=logs/copy_li_202507_%j.err
#SBATCH --time=12:00:00
#SBATCH --cpus-per-task=4
#SBATCH --mem=8G
#SBATCH --partition=general
#SBATCH --exclude=ssrde-c-[208,210,501-502]

set -euo pipefail
mkdir -p logs

SRC="/home/epiga/revelio_labs"
DEST="/labs/khanna/linkedin_202507"   # this already exists; we keep mkdir -p just in case
mkdir -p "$DEST"

DATASETS=(
  academic_company_ref
  academic_individual_position
  academic_individual_user
  academic_individual_user_education
  academic_individual_user_skill
  revelio_patents_assignee_matches
  revelio_patents_inventor_matches
)

CHECK_SH="check_revelio_downloads.sh"

echo "== Copy job starting =="
echo "Host: $(hostname)"
echo "Time: $(date)"
echo "SRC : $SRC"
echo "DEST: $DEST"
echo "Datasets: ${DATASETS[*]}"
echo

# Simple parallel limiter using background jobs (up to $SLURM_CPUS_PER_TASK concurrent)
parallel_limit() {
  while [ "$(jobs -rp | wc -l)" -ge "${SLURM_CPUS_PER_TASK:-4}" ]; do
    sleep 1
  done
}

# Copy datasets (Parquet only), preserving directory structure
for d in "${DATASETS[@]}"; do
  srcd="$SRC/$d"
  destd="$DEST/$d"
  if [ -d "$srcd" ]; then
    mkdir -p "$destd"
    echo ">> Copying $d (parquet only)…"
    parallel_limit
    rsync -a --info=stats1 \
      --prune-empty-dirs \
      --include '*/' --include '*.parquet' --exclude '*' \
      "$srcd/" "$destd/" &
  else
    echo "WARN: missing source dataset directory: $srcd" >&2
  fi
done
wait

# Copy the check script
if [ -f "$SRC/$CHECK_SH" ]; then
  echo ">> Copying $CHECK_SH…"
  install -m 0755 "$SRC/$CHECK_SH" "$DEST/$CHECK_SH"
else
  echo "WARN: $CHECK_SH not found in $SRC" >&2
fi

echo
echo "== Verifying counts =="
printf "%-42s %12s %12s\n" "DATASET" "SRC(.parq)" "DEST(.parq)"
for d in "${DATASETS[@]}"; do
  srcd="$SRC/$d"; destd="$DEST/$d"
  src_cnt=$( [ -d "$srcd" ] && find "$srcd" -type f -name '*.parquet' | wc -l || echo 0 )
  dst_cnt=$( [ -d "$destd" ] && find "$destd" -type f -name '*.parquet' | wc -l || echo 0 )
  printf "%-42s %12d %12d\n" "$d" "$src_cnt" "$dst_cnt"
done

[ -f "$DEST/$CHECK_SH" ] && echo "OK: $CHECK_SH present in $DEST" || echo "WARN: $CHECK_SH missing in $DEST"

echo
echo "Disk usage summary:"
du -sh "$DEST" || true
echo "Done at: $(date)"
