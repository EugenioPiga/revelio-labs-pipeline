#!/usr/bin/env bash
# Run from the extracted analyses_v3_reviewed directory.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REVELIO_ANALYSES_ROOT="$ROOT"

source "$ROOT/00_config/v3_env.sh"

mkdir -p \
  "$ROOT/logs/preflight" \
  "$ROOT/logs/tenure" \
  "$ROOT/logs/lifecycle" \
  "$ROOT/logs/colleague_links" \
  "$ROOT/logs/mobility" \
  "$ROOT/logs/metro_parent_productivity" \
  "$REVELIO_V3_OUTPUT_ROOT"

echo "============================================================"
echo "REVIEWED V3 ANALYSIS SUITE"
echo "============================================================"
echo "Root:             $ROOT"
echo "Core panel:       $REVELIO_V3_INPUT"
echo "Colleague panel:  $REVELIO_V3_COLLEAGUE_INPUT"
echo "Output root:      $REVELIO_V3_OUTPUT_ROOT"
echo "Density crosswalk:$REVELIO_METRO_DENSITY_CROSSWALK"
echo

for d in "$REVELIO_V3_INPUT" "$REVELIO_V3_COLLEAGUE_INPUT"; do
  [[ -d "$d" ]] || { echo "[ERROR] Missing input panel: $d"; exit 1; }
done

if [[ ! -f "$REVELIO_METRO_DENSITY_CROSSWALK" ]]; then
  echo "[WARN] Density crosswalk is not currently visible."
  echo "       Main analyses will work; the optional TENURE_SPLITS=density will not."
fi

module purge
module load R/4.5.1
export R_LIBS_USER="$HOME/R/library"

echo
echo "Parsing all R scripts:"
while IFS= read -r -d '' f; do
  Rscript -e "parse(file='$f')" >/dev/null
  echo "  OK  ${f#$ROOT/}"
done < <(find "$ROOT" -type f -name '*.R' -print0)

echo
echo "Checking all sbatch/bash scripts:"
while IFS= read -r -d '' f; do
  bash -n "$f"
  echo "  OK  ${f#$ROOT/}"
done < <(find "$ROOT" -type f \( -name '*.sbatch' -o -name '*.sh' \) -print0)


echo
echo "Running shared-helper synthetic checks:"
Rscript "$ROOT/00_preflight/synthetic_helper_checks.R"

echo
echo "[PASS] Installation and syntax checks complete."
echo "Next:"
echo "  sbatch $ROOT/00_preflight/preflight_v3.sbatch"
