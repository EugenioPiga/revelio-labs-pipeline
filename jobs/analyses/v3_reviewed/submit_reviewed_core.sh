#!/usr/bin/env bash
# Optional convenience launcher. It does not run automatically.
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export REVELIO_ANALYSES_ROOT="$ROOT"

preflight_id="$(sbatch --parsable "$ROOT/00_preflight/preflight_v3.sbatch")"
dep="afterok:${preflight_id}"

echo "Preflight: $preflight_id"
echo "Submitting reviewed core jobs after preflight succeeds..."

for job in \
  "$ROOT/01_tenure/tenure_suite_2010_2019.sbatch" \
  "$ROOT/02_lifecycle/lifecycle_v3.sbatch" \
  "$ROOT/03_colleague_links/colleague_links_v3_reviewed.sbatch" \
  "$ROOT/04_mobility/mobility_v3_reviewed.sbatch" \
  "$ROOT/05_metro_parent_productivity/metro_parent_productivity_v3_reviewed.sbatch"
do
  id="$(sbatch --parsable --dependency="$dep" "$job")"
  echo "$(basename "$job"): $id"
done

echo
echo "The 2000–2019 tenure robustness is intentionally not submitted here."
echo "Run it separately after inspecting the 2010–2019 core outputs."
