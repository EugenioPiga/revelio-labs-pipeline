#!/usr/bin/env bash
set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
cd "$ROOT"

# Default: wait for the currently running reviewed V3 tenure and metro jobs.
# Override with colon-separated IDs, e.g. ./submit_all_original_v2_logic_on_v3.sh 1432700:1432706
UPSTREAM_IDS="${1:-1432700:1432706}"
BASE_DEP="afterok:${UPSTREAM_IDS}"
mkdir -p logs/submissions
STAMP="$(date +%Y%m%d_%H%M%S)"
LOG="logs/submissions/original_v2logic_v3_all_${STAMP}.tsv"

submit_after() {
  local dep="$1"; shift
  sbatch --parsable --dependency="$dep" "$@"
}

PRE="$(submit_after "$BASE_DEP" 00_preflight/preflight_original_v2_logic_on_v3.sbatch)"
MAIN10="$(submit_after "afterok:${PRE}" 01_tenure/no_seniority/ppml_tenure_ladder_original_v2logic_2010_2019.sbatch)"
SEN10="$(submit_after "afterok:${MAIN10}" 01_tenure/with_seniority/ppml_tenure_ladder_seniority_original_v2logic_2010_2019.sbatch)"
LIFE="$(submit_after "afterok:${SEN10}" 02_lifecycle/lifecycle_extended_original_v2logic.sbatch)"
COL="$(submit_after "afterok:${LIFE}" 03_colleague_links/colleague_links_original_v2logic.sbatch)"
MOVE1="$(submit_after "afterok:${COL}" 04_mobility/churn_mobility_extended_original_v2logic.sbatch)"
MOVE2="$(submit_after "afterok:${MOVE1}" 04_mobility/churn_vs_firm_characteristics_original_v2logic.sbatch)"
METRO="$(submit_after "afterok:${MOVE2}" 05_metro_parent_productivity/metro_parent_productivity_original_v2logic.sbatch)"
MAIN00="$(submit_after "afterok:${METRO}" 01_tenure/no_seniority/ppml_tenure_ladder_original_v2logic_2000_2019.sbatch)"
SEN00="$(submit_after "afterok:${MAIN00}" 01_tenure/with_seniority/ppml_tenure_ladder_seniority_original_v2logic_2000_2019.sbatch)"

{
  printf 'stage\tjob_id\tdependency\n'
  printf 'preflight\t%s\t%s\n' "$PRE" "$BASE_DEP"
  printf 'tenure_no_seniority_2010_2019\t%s\tafterok:%s\n' "$MAIN10" "$PRE"
  printf 'tenure_seniority_2010_2019\t%s\tafterok:%s\n' "$SEN10" "$MAIN10"
  printf 'lifecycle\t%s\tafterok:%s\n' "$LIFE" "$SEN10"
  printf 'colleague_links\t%s\tafterok:%s\n' "$COL" "$LIFE"
  printf 'churn_mobility_extended\t%s\tafterok:%s\n' "$MOVE1" "$COL"
  printf 'churn_vs_firm_characteristics\t%s\tafterok:%s\n' "$MOVE2" "$MOVE1"
  printf 'metro_parent_productivity\t%s\tafterok:%s\n' "$METRO" "$MOVE2"
  printf 'tenure_no_seniority_2000_2019\t%s\tafterok:%s\n' "$MAIN00" "$METRO"
  printf 'tenure_seniority_2000_2019\t%s\tafterok:%s\n' "$SEN00" "$MAIN00"
} | tee "$LOG"

echo "[PASS] Submitted the full original V2-logic-on-V3 chain. Submission log: $ROOT/$LOG"
