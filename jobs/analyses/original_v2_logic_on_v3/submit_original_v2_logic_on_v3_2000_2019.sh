#!/usr/bin/env bash
set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
cd "$ROOT"
# Supply the successful final job from the main chain as argument; this is intentionally separate.
FINAL_MAIN_JOB="${1:?Usage: ./submit_original_v2_logic_on_v3_2000_2019.sh <successful_final_main_job_id>}"
PRE="$(sbatch --parsable --dependency="afterok:${FINAL_MAIN_JOB}" 00_preflight/preflight_original_v2_logic_on_v3.sbatch)"
MAIN00="$(sbatch --parsable --dependency="afterok:${PRE}" 01_tenure/no_seniority/ppml_tenure_ladder_original_v2logic_2000_2019.sbatch)"
SEN00="$(sbatch --parsable --dependency="afterok:${MAIN00}" 01_tenure/with_seniority/ppml_tenure_ladder_seniority_original_v2logic_2000_2019.sbatch)"
printf 'preflight=%s
tenure_no_seniority_2000_2019=%s
tenure_seniority_2000_2019=%s
' "$PRE" "$MAIN00" "$SEN00"
