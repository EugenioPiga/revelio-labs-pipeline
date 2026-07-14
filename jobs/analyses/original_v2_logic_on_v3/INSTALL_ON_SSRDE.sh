#!/usr/bin/env bash
set -euo pipefail
ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3"
cd "$ROOT"
source 00_config/v3_env.sh
[[ -d "$REVELIO_V3_INPUT" ]] || { echo "[ERROR] Missing V3 core input"; exit 1; }
[[ -d "$REVELIO_V3_COLLEAGUE_INPUT" ]] || { echo "[ERROR] Missing V3 colleague input"; exit 1; }
[[ -f "$REVELIO_V3_DENSITY_CROSSWALK" ]] || { echo "[ERROR] Missing density crosswalk"; exit 1; }
module purge
module load R/4.5.1
for f in $(find . -type f -name '*.R' | sort); do
  Rscript -e "parse(file='$f'); cat('[PASS] ', '$f', '\n', sep='')"
done
for f in $(find . -type f \( -name '*.sbatch' -o -name '*.sh' \) | sort); do
  bash -n "$f"
  echo "[PASS] $f"
done
mkdir -p logs/submissions output
echo '[PASS] Original V2-logic-on-V3 package installed and parsed.'
