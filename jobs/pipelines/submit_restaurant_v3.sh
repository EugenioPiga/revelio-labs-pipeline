#!/bin/bash
set -eo pipefail
ROOT="$HOME/innovation_linkedin"
cd "$ROOT"
J0=$(sbatch --parsable jobs/pipelines/00_restaurant_preflight_v3.sbatch); J0=${J0%%;*}
J1=$(sbatch --parsable --dependency=afterok:"$J0" jobs/pipelines/01_restaurant_select_v3.sbatch); J1=${J1%%;*}
J2=$(sbatch --parsable --dependency=afterok:"$J1" jobs/pipelines/02_restaurant_enrich_people_v3.sbatch); J2=${J2%%;*}
J3=$(sbatch --parsable --dependency=afterok:"$J1" jobs/pipelines/03_restaurant_extract_histories_v3.sbatch); J3=${J3%%;*}
echo "Restaurant preflight:       $J0"
echo "Restaurant selection:       $J1 (after preflight)"
echo "Restaurant people enrich:   $J2 (after selection)"
echo "Restaurant full histories:  $J3 (after selection)"
