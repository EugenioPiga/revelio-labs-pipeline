#!/bin/bash
set -eo pipefail
ROOT="$HOME/innovation_linkedin"
cd "$ROOT"
J0=$(sbatch --parsable jobs/pipelines/00_founder_preflight_v3.sbatch); J0=${J0%%;*}
J1=$(sbatch --parsable --dependency=afterok:"$J0" jobs/pipelines/01_founder_firm_year_v3.sbatch); J1=${J1%%;*}
J2=$(sbatch --parsable --dependency=afterok:"$J1" jobs/pipelines/02_founder_shards_v3.sbatch); J2=${J2%%;*}
J3=$(sbatch --parsable --dependency=afterok:"$J2" jobs/pipelines/03_founder_merge_v3.sbatch); J3=${J3%%;*}
echo "Founder preflight:      $J0"
echo "Founder firm-year:      $J1 (after preflight)"
echo "Founder shard array:    $J2 (after firm-year)"
echo "Founder merge:          $J3 (after all founder shards)"
