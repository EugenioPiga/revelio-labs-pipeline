#!/usr/bin/env bash
# Original V2-derived analysis logic, run on spell-integrity V3 inputs.
export REVELIO_V3_INPUT="/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged"
export REVELIO_V3_COLLEAGUE_INPUT="/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged_colleague"
export REVELIO_V3_DENSITY_CROSSWALK="/home/epiga/revelio_labs/output/metro_density_match_check/diagnostics/final_revelio_metro_density_crosswalk.csv"
export REVELIO_V3_OUTPUT_ROOT="$HOME/revelio_labs/analyses_original_v2_logic_on_v3/output"
export REVELIO_R_THREADS="${SLURM_CPUS_PER_TASK:-1}"
