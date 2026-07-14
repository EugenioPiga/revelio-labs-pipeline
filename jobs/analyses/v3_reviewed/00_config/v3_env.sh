#!/usr/bin/env bash
# Shared V3 configuration for the reviewed analysis suite.
#
# This file is intentionally the only place that should contain dataset paths.
# Source it before running an R script directly:
#   source ~/revelio_labs/analyses_v3_reviewed/00_config/v3_env.sh

export REVELIO_ANALYSES_ROOT="${REVELIO_ANALYSES_ROOT:-$HOME/revelio_labs/analyses_v3_reviewed}"

# Spell-safe core V3 inventor-year panel.
export REVELIO_V3_INPUT="${REVELIO_V3_INPUT:-/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged}"

# Step-6-enriched V3 panel for colleague citation/collaboration outcomes only.
export REVELIO_V3_COLLEAGUE_INPUT="${REVELIO_V3_COLLEAGUE_INPUT:-/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged_colleague}"

# Reviewed outputs are isolated from the original analyses_v3 outputs.
export REVELIO_V3_OUTPUT_ROOT="${REVELIO_V3_OUTPUT_ROOT:-$REVELIO_ANALYSES_ROOT/output}"

# This is an input, not an output directory. It is used only by the optional
# metro-density tenure split. Change it only if you intentionally rebuild the
# crosswalk elsewhere.
export REVELIO_METRO_DENSITY_CROSSWALK="${REVELIO_METRO_DENSITY_CROSSWALK:-$HOME/revelio_labs/output/metro_density_match_check/diagnostics/final_revelio_metro_density_crosswalk.csv}"

# Reproducible defaults shared by the economic analyses.
export REVELIO_US_COUNTRY="${REVELIO_US_COUNTRY:-United States}"
export REVELIO_R_THREADS="${REVELIO_R_THREADS:-${SLURM_CPUS_PER_TASK:-1}}"
