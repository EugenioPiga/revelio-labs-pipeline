# Reviewed Revelio V3 Analysis Suite

This folder is a **safe replacement candidate**, not an in-place overwrite of
`analyses_v3`. Install it as:

```bash
~/revelio_labs/analyses_v3_reviewed
```

Then run `bash INSTALL_ON_SSRDE.sh` and submit the preflight job before any
analysis.

## V3 inputs

- Core analyses (tenure, lifecycle, mobility, metro-parent productivity):

  `/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged`

- Colleague citations/collaboration:

  `/labs/khanna/linkedin_202507/processed/v3_spell_integrity/inventor_year_merged_colleague`

The exact paths live only in `00_config/v3_env.sh`.

## Deliberate maintained choices

1. `first_parent_rcid`, `first_rcid`, `first_city`, and
   `first_metro_area` are interpreted as the **first observed values within an
   inventor-year**. They are therefore valid year-specific spell/location
   identifiers for the analyses here.

2. Main estimates use **U.S. position-years and U.S. last-degree-country**
   restrictions.

3. Tenure PPML figures retain the requested rescaling:

   `mean patents in the 0–5 reference bin × exp(PPML coefficient)`.

   The plots label this correctly as an *implied level rescaling*, not a full
   structural prediction.

4. Each R script remains self-contained at runtime: packages are installed
   only if missing.

## What is materially different from the original folder

- The accidental V3 migration bug that redirected the density-crosswalk input
  to an output directory is removed.
- The five-array-job tenure setup is replaced by a single suite per window.
  It reads the panel once and runs base plus seniority models from the same
  cleaned sample.
- Default tenure outputs focus on the core FE ladder and career split.
  Metro size, productivity, and density splits are retained as optional
  supplementary specifications.
- The lifecycle script no longer reruns costly PPML models already estimated
  by the tenure suite.
- The colleague script reads its filtered V3 panel once instead of making
  repeated Arrow scans and now reports denominator/classification integrity
  checks.
- The mobility module is an actual V3 program, not a wrapper that rewrites
  original source code at runtime. Origin and destination quality measures are
  leave-one-out peer averages.
- The metro decomposition uses a static PPML model as the primary
  specification; the dynamic lagged-patenting model is optional sensitivity
  because it has dynamic fixed-effect bias concerns.

## Run order

```bash
sbatch 00_preflight/preflight_v3.sbatch
sbatch 01_tenure/tenure_suite_2010_2019.sbatch
sbatch 02_lifecycle/lifecycle_v3.sbatch
sbatch 03_colleague_links/colleague_links_v3_reviewed.sbatch
sbatch 04_mobility/mobility_v3_reviewed.sbatch
sbatch 05_metro_parent_productivity/metro_parent_productivity_v3_reviewed.sbatch
```

Start with the 2010–2019 tenure job. The 2000–2019 tenure job is a longer
historical robustness:

```bash
sbatch 01_tenure/tenure_suite_2000_2019.sbatch
```

## Optional supplementary switches

```bash
# Extended tenure split package:
export TENURE_SPLITS="career,metro_size,metro_prod,density"
export TENURE_FE_SET="all"

# Optional descriptive mobility event profile and saved panel:
export MOBILITY_RUN_EVENT_PROFILE=1
export MOBILITY_WRITE_PANEL=1

# Dynamic PPML sensitivity for metro decomposition:
export METRO_RUN_DYNAMIC=1
```

The defaults intentionally avoid the costly outputs that were not essential
for the main empirical objects.
