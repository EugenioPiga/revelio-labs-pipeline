# Final Revelio workflow snapshot

This repository snapshot collects the final code developed and validated during
late June and early July 2026.

## Included workflows

### Reviewed V3 analyses

- tenure analyses for the 2000–2019 and 2010–2019 windows;
- lifecycle analysis;
- colleague-link analysis;
- mobility analysis;
- metro-parent productivity analysis;
- shared V3 configuration and preflight checks.

Source code is under:

- `src/analyses/v3_reviewed/`

Slurm jobs and submission scripts are under:

- `jobs/analyses/v3_reviewed/`

### Original V2 logic on corrected V3 data

This suite preserves the original V2 empirical logic while using the corrected
V3 data construction.

Source code is under:

- `src/analyses/original_v2_logic_on_v3/`

Final Slurm jobs and submission scripts are under:

- `jobs/analyses/original_v2_logic_on_v3/`

Intermediate array jobs, superseded direct V3 jobs, and server-local source
snapshots are intentionally excluded.

### Founder-quality analysis

The retained workflow contains the final V3 audit, validation, and three
analysis phases, including the U.S.-year and U.S.-educated restricted
specification.

Source code is under:

- `src/analyses/founder_quality_v1/`

Slurm jobs and submission scripts are under:

- `jobs/analyses/founder_quality_v1/`

Unversioned and V2 audit drafts are intentionally excluded.

### Food-service restaurant linkage

The retained workflow contains:

- construction of Revelio entity-location keys;
- updated-backbone preflight;
- revised country-origin proxy using any foreign education or first job abroad;
- hierarchical exact restaurant matching;
- restricted fuzzy-match robustness;
- final chain classification and spell/person outputs;
- reconciliation and validation audits.

Pipeline code is under:

- `src/pipelines/foodservice_722/`
- `jobs/pipelines/foodservice_722/`

Diagnostic and validation code is under:

- `src/analyses/foodservice_722/`
- `jobs/analyses/foodservice_722/`

The hierarchical exact match is the preferred specification. The restricted
fuzzy match is retained as a robustness extension.

## SSRDE path convention

These files preserve the exact validated analysis logic and original SSRDE
execution paths. Data, logs, temporary files, and generated outputs remain
under `/home/epiga/revelio_labs`.

The copies in this repository are organized into `src` and `jobs` according to
their function. Some Slurm files therefore continue to reference the original
SSRDE project directories from which the validated runs were executed.

This is intentional: the snapshot preserves the versions actually used rather
than silently modifying them after validation.
