FOODSERVICE 722 CONTROLS V2
===========================

This is a non-destructive post-linkage stage. It reads the completed exact and
exact-plus-fuzzy datasets and writes new enriched copies. It never overwrites
the existing final linkage or the earlier controls-v1 project.

Preferred origin:
- non-U.S. when origin-v4 resolves a foreign country;
- U.S. only when education and employment histories are both present, every
  record in both histories has a valid country, and every country is the U.S.;
- otherwise origin is left unassigned.

Robustness origin:
- all workers with no observed non-U.S. signal are assigned to the U.S.,
  including workers with incomplete country coverage.

The output distinguishes:
- confirmed conservative U.S. origin;
- U.S. evidence in both histories with partial country coverage;
- unassigned workers with limited or incomplete histories;
- non-U.S. signals whose country cannot be resolved.

Controls:
- education and employment record counts and country coverage;
- highest degree category and degree-level flags;
- first education year, last education year, and first job year;
- career-entry-year proxy;
- all gender/ethnicity/race columns found in the Revelio user file, prefixed
  with revelio_user_. These are inferred variables, not self-reported.

Expected output:
/home/epiga/revelio_labs/output/foodservice_722_controls_v2/
owner_backbone_refresh_20260715_021137_controls_v2
