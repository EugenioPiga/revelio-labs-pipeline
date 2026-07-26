FOODSERVICE 722 CONTROLS V1
===========================

This is a non-destructive post-linkage stage. It reads the completed exact and
exact-plus-fuzzy datasets and writes new enriched copies. It never overwrites
the existing final linkage.

Primary origin:
- non-U.S. when origin-v4 records a foreign origin;
- U.S. only when education and employment are both observed, every record has
  a country, and every observed country is the United States;
- otherwise unassigned.

Robustness origin:
- all workers with no observed non-U.S. signal are assigned to the U.S.

Controls:
- education-record counts and country coverage;
- highest degree category and degree-level flags;
- first education year, last education year, first job year;
- career-entry-year proxy;
- all gender/ethnicity/race columns found in the Revelio user file, prefixed
  with revelio_user_. These are inferred variables, not self-reported.

Expected output:
/home/epiga/revelio_labs/output/foodservice_722_controls_v1/
owner_backbone_refresh_20260715_021137_controls_v1
