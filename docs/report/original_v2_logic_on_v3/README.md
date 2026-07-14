# Original V2-derived analysis logic on V3

This is an isolated reproducibility run of the older analysis sources on the validated spell-integrity V3 panel. It does **not** overwrite `analyses_v3_reviewed` or its outputs.

## Full original run

```bash
cd ~/revelio_labs/analyses_original_v2_logic_on_v3
bash INSTALL_ON_SSRDE.sh
bash submit_all_original_v2_logic_on_v3.sh 1432700:1432706
```

The full launcher waits for the current reviewed tenure and reviewed metro jobs. It then runs every original source module on V3: 2010–2019 no-seniority tenure, 2010–2019 seniority tenure, lifecycle, colleague links, the two original mobility components, metro-parent productivity, and both 2000–2019 tenure arrays. The jobs are deliberately chained to prevent concurrent repeated full-panel scans.

`submit_original_v2_logic_on_v3_main.sh` remains available when only the 2010–2019 original-source suite is desired.

Read `PORTING_MANIFEST.md` for exact changes.
