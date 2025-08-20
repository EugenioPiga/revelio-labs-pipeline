#!/usr/bin/env python
# make_tabs_report_txt.py
#
# Reads the stats parquet and writes:
#   - report/tabs_report.txt  (human-readable text tables)
#   - report/tabs_report.csv  (raw table for analysis)

import os
import dask.dataframe as dd

OUT_DIR = "report"
os.makedirs(OUT_DIR, exist_ok=True)

# Load stats parquet
df = dd.read_parquet("output/tabs_stats").compute()

# ---- Write CSV ----
csv_path = os.path.join(OUT_DIR, "tabs_report.csv")
df.to_csv(csv_path, index=False)
print(f"[OK] wrote CSV to {csv_path}")

# ---- Write TXT ----
txt_path = os.path.join(OUT_DIR, "tabs_report.txt")
with open(txt_path, "w", encoding="utf-8") as f:
    for ds, sub in df.groupby("dataset"):
        f.write("="*80 + "\n")
        f.write(f"DATASET: {ds}\n")
        f.write("="*80 + "\n\n")

        # Pick only the relevant columns
        sub = sub[["column","dtype","n_rows","n_missing","missing_frac","mean","std"]]

        # Format as text table (like .head() in Pandas but full)
        f.write(sub.to_string(index=False))
        f.write("\n\n")

print(f"[OK] wrote TXT to {txt_path}")
