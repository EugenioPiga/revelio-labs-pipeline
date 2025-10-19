#!/usr/bin/env python3
# ==========================================
# Inspect crosswalk_rcid_employer_assignee_weighted.parquet
# ==========================================

import os
import pandas as pd
import pyarrow.parquet as pq

# -----------------------------
# Config
# -----------------------------
FILE = "/home/epiga/revelio_labs/output/crosswalk_rcid_employer_assignee_weighted.parquet"
OUT_DIR = "/home/epiga/revelio_labs/output"
os.makedirs(OUT_DIR, exist_ok=True)

print(f"[INFO] Reading Parquet file: {FILE}")
table = pq.read_table(FILE)
df = table.to_pandas()
print(f"[INFO] Loaded {len(df):,} rows and {len(df.columns)} columns")

# -----------------------------
# Quick schema & sample
# -----------------------------
print("\n[INFO] Columns:")
print(df.columns.tolist())

print("\n[INFO] Sample rows:")
print(df.head(5))

# -----------------------------
# Summary statistics
# -----------------------------
print("\n[INFO] Basic summary statistics:")
print(df.describe(include='all', datetime_is_numeric=True))

# -----------------------------
# Crosswalk diagnostics
# -----------------------------
if {"employer_rcid", "assignee_rcid", "weight"}.issubset(df.columns):
    print("\n[INFO] Summary of crosswalk weights:")
    summary = (
        df.groupby("employer_rcid")["weight"]
        .agg(["count", "sum", "mean", "max"])
        .reset_index()
        .rename(columns={"count": "n_assignees", "sum": "total_weight"})
    )
    print(summary.head(10))
    print(f"[INFO] Total unique employers: {summary.shape[0]}")
    print(f"[INFO] Total unique assignees: {df['assignee_rcid'].nunique()}")
    print(f"[INFO] Mean links per employer: {summary['n_assignees'].mean():.2f}")

    # Export diagnostic CSV
    out_summary = os.path.join(OUT_DIR, "crosswalk_employer_summary.csv")
    summary.to_csv(out_summary, index=False)
    print(f"[INFO] Employer-level summary saved to: {out_summary}")

elif {"employer_name", "assignee_name", "similarity"} <= set(df.columns):
    print("\n[INFO] Example name similarity diagnostics:")
    print(df.sort_values("similarity", ascending=False).head(10))
else:
    print("[WARN] Expected columns not found. Skipping detailed diagnostics.")
