#!/usr/bin/env python
# check_inv_pos_shard.py
# Run sanity checks on one shard of inventors_matched_positions_spark

import os, glob
import pyarrow.parquet as pq
import pandas as pd

# ---------------- Load one shard ----------------
shard = "output/inventors_matched_positions_spark/shard=00"
files = glob.glob(os.path.join(shard, "*.parquet"))

print(f"[INFO] Reading {len(files)} parquet files from {shard}")
table = pq.read_table(files)
df = table.to_pandas()
print(f"[INFO] Loaded {len(df):,} rows")

# ---------------- 1. Row key uniqueness ----------------
dupes = df.duplicated(subset=["user_id", "patent_id", "position_id"]).sum()
print("Duplicate (user_id, patent_id, position_id) rows:", dupes)

# ---------------- 2. Nulls in critical IDs ----------------
print("Null counts in IDs:")
print(df[["user_id", "patent_id", "position_id"]].isna().sum())

# ---------------- 3. Patents per position ----------------
patents_per_pos = df.groupby(["user_id", "position_id"])["patent_id"].nunique()
print("\nPatents per (user_id, position_id) summary:")
print(patents_per_pos.describe())

# ---------------- 4. Position consistency ----------------
consistency = (
    df.groupby(["user_id", "position_id"])
      .agg({
          "company_name": "nunique",
          "city": "nunique",
          "salary": "nunique"
      })
)
inconsist = (consistency > 1).any(axis=1).sum()
print("Inconsistent position attributes across patents:", inconsist)

# ---------------- 5. Patents per inventor ----------------
patents_per_user = df.groupby("user_id")["patent_id"].nunique()
print("\nPatents per user summary:")
print(patents_per_user.describe())

# ---------------- 6. Patent date range ----------------
date_ranges = df.groupby("user_id")["patent_date"].agg(["min", "max"])
print("\nPatent date range for first 5 users:")
print(date_ranges.head())

# ---------------- 7. Salary distribution ----------------
print("\nSalary distribution:")
print(df[["salary","total_compensation","start_salary","end_salary"]].describe())

# ---------------- 8. Date consistency ----------------
invalid_dates = df[
    (df["enddate"].notna()) & (df["startdate"] > df["enddate"])
]
print("Invalid startdate > enddate rows:", len(invalid_dates))

# ---------------- 9. Missing patents check ----------------
# Load STEP1 for comparison
step1_files = glob.glob("output/inventors_matched_users/*.parquet")
step1 = pq.read_table(step1_files).to_pandas()

# restrict to users in this shard only
users_in_shard = df["user_id"].unique()
step1_sub = step1[step1["user_id"].isin(users_in_shard)]

# compare patent counts
step1_counts = step1_sub.groupby("user_id")["patent_id"].nunique()
joined_counts = df.groupby("user_id")["patent_id"].nunique()
comparison = step1_counts.to_frame("n_patents_step1").join(
    joined_counts.to_frame("n_patents_joined"), how="left"
).fillna(0)
comparison["missing_patents"] = (
    comparison["n_patents_step1"] - comparison["n_patents_joined"]
)

print("\nMissing patents summary (shard=00):")
print(comparison["missing_patents"].describe())
print("Inventors with missing patents:", (comparison["missing_patents"] > 0).sum())
