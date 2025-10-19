#!/usr/bin/env python
"""
Quick diagnostics for inventor–year dataset (merged version)
Goal:
 - Check if (user_id, year) is unique
 - Verify movers
 - Export a richer sample for Excel inspection
"""

import pyarrow.dataset as ds
import pandas as pd
import numpy as np

# -----------------------
# CONFIG
# -----------------------
INPUT_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
N_SHARDS = 20
SHARD = 3
YEAR_MIN = 1990
SAMPLE_N = 2_000_000  # optional subsample for speed

print(f"[INFO] Reading data from {INPUT_DIR}")
dataset = ds.dataset(INPUT_DIR, format="parquet")

# ✅ Load core columns first (we’ll use all later for the sample)
table = dataset.to_table(filter=(ds.field("year") >= YEAR_MIN))
df = table.to_pandas()

print(f"[INFO] Loaded {len(df):,} rows and {df.user_id.nunique():,} unique inventors")

# ✅ Apply shard filtering
df = df[df["first_rcid"].astype("Int64") % N_SHARDS == SHARD]
print(f"[INFO] Kept {len(df):,} rows for shard {SHARD}")

# Optional subsample
if len(df) > SAMPLE_N:
    df = df.sample(SAMPLE_N, random_state=42)
    print(f"[INFO] Subsampled to {len(df):,} rows for diagnostics")

# ===============================================================
# 0️⃣ Check uniqueness of (user_id, year)
# ===============================================================
dups = df.duplicated(subset=["user_id", "year"], keep=False)
dup_share = dups.mean()
n_dups = dups.sum()
print(f"\n[0] Uniqueness check:")
print(f"  → Total rows: {len(df):,}")
print(f"  → Duplicate inventor–year rows: {n_dups:,} ({dup_share:.2%})")

# ===============================================================
# 1️⃣ Basic panel structure
# ===============================================================
print("\n[1] Years per inventor:")
years_per = df.groupby("user_id")["year"].nunique()
print(years_per.describe())

print("\n[2] Time span per inventor:")
span = df.groupby("user_id").agg(min_year=("year", "min"), max_year=("year", "max"))
span["span"] = span["max_year"] - span["min_year"] + 1
print(span["span"].describe())

print("\n[3] Missing n_patents:")
print(df["n_patents"].isna().value_counts())
print("n_patents summary:")
print(df["n_patents"].describe())

# ===============================================================
# 2️⃣ Firm & city movers
# ===============================================================
df = df.sort_values(["user_id", "year"])
df["first_rcid_lag"] = df.groupby("user_id")["first_rcid"].shift()
df["first_city_lag"] = df.groupby("user_id")["first_city"].shift()

df["moved_firm"] = (df["first_rcid_lag"].notna()) & (df["first_rcid_lag"] != df["first_rcid"])
df["moved_city"] = (df["first_city_lag"].notna()) & (df["first_city_lag"] != df["first_city"])

print("\n[4] Movers summary:")
print(f"Share of obs where firm changes: {df['moved_firm'].mean():.4f}")
print(f"Share of obs where city changes: {df['moved_city'].mean():.4f}")

# ===============================================================
# 3️⃣ Check multiple firms per inventor–year
# ===============================================================
print("\n[5] Multiple firms per inventor–year?")
multi = df.groupby(["user_id", "year"]).size()
multi = multi[multi > 1]
print(f"Inventor-years with multiple rows: {len(multi):,}")
if len(multi) > 0:
    print(multi.head())

# ===============================================================
# 8️⃣ Export large sample with *all columns* for Excel inspection
# ===============================================================
sample_users = np.random.default_rng(42).choice(
    df["user_id"].dropna().unique(),
    size=min(10, df["user_id"].nunique()),
    replace=False
)

sample_df = df[df["user_id"].isin(sample_users)].copy()
sample_df = sample_df.sort_values(["user_id", "year"])

sample_path = "/home/epiga/revelio_labs/sample_inventor_year_fullcols.csv"
sample_df.to_csv(sample_path, index=False)

print(f"\n✅ Full-column sample written to: {sample_path}")
print(f"   Rows: {len(sample_df)} | Columns: {len(sample_df.columns)}")
print("   You can download this CSV and explore all variables directly in Excel.")

# ===============================================================
# 9️⃣ Example movers
# ===============================================================
mover_id = df.loc[df["moved_firm"], "user_id"].dropna().unique()
if len(mover_id) > 0:
    mid = mover_id[0]
    print(f"\n[Example] Inventor who changes firm: {mid}")
    print(df.loc[df.user_id == mid, ["year", "first_rcid", "n_patents"]])

city_mover_id = df.loc[df["moved_city"], "user_id"].dropna().unique()
if len(city_mover_id) > 0:
    cid = city_mover_id[0]
    print(f"\n[Example] Inventor who changes city: {cid}")
    print(df.loc[df.user_id == cid, ["year", "first_city", "n_patents"]])

print("\n[INFO] Diagnostics complete ✅")
