#!/usr/bin/env python
# ===========================================
# Memory-safe shard-based Employer ↔ Assignee crosswalk
# ===========================================
import os
import glob
import dask.dataframe as dd
import pandas as pd
from rapidfuzz import fuzz

# -----------------------------
# Config
# -----------------------------
ASSIGNEE_PATH = "/labs/khanna/linkedin_202507/revelio_patents_assignee_matches"
EMPLOYER_DIR = "/labs/khanna/linkedin_202507/processed/inventor_position_with_education"
OUT_DIR = "/home/epiga/revelio_labs/output/crosswalk_shards"
FINAL_PATH = "/home/epiga/revelio_labs/output/crosswalk_rcid_employer_assignee_weighted.parquet"

os.makedirs(OUT_DIR, exist_ok=True)

# -----------------------------
# Load the smaller (assignee) side once
# -----------------------------
print("[INFO] Loading assignee dataset...")
assignee = dd.read_parquet(
    ASSIGNEE_PATH, columns=["patent_id", "rcid", "company_name"]
).rename(columns={"rcid": "assignee_rcid", "company_name": "assignee_name"})
assignee = assignee.dropna(subset=["patent_id", "assignee_rcid"])
assignee = assignee.persist()

# -----------------------------
# Find shards of employer data
# -----------------------------
shards = sorted(glob.glob(os.path.join(EMPLOYER_DIR, "*.parquet")))
print(f"[INFO] Found {len(shards)} employer shards in {EMPLOYER_DIR}")

for shard in shards:
    shard_name = os.path.basename(shard)
    out_path = os.path.join(OUT_DIR, f"crosswalk_{shard_name}")
    if os.path.exists(out_path):
        print(f"[SKIP] {shard_name} already processed.")
        continue

    print(f"[INFO] Processing {shard_name}...")

    # Load this employer shard
    emp = dd.read_parquet(
        shard, columns=["user_id", "patent_id", "rcid", "company_name"]
    ).rename(columns={"rcid": "employer_rcid", "company_name": "employer_name"})
    emp = emp.dropna(subset=["patent_id", "employer_rcid"])

    # Merge within shard on patent_id
    merged = assignee.merge(emp, on="patent_id", how="inner")

    # Aggregate counts inside this shard
    pair_counts = (
        merged.groupby(["employer_rcid", "employer_name", "assignee_rcid", "assignee_name"])
        .size()
        .reset_index()
        .rename(columns={0: "pair_count"})
    )

    # Bring result to pandas (small after grouping)
    pdf = pair_counts.compute()

    # Compute fuzzy similarity
    pdf["name_similarity"] = pdf.apply(
        lambda r: fuzz.token_sort_ratio(str(r["employer_name"]), str(r["assignee_name"])) / 100, axis=1
    )
    pdf["valid_match"] = pdf["name_similarity"] >= 0.85

    pdf.to_parquet(out_path, index=False)
    print(f"[INFO] Saved shard output → {out_path}")

# -----------------------------
# Combine all shard results
# -----------------------------
print("[INFO] Combining shard outputs...")
parts = dd.read_parquet(os.path.join(OUT_DIR, "*.parquet"))

agg = (
    parts.groupby(["employer_rcid", "employer_name", "assignee_rcid", "assignee_name"])
    ["pair_count"]
    .sum()
    .reset_index()
    .rename(columns={"pair_count": "total_pair_count"})
)

agg.to_parquet(FINAL_PATH, overwrite=True)
print(f"[INFO] Final weighted crosswalk saved to {FINAL_PATH}")
