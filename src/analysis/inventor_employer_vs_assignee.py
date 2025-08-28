#!/usr/bin/env python
# inventor_employer_vs_assignee.py
#
# Question: How much the employer of the inventor matches with the assignees of the patent.
# Efficient design: prune columns early, filter positions to relevant inventors, stream to disk.

import os
import dask.dataframe as dd

# Paths
INVENTOR_PATH = "revelio_patents_inventor_matches/*.parquet"
POSITION_PATH = "academic_individual_position/*.parquet"
ASSIGNEE_PATH = "revelio_patents_assignee_matches/*.parquet"
OUTPUT_PATH   = "output/inventor_assignee_match.parquet"

# Optional sampling for testing (set SAMPLE_FRAC=0.01 for 1%)
SAMPLE_FRAC = float(os.environ.get("SAMPLE_FRAC", "1.0"))

def main():
    print("=== Loading datasets ===")

    # --- Inventors: keep only patent_id, user_id
    inventors = dd.read_parquet(INVENTOR_PATH)[["patent_id", "user_id"]]
    print("Inventor cols:", list(inventors.columns), "…")
    print("[OK] Inventors rows:", inventors.shape[0].compute())

    # --- Positions: keep only relevant cols
    positions = dd.read_parquet(POSITION_PATH)[["user_id", "rcid", "company_name"]]
    print("Position cols:", list(positions.columns), "…")
    print("[OK] Positions rows:", positions.shape[0].compute())

    # --- Assignees: keep only relevant cols
    assignees = dd.read_parquet(ASSIGNEE_PATH)[["patent_id", "rcid", "company_name"]]
    print("Assignee cols:", list(assignees.columns), "…")
    print("[OK] Assignees rows:", assignees.shape[0].compute())

    # --- Apply sampling (if set)
    if SAMPLE_FRAC < 1.0:
        inventors = inventors.sample(frac=SAMPLE_FRAC, random_state=42)
        positions = positions.sample(frac=SAMPLE_FRAC, random_state=42)
        assignees = assignees.sample(frac=SAMPLE_FRAC, random_state=42)
        print(f"[Sample mode] Using {SAMPLE_FRAC*100:.1f}% of rows")

    # --- Filter positions: only those inventors we care about
    inventor_ids = inventors["user_id"].drop_duplicates()
    positions = positions.merge(inventor_ids.to_frame(), on="user_id", how="inner")
    print("[Filter] Reduced positions to relevant inventors only")
    print("[OK] Filtered positions rows:", positions.shape[0].compute())

    # --- Inventor + employer
    inv_pos = inventors.merge(positions, on="user_id", how="left")

    # --- Join with assignees on patent_id
    merged = inv_pos.merge(
        assignees,
        on="patent_id",
        how="inner",
        suffixes=("_employer", "_assignee")
    )

    # --- Define match flags
    merged = merged.assign(
        match_rcid = (merged["rcid_employer"] == merged["rcid_assignee"]),
        match_company = (
            merged["company_name_employer"].fillna("").str.lower()
            == merged["company_name_assignee"].fillna("").str.lower()
        )
    )

    # --- Repartition for efficient write
    merged = merged.repartition(partition_size="256MB")

    # --- Save to Parquet
    print(f"[Write] Writing results to {OUTPUT_PATH}")
    merged.to_parquet(
        OUTPUT_PATH,
        write_index=False,
        engine="pyarrow",
        compression="snappy",
        overwrite=True,
    )

    # --- Summary stats
    total_rows, rcid_rate, company_rate = dd.compute(
        merged.shape[0],
        merged["match_rcid"].mean(),
        merged["match_company"].mean()
    )
    print(f"[Summary] Total inventor–patent–assignee links: {total_rows:,}")
    print(f"[Summary] RCID match rate: {rcid_rate:.3f}")
    print(f"[Summary] Company name match rate: {company_rate:.3f}")

if __name__ == "__main__":
    main()
