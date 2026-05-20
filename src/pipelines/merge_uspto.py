import dask.dataframe as dd
import pandas as pd
import argparse

# ----------------------------
# Parse arguments
# ----------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--test", action="store_true", help="Run on 0.1% sample for quick check")
args = parser.parse_args()

# ----------------------------
# File paths
# ----------------------------
dir_path = "/labs/khanna/USPTO_202507/"
file_us_cit = dir_path + "g_us_patent_citation.tsv"
file_assignee = dir_path + "g_assignee_disambiguated.tsv"
file_cpc = dir_path + "g_cpc_current.tsv"
file_foreign = dir_path + "g_foreign_citation.tsv"

frac = 0.001 if args.test else 1.0
print(f"Loading USPTO datasets with Dask... (test={args.test}, frac={frac})")

# ==========================================================
# US Patent Citations
# ==========================================================
print("Aggregating US citations...")
us_cit = dd.read_csv(
    file_us_cit,
    sep="\t",
    blocksize="256MB",
    dtype={
        "patent_id": "object",
        "citation_sequence": "float64",
        "citation_patent_id": "object",
        "citation_date": "object",
        "record_name": "object",
        "wipo_kind": "object",
        "citation_category": "object"
    },
    assume_missing=True
)
if args.test:
    us_cit = us_cit.sample(frac=frac, random_state=42)

us_cit_agg = us_cit.groupby("patent_id").size().to_frame("n_us_citations").reset_index()
us_cit_agg = us_cit_agg.compute()
us_cit_agg.to_parquet(dir_path + "us_cit_agg.parquet")

# ==========================================================
# Assignees
# ==========================================================
print("Aggregating Assignees...")
assignee = dd.read_csv(
    file_assignee,
    sep="\t",
    blocksize="256MB",
    dtype={
        "patent_id": "object",
        "assignee_sequence": "float64",
        "assignee_id": "object",
        "disambig_assignee_individual_name_first": "object",
        "disambig_assignee_individual_name_last": "object",
        "disambig_assignee_organization": "object",
        "assignee_type": "float64",
        "location_id": "object"
    },
    assume_missing=True
)
if args.test:
    assignee = assignee.sample(frac=frac, random_state=42)

assignee_main = assignee.map_partitions(
    lambda df: df.sort_values("assignee_sequence")
).groupby("patent_id").first().reset_index()
assignee_main = assignee_main.rename(
    columns={"disambig_assignee_organization": "main_assignee"}
)[["patent_id", "main_assignee"]]
assignee_main = assignee_main.compute()
assignee_main.to_parquet(dir_path + "assignee_main.parquet")

# ==========================================================
# CPC Current
# ==========================================================
print("Aggregating CPC classes...")
cpc = dd.read_csv(
    file_cpc,
    sep="\t",
    blocksize="256MB",
    dtype={
        "patent_id": "object",
        "cpc_sequence": "float64",
        "cpc_section": "object",
        "cpc_class": "object",
        "cpc_subclass": "object",
        "cpc_group": "object",
        "cpc_type": "object"
    },
    assume_missing=True
)
if args.test:
    cpc = cpc.sample(frac=frac, random_state=42)

# Only keep what we need, then compute in Pandas
cpc_small = cpc[["patent_id", "cpc_class"]].compute()

# Collapse to top-level section (A–H, Y)
cpc_small["cpc_class"] = cpc_small["cpc_class"].astype(str).str[:1]

# One-hot encode in Pandas
cpc_dummies = pd.get_dummies(cpc_small["cpc_class"], prefix="CPC")
cpc_agg = pd.concat([cpc_small["patent_id"], cpc_dummies], axis=1).groupby("patent_id").max().reset_index()

# Save checkpoint
cpc_agg.to_parquet(dir_path + "cpc_agg.parquet")


# ==========================================================
# Foreign Citations
# ==========================================================
print("Aggregating Foreign citations...")
foreign = dd.read_csv(
    file_foreign,
    sep="\t",
    blocksize="256MB",
    dtype={
        "patent_id": "object",
        "citation_sequence": "float64",
        "citation_application_id": "object",
        "citation_date": "object",
        "citation_category": "object",
        "citation_country": "object"
    },
    assume_missing=True
)
if args.test:
    foreign = foreign.sample(frac=frac, random_state=42)

foreign_agg = foreign.groupby("patent_id").size().to_frame("n_foreign_citations").reset_index()
foreign_agg = foreign_agg.compute()
foreign_agg.to_parquet(dir_path + "foreign_agg.parquet")

# ==========================================================
# Final merge in Pandas
# ==========================================================
print("Merging all datasets in Pandas...")
us_cit_agg = pd.read_parquet(dir_path + "us_cit_agg.parquet")
assignee_main = pd.read_parquet(dir_path + "assignee_main.parquet")
cpc_agg = pd.read_parquet(dir_path + "cpc_agg.parquet")
foreign_agg = pd.read_parquet(dir_path + "foreign_agg.parquet")

merged = (
    us_cit_agg
    .merge(assignee_main, on="patent_id", how="left")
    .merge(cpc_agg, on="patent_id", how="left")
    .merge(foreign_agg, on="patent_id", how="left")
)

# Fill NaNs
for col in merged.columns:
    if col.startswith("n_") or col.startswith("CPC_"):
        merged[col] = merged[col].fillna(0).astype(int)

outpath = dir_path + ("patent_level_dataset_TEST.tsv" if args.test else "patent_level_dataset.tsv")
merged.to_csv(outpath, sep="\t", index=False)

print(f"Final dataset saved to {outpath}")
print("Shape:", merged.shape)
