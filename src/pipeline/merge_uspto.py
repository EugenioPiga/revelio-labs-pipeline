import pandas as pd

# File paths (adjust if unzip gave different names)
dir_path = "/labs/khanna/USPTO_202507/"
file_us_cit = dir_path + "g_us_patent_citation.tsv"
file_assignee = dir_path + "g_assignee_disambiguated.tsv"
file_cpc = dir_path + "g_cpc_current.tsv"
file_foreign = dir_path + "g_foreign_citation.tsv"

print("Loading USPTO datasets...")

### --- US Patent Citations ---
us_cit = pd.read_csv(file_us_cit, sep="\t", usecols=["patent_id"])
us_cit_agg = us_cit.groupby("patent_id").size().reset_index(name="n_us_citations")

### --- Assignees (pick main assignee by lowest sequence) ---
assignee = pd.read_csv(file_assignee, sep="\t", usecols=["patent_id", "assignee_sequence", "disambig_assignee_organization"])
assignee_main = assignee.sort_values("assignee_sequence").groupby("patent_id").first().reset_index()
assignee_main = assignee_main.rename(columns={"disambig_assignee_organization": "main_assignee"})

### --- CPC (collapse to class level, one-hot encode) ---
cpc = pd.read_csv(file_cpc, sep="\t", usecols=["patent_id", "cpc_class"])
cpc["cpc_class"] = cpc["cpc_class"].astype(str).str[:1]  # top-level section A–H, Y
cpc_dummies = pd.get_dummies(cpc["cpc_class"], prefix="CPC", sparse=True)
cpc_agg = pd.concat([cpc["patent_id"], cpc_dummies], axis=1).groupby("patent_id").max().reset_index()

### --- Foreign Citations ---
foreign = pd.read_csv(file_foreign, sep="\t", usecols=["patent_id"])
foreign_agg = foreign.groupby("patent_id").size().reset_index(name="n_foreign_citations")

### --- Merge all patent-level aggregates ---
merged = (
    us_cit_agg
    .merge(assignee_main[["patent_id", "main_assignee"]], on="patent_id", how="left")
    .merge(cpc_agg, on="patent_id", how="left")
    .merge(foreign_agg, on="patent_id", how="left")
)

# Fill NaNs
for col in merged.columns:
    if col.startswith("n_") or col.startswith("CPC_"):
        merged[col] = merged[col].fillna(0).astype(int)

outpath = dir_path + "patent_level_dataset.tsv"
merged.to_csv(outpath, sep="\t", index=False)

print(f"Final dataset saved to {outpath}")
print("Shape:", merged.shape)
