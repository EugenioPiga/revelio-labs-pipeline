
#!/usr/bin/env python
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from glob import glob

# === CONFIG ===
DATA_PATH = "revelio_patents_assignee_matches/*.parquet"
OUTPUT_DIR = "output"
TOP_N = 10  # for concentration index

# === PREP OUTPUT FOLDER ===
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("Reading data...")
files = glob(DATA_PATH)
df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)

# === CLEANING ===
df["company_name"] = df["company_name"].astype(str).str.strip()
df["patent_date"] = pd.to_datetime(df["patent_date"], errors="coerce")
df["patent_year"] = df["patent_date"].dt.year
df["assignee_sequence"] = pd.to_numeric(df["assignee_sequence"], errors="coerce")

# Remove duplicate company–patent pairs
dfu = df.drop_duplicates(subset=["company_name", "patent_id"]).copy()

# === COMPANY-LEVEL STATS ===
company_stats = (
    dfu.groupby("company_name")
       .agg(
           n_patents=("patent_id", "nunique"),
           first_patent_year=("patent_year", "min"),
           last_patent_year=("patent_year", "max"),
           activity_span=("patent_year", lambda x: (x.max() - x.min()) if len(x) else pd.NA),
           avg_assignee_seq=("assignee_sequence", "mean"),
           n_unique_titles=("patent_title", "nunique")
       )
       .reset_index()
)

company_stats["share_unique_titles"] = (
    company_stats["n_unique_titles"] / company_stats["n_patents"]
)

# === PER-YEAR PATENT COUNTS ===
per_year = (
    dfu.groupby(["company_name", "patent_year"], dropna=True)
       .agg(yearly_patents=("patent_id", "nunique"))
       .reset_index()
)

# === RECENT MOMENTUM ===
latest_year = per_year["patent_year"].max()
trend = (
    per_year[per_year["patent_year"].isin([latest_year, latest_year - 1])]
      .pivot(index="company_name", columns="patent_year", values="yearly_patents")
      .rename(columns={
          latest_year: "patents_last_year",
          latest_year - 1: "patents_prev_year"
      })
      .reset_index()
)

for col in ["patents_last_year", "patents_prev_year"]:
    trend[col] = trend[col].fillna(0).astype("int64")

prev = trend["patents_prev_year"].to_numpy()
last = trend["patents_last_year"].to_numpy()
trend["pct_change"] = np.where(prev > 0, (last - prev) / prev * 100.0, np.nan)

# Merge
final_stats = company_stats.merge(trend, on="company_name", how="left")

# === CONCENTRATION INDEX ===
total_patents = final_stats["n_patents"].sum()
top_patents = final_stats.nlargest(TOP_N, "n_patents")["n_patents"].sum()
concentration_index = top_patents / total_patents if total_patents > 0 else np.nan

# === INTERESTING HIGHLIGHTS ===
first_year_all = per_year["patent_year"].min()
last_year_all = per_year["patent_year"].max()
patents_first_year = per_year.loc[per_year["patent_year"] == first_year_all, "yearly_patents"].sum()
patents_last_year = per_year.loc[per_year["patent_year"] == last_year_all, "yearly_patents"].sum()
growth_pct = ((patents_last_year - patents_first_year) / patents_first_year * 100
              if patents_first_year else None)

accelerators = (final_stats
                .query("patents_prev_year >= 10")
                .sort_values("pct_change", ascending=False)
                .head(5))

persistent = (final_stats
              .sort_values("activity_span", ascending=False)
              .head(5))

# === SAVE DATASETS ===
final_stats.to_csv(os.path.join(OUTPUT_DIR, "company_patent_stats.csv"), index=False)
final_stats.to_parquet(os.path.join(OUTPUT_DIR, "company_patent_stats.parquet"), index=False)
per_year.to_csv(os.path.join(OUTPUT_DIR, "company_patents_per_year.csv"), index=False)

# === PLOTS ===
# 1. Top 10 companies yearly trend
top_companies = final_stats.nlargest(10, "n_patents")["company_name"].tolist()
plot_data = per_year[per_year["company_name"].isin(top_companies)]
plt.figure(figsize=(12, 6))
for company in top_companies:
    cdata = plot_data[plot_data["company_name"] == company]
    plt.plot(cdata["patent_year"], cdata["yearly_patents"], marker='o', label=company)
plt.xlabel("Year")
plt.ylabel("Number of Patents")
plt.title("Yearly Patent Counts – Top 10 Companies")
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "top10_patent_trends.png"), dpi=300)
plt.close()

# 2. Cumulative patents over time
cum_data = (per_year.groupby("patent_year")["yearly_patents"].sum()
            .sort_index()
            .cumsum()
            .reset_index())
plt.figure(figsize=(10, 5))
plt.plot(cum_data["patent_year"], cum_data["yearly_patents"], marker='o')
plt.xlabel("Year")
plt.ylabel("Cumulative Patents")
plt.title("Cumulative Patents Over Time (All Companies)")
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "cumulative_patents.png"), dpi=300)
plt.close()

# 3. Concentration over time (share of top N firms)
shares = []
for year in sorted(per_year["patent_year"].dropna().unique()):
    year_data = per_year[per_year["patent_year"] == year]
    top_share = (year_data.nlargest(TOP_N, "yearly_patents")["yearly_patents"].sum() /
                 year_data["yearly_patents"].sum())
    shares.append((year, top_share))
shares_df = pd.DataFrame(shares, columns=["year", "top_share"])
plt.figure(figsize=(10, 5))
plt.plot(shares_df["year"], shares_df["top_share"], marker='o')
plt.xlabel("Year")
plt.ylabel(f"Share of Patents by Top {TOP_N} Firms")
plt.title(f"Concentration of Innovation Over Time (Top {TOP_N} Firms)")
plt.grid(True)
plt.tight_layout()
plt.savefig(os.path.join(OUTPUT_DIR, "concentration_over_time.png"), dpi=300)
plt.close()

# === TEXT SUMMARY ===
summary_lines = [
    f"Total patents: {total_patents:,}",
    f"Coverage: {first_year_all}–{last_year_all}",
    f"Total yearly patents grew from {patents_first_year} to {patents_last_year} "
    f"({growth_pct:.1f}% change)",
    f"Concentration index (top {TOP_N} companies share of patents): {concentration_index:.2%}",
    "\nTop 5 fastest-growing companies (YoY % change):",
    accelerators[["company_name", "patents_prev_year", "patents_last_year", "pct_change"]]
    .to_string(index=False),
    "\nTop 5 most persistent innovators:",
    persistent[["company_name", "first_patent_year", "last_patent_year", "activity_span"]]
    .to_string(index=False)
]
with open(os.path.join(OUTPUT_DIR, "summary.txt"), "w") as f:
    f.write("\n".join(summary_lines))

print("Analysis complete. Results saved in 'output' folder.")
