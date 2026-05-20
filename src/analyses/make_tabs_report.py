#!/usr/bin/env python
# make_tabs_report_txt.py
#
# Reads the stats parquet and writes:
#   - report/tabs_report.txt  (plain text)
#   - report/tabs_report.md   (markdown for GitHub)

import os
import dask.dataframe as dd

OUT_DIR = "report"
os.makedirs(OUT_DIR, exist_ok=True)

# Map dataset names → actual parquet directories
DATASET_PATHS = {
    "academic_company_ref": "academic_company_ref/*.parquet",
    "academic_individual_position": "academic_individual_position/*.parquet",
    "academic_individual_user_education": "academic_individual_user_education/*.parquet",
    # Add more datasets here as needed
}

# Load stats parquet (summary info)
df = dd.read_parquet("output/tabs_stats").compute()

# ---- Write TXT ----
txt_path = os.path.join(OUT_DIR, "tabs_report.txt")
md_path  = os.path.join(OUT_DIR, "tabs_report.md")

with open(txt_path, "w", encoding="utf-8") as f_txt, open(md_path, "w", encoding="utf-8") as f_md:
    for ds, sub in df.groupby("dataset"):
        # ---------- TXT ----------
        f_txt.write("="*80 + "\n")
        f_txt.write(f"DATASET: {ds}\n")
        f_txt.write("="*80 + "\n\n")

        sub_stats = sub[["column","dtype","n_rows","n_missing","missing_frac","mean","std"]]
        f_txt.write(sub_stats.to_string(index=False))
        f_txt.write("\n\n")

        f_txt.write("---- Example values ----\n")

        # ---------- Markdown ----------
        f_md.write(f"## Dataset: `{ds}`\n\n")
        f_md.write("### Stats\n\n")
        f_md.write(sub_stats.to_markdown(index=False))
        f_md.write("\n\n")
        f_md.write("### Example values\n\n")

        # Load dataset for examples
        ds_path = DATASET_PATHS.get(ds)
        if ds_path is None:
            f_txt.write(f"(No raw path configured for dataset '{ds}')\n\n")
            f_md.write(f"_No raw path configured for dataset `{ds}`_\n\n")
            continue

        try:
            ds_df = dd.read_parquet(ds_path)

            for col in sub["column"]:
                if col in ds_df.columns:
                    try:
                        examples = (
                            ds_df[col]
                            .dropna()
                            .head(5)  # pandas already
                            .tolist()
                        )
                        if examples:
                            ex_str = " | ".join(map(str, examples))
                            f_txt.write(f"{col}: {ex_str}\n")
                            f_md.write(f"- **{col}**: {', '.join(map(str, examples))}\n")
                        else:
                            f_txt.write(f"{col}: (no non-missing examples)\n")
                            f_md.write(f"- **{col}**: (no non-missing examples)\n")
                    except Exception as e:
                        f_txt.write(f"{col}: (error while sampling: {e})\n")
                        f_md.write(f"- **{col}**: (error while sampling: {e})\n")
                else:
                    f_txt.write(f"{col}: (not found in raw dataset)\n")
                    f_md.write(f"- **{col}**: (not found in raw dataset)\n")

        except Exception as e:
            f_txt.write(f"(Error while reading dataset '{ds}': {e})\n")
            f_md.write(f"(Error while reading dataset `{ds}`: {e})\n")

        f_txt.write("\n\n")
        f_md.write("\n\n")

print(f"[OK] wrote TXT to {txt_path}")
print(f"[OK] wrote MD  to {md_path}")
