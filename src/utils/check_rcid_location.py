#!/usr/bin/env python3
# check_rcid_location.py
# RCID vs Location checker (dir/glob or single file). Robust to your Dask version.

import os, glob, argparse, re
from functools import reduce
import dask.dataframe as dd
import pandas as pd

REQ_COLS = ["rcid", "company", "primary_name", "hq_city", "hq_state", "hq_country", "hq_zip_code"]

SUFFIX_PATTERN = re.compile(
    r"\b(inc|inc\.|incorporated|llc|l\.l\.c\.|corp|corporation|co|co\.|ltd|ltd\.|company)\b",
    flags=re.IGNORECASE
)

def list_parquet_paths(root_or_glob: str, limit: int | None):
    if os.path.isdir(root_or_glob):
        paths = []
        for b, _, fs in os.walk(root_or_glob):
            for f in fs:
                if f.endswith(".parquet"):
                    paths.append(os.path.join(b, f))
    else:
        paths = glob.glob(root_or_glob)
    paths.sort()
    if limit is not None:
        paths = paths[:max(0, limit)]
    if not paths:
        raise FileNotFoundError(f"No parquet files found for '{root_or_glob}' with limit={limit}.")
    return paths

def read_parquets_any(root_or_glob: str, limit: int | None, columns=None) -> dd.DataFrame:
    files = list_parquet_paths(root_or_glob, limit)
    first_cols = dd.read_parquet(files[:1], engine="pyarrow").columns
    usecols = [c for c in (columns or []) if c in first_cols] or [c for c in REQ_COLS if c in first_cols]
    return dd.read_parquet(files, engine="pyarrow", columns=usecols)

def normalize_name(s: dd.Series) -> dd.Series:
    s = s.astype("string").str.lower()
    s = s.str.replace(r"[^\w\s]", " ", regex=True)
    s = s.str.replace(SUFFIX_PATTERN, "", regex=True)
    s = s.str.replace(r"\s+", " ", regex=True).str.strip()
    return s

def build_location(df: dd.DataFrame, cols, name_col) -> dd.Series:
    parts = [df[c].astype("string").str.strip().str.lower() for c in cols if c in df.columns]
    if not parts:
        # return an all-NA series aligned to df
        return df[name_col].map_partitions(lambda s: pd.Series(pd.NA, index=s.index))
    if len(parts) == 1:
        loc = parts[0]
    else:
        loc = reduce(lambda a, b: (a.fillna("") + "|" + b.fillna("")), parts)
        loc = loc.str.replace(r"^\|+|\|+$", "", regex=True).str.replace(r"\|{2,}", "|", regex=True)
    return loc.map_partitions(lambda s: s.replace("", pd.NA))

def main():
    ap = argparse.ArgumentParser(description="RCID vs Location checker (dir/glob or single file).")
    ap.add_argument("--path", default=None, help="Single parquet FILE path (optional).")
    ap.add_argument("--company-dir", default=None,
                    help="Directory or glob for academic_company_ref (e.g. '/path/dir' or '/path/dir/company_ref_*.parquet').")
    ap.add_argument("--limit-files", type=int, default=1, help="Max number of files to read from dir/glob.")
    ap.add_argument("--n-examples", type=int, default=12, help="How many company names to include.")
    ap.add_argument("--max-per-name", type=int, default=10, help="Max (rcid,loc) rows per name.")
    ap.add_argument("--output-csv", default="rcid_location_examples.csv", help="CSV to write.")
    args = ap.parse_args()

    # Resolve input
    if args.path:
        if not os.path.exists(args.path):
            raise FileNotFoundError(f"Parquet not found: {args.path}")
        ddf = dd.read_parquet(args.path, engine="pyarrow")
        usecols = [c for c in REQ_COLS if c in ddf.columns]
        ddf = dd.read_parquet(args.path, engine="pyarrow", columns=usecols)
    else:
        if not args.company_dir:
            raise ValueError("Provide either --path OR --company-dir.")
        ddf = read_parquets_any(args.company_dir, args.limit_files, columns=REQ_COLS)

    cols = set(ddf.columns)
    if "rcid" not in cols:
        raise ValueError("Required column 'rcid' not found in the provided parquet(s).")

    name_col = "primary_name" if "primary_name" in cols else ("company" if "company" in cols else None)
    if not name_col:
        raise ValueError("Need 'primary_name' or 'company' column; neither found.")

    loc_cols = [c for c in ["hq_city", "hq_state", "hq_country", "hq_zip_code"] if c in cols]

    # Minimal projection & features
    ddf = ddf[[name_col, "rcid"] + loc_cols].dropna(subset=[name_col, "rcid"])
    ddf = ddf.assign(
        name_norm = normalize_name(ddf[name_col]),
        loc       = build_location(ddf, loc_cols, name_col),
    )

    # Distinct triples to lighten the graph
    triples = ddf[["name_norm", "rcid", "loc"]].drop_duplicates()

    # Counts per normalized name
    n_rcid = triples.groupby("name_norm")["rcid"].nunique().rename("n_rcid")
    # Dask on your cluster doesn't accept dropna=...; just pre-filter nulls:
    loc_non_null = triples[~triples["loc"].isna()]
    n_loc  = loc_non_null.groupby("name_norm")["loc"].nunique().rename("n_loc")

    agg = dd.concat([n_rcid, n_loc], axis=1).fillna(0).compute().sort_values(["n_rcid", "n_loc"], ascending=False)

    cand = agg[(agg["n_rcid"] > 1) & (agg["n_loc"] > 1)]
    if cand.empty:
        print("[INFO] No names with >1 rcid AND >1 location found.")
        pd.DataFrame(columns=["name","name_norm","rcid","loc"]).to_csv(args.output_csv, index=False)
        return

    chosen = cand.head(args.n_examples).index.tolist()

    # Representative original spelling per name_norm
    names_map = (
        ddf[["name_norm", name_col]]
        .dropna().drop_duplicates().compute()
        .rename(columns={name_col: "name"})
        .groupby("name_norm", as_index=False).head(1)
    )
    distinct = triples.drop_duplicates().compute()

    out = (
        distinct[distinct["name_norm"].isin(chosen)]
        .merge(names_map, on="name_norm", how="left")
        .sort_values(["name_norm", "rcid", "loc"])
        .groupby("name_norm", group_keys=False).head(args.max_per_name)
        [["name", "name_norm", "rcid", "loc"]]
    )

    out.to_csv(args.output_csv, index=False)
    print(f"[OK] Wrote examples to {os.path.abspath(args.output_csv)}")
    print(f"[INFO] Candidates found: {len(cand):,} (showing {min(len(chosen), len(cand))})")
    print(out.head(20).to_string(index=False))

if __name__ == "__main__":
    main()
