#!/usr/bin/env python
# tabs_stats.py
# Computes per-column missing counts for ALL columns and mean/std for numeric columns
# across multiple Parquet datasets located under the current repo directory.
#
# Outputs a compact Parquet table to ./output/tabs_stats

import argparse, os, math
import pandas as pd
import dask.dataframe as dd
from pandas.api.types import is_numeric_dtype

try:
    from dask.diagnostics import ProgressBar
    HAS_PBAR = True
except Exception:
    HAS_PBAR = False

DEFAULT_DATASETS = [
    "academic_company_ref",
    "academic_individual_position",
    "academic_individual_user",
    "academic_individual_user_education",
    "academic_individual_user_skill",
    "revelio_patents_assignee_matches",
    "revelio_patents_inventor_matches",
]

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def safe_read_parquet(path: str):
    if not os.path.exists(path):
        print(f"[WARN] missing dataset dir: {path}")
        return None
    try:
        # gather_statistics=False avoids expensive metadata scans on large dir trees
        return dd.read_parquet(path, engine="pyarrow", gather_statistics=False)
    except Exception as e:
        print(f"[WARN] failed to read {path}: {e}")
        return None

def stats_for_dataset(name: str, path: str, chunk_cols: int = 48) -> pd.DataFrame:
    ddf = safe_read_parquet(path)
    if ddf is None:
        return pd.DataFrame(columns=["dataset","column","dtype","n_rows","n_missing","missing_frac","mean","std"])

    # total rows (lazy -> compute)
    n_rows = int(ddf.shape[0].compute())

    dtypes = ddf.dtypes.to_dict()
    cols = list(dtypes.keys())
    num_cols = [c for c, dt in dtypes.items() if is_numeric_dtype(dt)]

    results = []

    # Missing counts for ALL columns (batched)
    for grp in chunked(cols, chunk_cols):
        miss = ddf[grp].isna().sum().compute()
        for c in grp:
            n_miss = int(miss.get(c, 0))
            results.append({
                "dataset": name,
                "column": c,
                "dtype": str(dtypes[c]),
                "n_rows": int(n_rows),
                "n_missing": n_miss,
                "missing_frac": (n_miss / n_rows) if n_rows else math.nan,
                "mean": math.nan,
                "std": math.nan,
            })

    # Mean/std for NUMERIC columns (batched)
    mean_map, std_map = {}, {}
    for grp in chunked(num_cols, chunk_cols):
        sub = ddf[grp]
        m = sub.mean().compute()
        s = sub.std().compute()
        mean_map.update({k: (float(v) if v == v else math.nan) for k, v in m.items()})  # v==v filters NaN
        std_map.update({k: (float(v) if v == v else math.nan) for k, v in s.items()})

    # Fill back mean/std
    for row in results:
        c = row["column"]
        if c in mean_map:
            row["mean"] = mean_map[c]
            row["std"]  = std_map.get(c, math.nan)

    return pd.DataFrame(results)

def main():
    ap = argparse.ArgumentParser(description="Compute per-column stats for multiple Parquet datasets.")
    ap.add_argument("--base", default=".", help="Base directory (default: current repo).")
    ap.add_argument("--datasets", nargs="*", default=DEFAULT_DATASETS,
                    help="Dataset folder names under --base.")
    ap.add_argument("--out", default="output/tabs_stats", help="Output Parquet directory.")
    ap.add_argument("--chunk-cols", type=int, default=48, help="Columns per batch.")
    args = ap.parse_args()

    os.makedirs(args.out, exist_ok=True)

    frames = []
    for ds in args.datasets:
        path = os.path.join(args.base, ds)
        print(f"[INFO] scanning {ds} -> {path}")
        frames.append(stats_for_dataset(ds, path, chunk_cols=args.chunk_cols))

    out = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()
    if out.empty:
        print("[WARN] no stats produced (no datasets found or all failed to load).")
        return

    ddf_out = dd.from_pandas(out, npartitions=max(1, len(out)//5000))
    if HAS_PBAR:
        with ProgressBar():
            ddf_out.to_parquet(args.out, engine="pyarrow", compression="snappy",
                               write_index=False, write_metadata_file=False, overwrite=True)
    else:
        ddf_out.to_parquet(args.out, engine="pyarrow", compression="snappy",
                           write_index=False, write_metadata_file=False, overwrite=True)

    print(f"[OK] wrote stats parquet to: {args.out}")
    # small preview
    print(out.head(10).to_string(index=False))

if __name__ == "__main__":
    main()
