#!/usr/bin/env python
# tabs_stats_examples.py
#
# Compute summary stats for each dataset, with up to 5 example values per column.

import os
import dask.dataframe as dd
import pandas as pd
import argparse

def compute_column_stats(df, dataset_name, col):
    """Compute stats + examples for a single column safely."""
    s = df[col]

    out = {
        "dataset": dataset_name,
        "column": col,
        "dtype": str(s.dtype),
        "n_rows": len(s),
    }

    try:
        out["n_missing"] = s.isna().sum().compute()
        out["missing_frac"] = out["n_missing"] / out["n_rows"]
    except Exception:
        out["n_missing"] = None
        out["missing_frac"] = None

    if pd.api.types.is_numeric_dtype(s.dtype):
        try:
            mean, std = dd.compute(s.mean(), s.std())
            out["mean"], out["std"] = mean, std
        except Exception:
            out["mean"], out["std"] = None, None
    else:
        out["mean"], out["std"] = None, None

    # Get up to 5 example non-missing values
    try:
        examples = (
            s.dropna()
             .drop_duplicates()
             .head(5)
             .compute()
             .tolist()
        )
        out["examples"] = [str(x) for x in examples]
    except Exception:
        out["examples"] = []

    return out


def process_dataset(path, dataset_name, limit_cols=None):
    print(f"=== Processing {dataset_name} ===")
    df = dd.read_parquet(path)

    cols = [c for c in df.columns if (not limit_cols or c in limit_cols)]

    results = []
    for col in cols:
        print(f"  -> Column {col}")
        stats = compute_column_stats(df, dataset_name, col)
        results.append(stats)
    return results


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default=".", help="Base dir with parquet datasets")
    ap.add_argument("--out", default="report/tabs_company_ref.parquet",
                    help="Output file (parquet)")
    args = ap.parse_args()

    datasets = {
        "academic_company_ref": os.path.join(args.base, "academic_company_ref/*.parquet"),
        "academic_individual_position": os.path.join(args.base, "academic_individual_position/*.parquet"),
        "academic_individual_user": os.path.join(args.base, "academic_individual_user/*.parquet"),
        "academic_individual_user_education": os.path.join(args.base, "academic_individual_user_education/*.parquet"),
        "academic_individual_user_skill": os.path.join(args.base, "academic_individual_user_skill/*.parquet"),
        "revelio_patents_assignee_matches": os.path.join(args.base, "revelio_patents_assignee_matches/*.parquet"),
        "revelio_patents_inventor_matches": os.path.join(args.base, "revelio_patents_inventor_matches/*.parquet"),
    }

    all_results = []
    for ds_name, path in datasets.items():
        try:
            res = process_dataset(path, ds_name)
            all_results.extend(res)
        except Exception as e:
            print(f"⚠️ Skipping {ds_name}: {e}")

    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df_out = pd.DataFrame(all_results)
    df_out.to_parquet(args.out, index=False)
    print(f"[OK] Wrote stats+examples to {args.out}")


if __name__ == "__main__":
    main()
