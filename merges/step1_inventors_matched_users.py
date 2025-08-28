#!/usr/bin/env python
# step1_inventors_matched_users.py
# - Reads USERS and INVENTORS parquet (dirs or globs)
# - Optional limits on number of files read for smoke tests
# - Broadcast-semi-join (no shuffle): keep inventors whose user_id ∈ users
# - Writes Parquet only; keeps ALL columns

import os, glob, argparse
import dask.dataframe as dd

try:
    from dask.diagnostics import ProgressBar
    HAS_PBAR = True
except Exception:
    HAS_PBAR = False

USERS_PATH_DEFAULT     = "academic_individual_user"
INVENTORS_PATH_DEFAULT = "revelio_patents_inventor_matches"
OUTPUT_DIR             = "output"
FINAL_DIR              = os.path.join(OUTPUT_DIR, "inventors_matched_users")

os.makedirs(OUTPUT_DIR, exist_ok=True)

def list_parquet_paths(root_or_glob: str, limit: int | None):
    if os.path.isdir(root_or_glob):
        # accept nested layouts
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

def read_parquets_any(root_or_glob: str, limit: int | None):
    files = list_parquet_paths(root_or_glob, limit)
    return dd.read_parquet(files, engine="pyarrow")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--users", default=USERS_PATH_DEFAULT,
                    help="Directory or glob for USERS parquet (e.g., dir or '.../*.parquet').")
    ap.add_argument("--inventors", default=INVENTORS_PATH_DEFAULT,
                    help="Directory or glob for INVENTORS parquet.")
    ap.add_argument("--limit-users", type=int, default=None,
                    help="Max number of USERS parquet files to read.")
    ap.add_argument("--limit-inv", type=int, default=None,
                    help="Max number of INVENTORS parquet files to read.")
    ap.add_argument("--part-size", default="128MB", help="Output partition size.")
    args = ap.parse_args()

    print("[1/5] Loading USERS …")
    users = read_parquets_any(args.users, args.limit_users).dropna(subset=["user_id"])
    print(f"     -> USERS partitions: {users.npartitions}")

    print("[2/5] Loading INVENTORS …")
    inv = read_parquets_any(args.inventors, args.limit_inv).dropna(subset=["user_id"])
    print(f"     -> INVENTORS partitions: {inv.npartitions}")

    print("[3/5] Building USER ID set (broadcast) …")
    uid_series = users["user_id"].drop_duplicates().compute()
    uid_set = set(uid_series.tolist())
    print(f"     -> unique users in subset: {len(uid_set):,}")

    print("[4/5] Filtering INVENTORS by USERS (no shuffle) …")
    inv_filtered = inv[inv["user_id"].isin(uid_set)]  # keep ALL columns

    print(f"[5/5] Writing Parquet to: {FINAL_DIR}")
    inv_filtered = inv_filtered.repartition(partition_size=args.part_size)
    if HAS_PBAR:
        with ProgressBar():
            inv_filtered.to_parquet(
                FINAL_DIR,
                write_index=False,
                engine="pyarrow",
                compression="snappy",
                overwrite=True,
                write_metadata_file=False,
            )
    else:
        inv_filtered.to_parquet(
            FINAL_DIR,
            write_index=False,
            engine="pyarrow",
            compression="snappy",
            overwrite=True,
            write_metadata_file=False,
        )

    # Compact stats (single compute; all optional)
    try:
        total_rows = inv_filtered.map_partitions(len).sum()
        uniq_users = inv_filtered["user_id"].nunique_approx()
        total_rows, uniq_users = dd.compute(total_rows, uniq_users)
        print(f"==> Done. Rows: {total_rows:,} | approx unique user_id: {uniq_users:,}")
        print(f"    Parquet directory: {FINAL_DIR}")
    except Exception:
        print("==> Done. (Skipped final count)")

if __name__ == "__main__":
    main()
