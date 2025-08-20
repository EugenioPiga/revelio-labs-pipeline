#!/usr/bin/env python
# step2_inventors_matched_positions.py
# - Reads Step1 output + POSITIONS parquet (dir or glob)
# - Optional limit on number of POSITIONS files
# - Broadcast-trim positions, left-join on user_id
# - No suffixes: overlapping column names prefer POSITIONS
# - Writes Parquet only

import os, glob, argparse
import dask.dataframe as dd

try:
    from dask.diagnostics import ProgressBar
    HAS_PBAR = True
except Exception:
    HAS_PBAR = False

OUTPUT_DIR = "output"
FINAL_DIR  = os.path.join(OUTPUT_DIR, "inventors_matched_positions")
os.makedirs(OUTPUT_DIR, exist_ok=True)

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

def read_parquets_any(root_or_glob: str, limit: int | None):
    files = list_parquet_paths(root_or_glob, limit)
    return dd.read_parquet(files, engine="pyarrow")

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step1-dir", required=True,
                    help="Parquet directory from step1 (e.g., output/inventors_matched_users).")
    ap.add_argument("--positions-dir", required=True,
                    help="Directory or glob for POSITIONS parquet.")
    ap.add_argument("--limit-pos", type=int, default=None,
                    help="Max number of POSITIONS parquet files to read.")
    ap.add_argument("--part-size", default="128MB", help="Output partition size.")
    ap.add_argument("--exact-unique", action="store_true",
                    help="Use exact nunique for reporting (slower).")
    args = ap.parse_args()

    print("[1/6] Loading STEP1 …")
    step1 = dd.read_parquet(os.path.join(args.step1_dir, "*.parquet"), engine="pyarrow").dropna(subset=["user_id"])
    print(f"     -> STEP1 partitions: {step1.npartitions}")

    print("[2/6] Loading POSITIONS …")
    positions = read_parquets_any(args.positions_dir, args.limit_pos).dropna(subset=["user_id"])
    print(f"     -> POSITIONS partitions: {positions.npartitions}")

    print("[3/6] Building USER ID set from STEP1 (broadcast) …")
    uid_set = set(step1["user_id"].drop_duplicates().compute().tolist())
    print(f"     -> unique user_id in STEP1: {len(uid_set):,}")

    print("[4/6] Filtering POSITIONS by STEP1 user_id (no shuffle) …")
    positions_trim = positions[positions["user_id"].isin(uid_set)].assign(_pos_marker=1)

    # Prefer POSITIONS on name collisions: drop overlaps from step1 slice (except key/marker)
    print("     -> Resolving overlapping columns by preferring POSITIONS …")
    s1_cols  = set(step1.columns)
    pos_cols = set(positions_trim.columns)
    overlap  = (s1_cols & pos_cols) - {"user_id", "_pos_marker"}
    if overlap:
        step1 = step1.drop(columns=list(overlap))

    print("[5/6] Merging on user_id (left join) …")
    merged = step1.merge(positions_trim, on="user_id", how="left")
    merged = merged.assign(has_position=merged["_pos_marker"].fillna(0).astype(bool)).drop(columns=["_pos_marker"])

    print(f"[6/6] Writing Parquet to: {FINAL_DIR}")
    merged = merged.repartition(partition_size=args.part_size)
    if HAS_PBAR:
        with ProgressBar():
            merged.to_parquet(
                FINAL_DIR,
                write_index=False,
                engine="pyarrow",
                compression="snappy",
                overwrite=True,
                write_metadata_file=False,
            )
    else:
        merged.to_parquet(
            FINAL_DIR,
            write_index=False,
            engine="pyarrow",
            compression="snappy",
            overwrite=True,
            write_metadata_file=False,
        )

    # Compact stats
    try:
        total_rows = merged.map_partitions(len).sum()
        if args.exact_unique:
            uniq_users = merged["user_id"].nunique()
        else:
            uniq_users = merged["user_id"].nunique_approx()
        have_pos = merged["has_position"].sum()
        total_rows, uniq_users, have_pos = dd.compute(total_rows, uniq_users, have_pos)
        print(f"==> Done. Rows: {total_rows:,} | unique user_id: {uniq_users:,} | rows with has_position=True: {int(have_pos):,}")
        print(f"    Parquet directory: {FINAL_DIR}")
    except Exception:
        print("==> Done. (Skipped final count)")

if __name__ == "__main__":
    main()
