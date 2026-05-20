#!/usr/bin/env python
# step2_inventors_matched_positions.py
#
# Reads Step1 output + POSITIONS parquet (dir or glob)
# Broadcast-trims POSITIONS to STEP1 user_ids
# Resolves overlaps (prefer POSITIONS), left-joins, and writes Parquet
# Outputs stats at the end

import os, glob, argparse
import dask.dataframe as dd

try:
    from dask.diagnostics import ProgressBar
    HAS_PBAR = True
except Exception:
    HAS_PBAR = False

OUTPUT_DIR = "output"
FINAL_DIR = os.path.join(OUTPUT_DIR, "inventors_matched_positions")
os.makedirs(OUTPUT_DIR, exist_ok=True)


def list_parquet_paths(root_or_glob: str, limit: int | None):
    """List parquet paths given dir or glob, apply optional limit."""
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

    # --------------------------
    print("[1/6] Loading STEP1 …")
    step1 = dd.read_parquet(os.path.join(args.step1_dir, "*.parquet"),
                            engine="pyarrow").dropna(subset=["user_id"])
    print(f"     -> STEP1 partitions: {step1.npartitions}")

    print("[2/6] Loading POSITIONS …")
    positions = read_parquets_any(args.positions_dir, args.limit_pos).dropna(subset=["user_id"])
    print(f"     -> POSITIONS partitions: {positions.npartitions}")

    print("[3/6] Building user_id set from STEP1 …")
    step1_uids = step1[["user_id"]].drop_duplicates()
    print("     -> collected distinct user_id from STEP1")

    print("[4/6] Filtering POSITIONS by STEP1 user_id …")
    positions_trim = positions.merge(step1_uids, on="user_id", how="inner").assign(_pos_marker=1)

    print("[5/6] Resolving overlapping columns …")
    overlap = (set(step1.columns) & set(positions_trim.columns)) - {"user_id", "_pos_marker"}
    if overlap:
        step1 = step1.drop(columns=list(overlap))

    print("[6/6] Joining and marking …")
    merged = step1.merge(positions_trim, on="user_id", how="left")
    merged = merged.assign(
        has_position=merged["_pos_marker"].fillna(0).astype(bool)
    ).drop(columns=["_pos_marker"])

    # --------------------------
    final_dir = os.environ.get("OUTDIR", FINAL_DIR)
    part_size = os.environ.get("PARTITION_SIZE", args.part_size)
    merged = merged.repartition(partition_size=part_size)

    print(f"[Write] Writing Parquet to: {final_dir}")
    write_kwargs = dict(
        write_index=False,
        engine="pyarrow",
        compression="snappy",
        overwrite=True,
        write_metadata_file=False,
    )
    if HAS_PBAR:
        with ProgressBar():
            merged.to_parquet(final_dir, **write_kwargs)
    else:
        merged.to_parquet(final_dir, **write_kwargs)

    try:
        open(os.path.join(final_dir, "_SUCCESS"), "w").close()
    except Exception:
        pass

    # Compact stats
    try:
        total_rows = merged.map_partitions(len).sum()
        uniq_users = merged["user_id"].nunique() if args.exact_unique else merged["user_id"].nunique_approx()
        have_pos = merged["has_position"].sum()
        total_rows, uniq_users, have_pos = dd.compute(total_rows, uniq_users, have_pos)
        print(f"==> Done. Rows: {total_rows:,} | unique user_id: {uniq_users:,} | "
              f"rows with has_position=True: {have_pos:,}")
        print(f"    Parquet directory: {final_dir}")
    except Exception:
        print("==> Done. (Skipped final count)")


if __name__ == "__main__":
    main()

