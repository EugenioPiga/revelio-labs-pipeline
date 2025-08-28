#!/usr/bin/env python
# step3_inventors_matched_education.py
#
# Inputs:
#   --step2-dir        Parquet dir from step2 (inventors ∩ users, possibly with positions)
#   --education-dir    Root directory OR glob for education parquet files
# Output:
#   output/inventors_matched_education (Parquet only)
# Behavior:
#   - has_education flag via broadcast-set (no shuffle)
#   - join education details ONLY where has_education=True
#   - keep all columns; if names overlap, education columns take precedence
#   - no CSV, no JSON

import os
import argparse
import glob
import dask.dataframe as dd

try:
    from dask.diagnostics import ProgressBar
    HAS_PBAR = True
except Exception:
    HAS_PBAR = False

OUTPUT_DIR = "output"
FINAL_DIR  = os.path.join(OUTPUT_DIR, "inventors_matched_education")

# If you want to restrict columns pulled from education, set a list here; None = all columns
EDU_COLS_FOR_JOIN = None   # e.g., ["user_id","degree","field","institution","startdate","enddate"]

def list_parquet_recursive(root_or_glob: str, limit: int | None):
    """Return a sorted list of .parquet paths. Accepts a directory or a glob pattern."""
    paths = []
    if os.path.isdir(root_or_glob):
        for base, _, files in os.walk(root_or_glob):
            for f in files:
                if f.endswith(".parquet"):
                    paths.append(os.path.join(base, f))
    else:
        paths = glob.glob(root_or_glob)
    paths.sort()
    if limit is not None:
        paths = paths[:max(0, limit)]
    if not paths:
        raise FileNotFoundError(f"No parquet files found for '{root_or_glob}' with limit={limit}.")
    return paths

def read_step2(step2_dir: str) -> dd.DataFrame:
    df = dd.read_parquet(step2_dir, columns=None, engine="pyarrow", assume_missing=True)
    return df.assign(user_id=df["user_id"].astype("string").str.strip()).dropna(subset=["user_id"])

def read_edu_ids(file_list: list[str]) -> set:
    edu_ids_dd = dd.read_parquet(
        file_list,
        columns=["user_id"],
        engine="pyarrow",
        dtype={"user_id": "string"},
        assume_missing=True,
        aggregate_files=True,
        split_row_groups=True,
    )
    edu_ids_dd = (edu_ids_dd
                  .assign(user_id=edu_ids_dd["user_id"].astype("string").str.strip())
                  .dropna(subset=["user_id"])
                  .drop_duplicates(subset=["user_id"]))
    return set(edu_ids_dd["user_id"].compute().tolist())

def read_edu_details(file_list: list[str], cols_for_join):
    if cols_for_join is not None and "user_id" not in cols_for_join:
        cols_for_join = ["user_id"] + list(cols_for_join)
    edu = dd.read_parquet(
        file_list,
        columns=cols_for_join,   # None = all columns
        engine="pyarrow",
        dtype={"user_id": "string"},
        assume_missing=True,
        aggregate_files=True,
        split_row_groups=True,
    )
    return edu.assign(user_id=edu["user_id"].astype("string").str.strip()).dropna(subset=["user_id"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--step2-dir", required=True, help="Parquet directory from step2.")
    ap.add_argument("--education-dir", required=True,
                    help="Education root directory OR glob, e.g. academic_individual_user_education/ or '.../*.parquet'")
    ap.add_argument("--limit-edu", type=int, default=10,
                    help="Limit number of education parquet files for a quick test. Use a large value or omit for full run.")
    ap.add_argument("--part-size", default="128MB", help="Output partition size (e.g., 64MB, 128MB, 256MB).")
    ap.add_argument("--exact-unique", action="store_true",
                    help="Use exact nunique for reporting (slower). Default uses approximate.")
    args = ap.parse_args()

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    print("==> Step 3: flag has_education and attach details efficiently (no CSV/JSON)")
    print(f"Step2 dir     : {args.step2_dir}")
    print(f"Education dir : {args.education_dir}")
    print(f"Edu limit     : {args.limit_edu}")

    # [1/6] Load step2
    print("[1/6] Reading Step2 …")
    step2 = read_step2(args.step2_dir)
    print(f"     -> Step2 partitions: {step2.npartitions}")

    # [2/6] Locate education parquet files (limited for smoke tests)
    print("[2/6] Scanning education files …")
    edu_files = list_parquet_recursive(args.education_dir, args.limit_edu)
    print(f"     -> Using {len(edu_files)} education file(s)")
    for p in edu_files[:5]:
        print(f"        - {p}")

    # [3/6] Broadcast-set for has_education (no shuffle)
    print("[3/6] Building education user_id set …")
    edu_id_set = read_edu_ids(edu_files)
    print(f"     -> Unique user_id with education in loaded files: {len(edu_id_set):,}")

    # Add flag
    step2_flagged = step2.assign(has_education=step2["user_id"].isin(edu_id_set))

    # [4/6] Split into needs_details vs rest
    print("[4/6] Splitting rows by has_education …")
    needs_details = step2_flagged[step2_flagged["has_education"]]
    rest          = step2_flagged[~step2_flagged["has_education"]]

    try:
        need_count = needs_details.map_partitions(len).sum().compute()
    except Exception:
        need_count = 0
    print(f"     -> Rows needing education details: {need_count:,}")

    if need_count == 0:
        # Nothing to join; just carry the flag
        final = step2_flagged
    else:
        # [5/6] Read education details and merge ONLY for needed rows
        print("[5/6] Reading education details and merging …")
        edu_details = read_edu_details(edu_files, EDU_COLS_FOR_JOIN).assign(_edu_marker=1)

        # Prefer EDUCATION columns on name collisions: drop overlaps (except key) from step2 slice
        needs_cols = set(needs_details.columns)
        edu_cols   = set(edu_details.columns)
        overlap    = (needs_cols & edu_cols) - {"user_id", "_edu_marker"}
        if overlap:
            needs_details = needs_details.drop(columns=list(overlap))

        enriched = needs_details.merge(edu_details, on="user_id", how="left")

        # Derive has_education from marker (robust when details exist)
        enriched = enriched.assign(
            has_education=enriched["_edu_marker"].fillna(0).astype(bool)
        ).drop(columns=["_edu_marker"])

        # Align schemas and concat back
        final_cols = list(enriched.columns)
        add_to_rest = [c for c in final_cols if c not in rest.columns]
        if add_to_rest:
            rest = rest.assign(**{c: None for c in add_to_rest})
        rest = rest[final_cols]
        final = dd.concat([enriched, rest], axis=0, interleave_partitions=True)

    # [6/6] Normalize dtypes → write parquet
    print("[6/6] Normalizing dtypes and writing Parquet …")
    # Make common edu-like fields strings if present
    force_string_cols = [
        "degree","degree_level","field","major","minor",
        "institution","institution_cleaned","institution_country",
        "startdate","enddate","graduation_date",
        "gpa","honors","description",
        "education_id","program","department",
        "city","state","country","campus",
        "source","source_id","verification_status",
        # IDs / numbers frequently parsed oddly
        "rsid","education_number","ultimate_parent_rsid",
        # raw fields that can be mixed type
        "university_raw","university_name","degree_raw","field_raw",
        "university_country","university_location","ultimate_parent_school_name",
        # any possible leftovers from earlier merges
        "startdate_x","enddate_x","description_x","startdate_y","enddate_y","description_y",
    ]
    present = [c for c in force_string_cols if c in final.columns]
    if present:
        final = final.assign(**{c: final[c].astype("string") for c in present})

    if "user_id" in final.columns:
        final = final.assign(user_id=final["user_id"].astype("string"))
    if "has_education" in final.columns:
        final = final.assign(has_education=final["has_education"].astype("boolean"))

    # Fix null-typed columns (can occur if a partition is all-NA)
    try:
        nullish = [col for col, dt in final.dtypes.items() if str(dt).lower() in ("null","unknown")]
        if nullish:
            final = final.assign(**{c: final[c].astype("string") for c in nullish})
    except Exception:
        pass

    # Repartition and write
    final = final.repartition(partition_size=args.part_size)
    if HAS_PBAR:
        with ProgressBar():
            final.to_parquet(
                FINAL_DIR,
                write_index=False,
                engine="pyarrow",
                compression="snappy",
                overwrite=True,
                write_metadata_file=False,
            )
    else:
        final.to_parquet(
            FINAL_DIR,
            write_index=False,
            engine="pyarrow",
            compression="snappy",
            overwrite=True,
            write_metadata_file=False,
        )

    # Optional: compact stats (single compute)
    try:
        total_rows = final.map_partitions(len).sum()
        uniq_users = final["user_id"].nunique() if args.exact-unique else final["user_id"].nunique_approx()
        have_edu   = final["has_education"].sum()
        total_rows, uniq_users, have_edu = dd.compute(total_rows, uniq_users, have_edu)
        print(f"==> Done. Rows: {total_rows:,} | unique user_id: {uniq_users:,} | rows with has_education=True: {int(have_edu):,}")
        print(f"    Parquet directory: {FINAL_DIR}")
    except Exception:
        print("==> Done. (Skipped final count)")

if __name__ == "__main__":
    main()
