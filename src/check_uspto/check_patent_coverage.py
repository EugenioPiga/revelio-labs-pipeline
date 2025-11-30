#!/usr/bin/env python3
"""Check coverage between PatentsView inventor data and Revelio inventor matches."""

import os
import shutil
import zipfile
from glob import glob

import pandas as pd

USPTO_DIR = "/labs/khanna/USPTO_202511"
INVENTOR_ZIP = os.path.join(USPTO_DIR, "g_inventor_disambiguated.tsv.zip")
PATENT_ZIP = os.path.join(USPTO_DIR, "g_patent.tsv.zip")
MATCHES_DIR = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
COVERAGE_OUTPUT = os.path.expanduser(
    "~/code/revelio-labs-pipeline/data/revelio_patents_inventor_matches_with_USPTO.parquet"
)

def ensure_tsv(zip_path: str) -> str:
    """
    Extract a PatentsView TSV once and return its path.

    If the TSV already exists, we reuse it to avoid re-extraction.
    """
    tsv_path, ext = os.path.splitext(zip_path)
    if ext.lower() != ".zip":
        raise ValueError(f"Expected a .zip file, got: {zip_path}")
    if os.path.exists(tsv_path):
        print(f"[SKIP] {tsv_path} already exists.")
        return tsv_path
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Zip archive missing: {zip_path}")
    print(f"[INFO] Extracting {zip_path} ...")
    with zipfile.ZipFile(zip_path) as zf:
        members = [name for name in zf.namelist() if name.endswith(".tsv")]
        if not members:
            raise RuntimeError(f"No TSV member found inside {zip_path}")
        member = members[0]
        with zf.open(member) as src, open(tsv_path, "wb") as dst:
            shutil.copyfileobj(src, dst)
    print(f"[DONE] Saved TSV to {tsv_path}")
    return tsv_path


def ensure_parquet(tsv_path: str) -> tuple[str, pd.DataFrame | None]:
    """Convert a TSV to parquet once (keeping all columns) and return the parquet path."""
    base, ext = os.path.splitext(tsv_path)
    if ext.lower() != ".tsv":
        raise ValueError(f"Expected a .tsv file, got: {tsv_path}")
    parquet_path = f"{base}.parquet"
    if os.path.exists(parquet_path):
        print(f"[SKIP] {parquet_path} already exists.")
        return parquet_path, None
    print(f"[INFO] Converting {tsv_path} to parquet ...")
    df = pd.read_csv(
        tsv_path,
        sep="\t",
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )
    df.to_parquet(parquet_path, index=False)
    print(f"[DONE] Saved parquet to {parquet_path}")
    return parquet_path, df


def load_inventor(parquet_path: str, cached_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Load inventor data (all columns) and assert uniqueness on (patent_id, inventor_sequence)."""
    if cached_df is not None:
        df = cached_df[["patent_id", "inventor_id", "inventor_sequence"]].copy()
    else:
        df = pd.read_parquet(parquet_path, columns=["patent_id", "inventor_id", "inventor_sequence"])
    duplicates = df.duplicated(subset=["patent_id", "inventor_sequence"]).sum()
    if duplicates:
        print(
            f"[WARN] Inventor file has {duplicates:,} duplicate (patent_id, inventor_sequence) pairs "
            f"out of {len(df):,} rows. Dropping duplicates."
        )
        df = df.drop_duplicates(subset=["patent_id", "inventor_sequence"])
    print(f"[OK] Loaded {len(df):,} inventor rows (unique on patent_id + inventor_sequence).")
    return df


def load_patent(parquet_path: str, cached_df: pd.DataFrame | None = None) -> pd.DataFrame:
    """Load patent data and assert uniqueness on patent_id."""
    if cached_df is not None:
        df = cached_df[["patent_id", "patent_date"]].copy()
    else:
        df = pd.read_parquet(parquet_path, columns=["patent_id", "patent_date"])
    duplicates = df.duplicated(subset=["patent_id"]).sum()
    if duplicates:
        raise ValueError(f"Patent file has {duplicates:,} duplicate patent_id values.")
    print(f"[OK] Loaded {len(df):,} patent rows (unique on patent_id).")
    return df


def merge_inventor_patent(inventor_df: pd.DataFrame, patent_df: pd.DataFrame) -> pd.DataFrame:
    """Merge g_inventor_disambiguated with g_patent and ensure many-to-one join."""
    merged = inventor_df.merge(
        patent_df,
        how="left",
        on="patent_id",
        validate="many_to_one",
        indicator=True,
    )
    left_only = (merged["_merge"] == "left_only").sum()
    right_only = (~patent_df["patent_id"].isin(inventor_df["patent_id"])).sum()
    both = len(merged) - left_only
    print(f"[STATS] Inventor rows without patent match: {left_only:,}")
    print(f"[STATS] Patent rows without inventor match: {right_only:,}")
    print(f"[STATS] Rows present in both: {both:,}")
    if left_only:
        raise ValueError(f"{left_only:,} inventor rows do not have a matching patent_id in g_patent.")
    merged = merged.drop(columns="_merge")
    print(f"[OK] Inventor ↔ patent merge validated ({len(merged):,} rows).")
    return merged


def load_revelio_matches(directory: str) -> pd.DataFrame:
    """Load Revelio inventor matches and assert uniqueness on (patent_id, inventor_sequence)."""
    shard_paths = sorted(glob(os.path.join(directory, "*.parquet")))
    if not shard_paths:
        raise FileNotFoundError(f"No parquet shards found under {directory}")
    frames = []
    for path in shard_paths:
        part = pd.read_parquet(
            path,
            columns=[
                "patent_id",
                "patent_date",
                "filing_date",
                "inventor_sequence",
                "pv_inventor_id",
                "user_id",
            ],
        )
        part = part.rename(columns={"patent_date": "pv_patent_date"})
        frames.append(part)
    df = pd.concat(frames, ignore_index=True)
    duplicates = df.duplicated(subset=["patent_id", "inventor_sequence"]).sum()
    if duplicates:
        print(
            f"[WARN] Revelio matches contain {duplicates:,} duplicate (patent_id, inventor_sequence) pairs "
            f"out of {len(df):,} rows. Dropping duplicates."
        )
        df = df.drop_duplicates(subset=["patent_id", "inventor_sequence"])
    print(f"[OK] Loaded {len(df):,} Revelio inventor matches (unique on patent_id + inventor_sequence).")
    return df


def merge_revelio_uspto(step2_df: pd.DataFrame, matches_df: pd.DataFrame, output_path: str | None = None) -> pd.DataFrame:
    """Merge Step 2 output with Revelio matches, report stats, and optionally persist output."""
    combo = step2_df.merge(
        matches_df,
        how="outer",
        left_on=["patent_id", "inventor_sequence"],
        right_on=["patent_id", "inventor_sequence"],
        validate="one_to_one",
        indicator=True,
    )
    counts = combo["_merge"].value_counts()
    left_only = counts.get("left_only", 0)
    right_only = counts.get("right_only", 0)
    both = counts.get("both", 0)
    print(f"[STATS] PatentsView-only inventor rows: {left_only:,}")
    print(f"[STATS] Revelio-only inventor rows: {right_only:,}")
    print(f"[STATS] Rows present in both: {both:,}")
    if right_only > 0:
        raise ValueError(
            "Revelio matches are not a subset of PatentsView inventors "
            f"(missing {right_only:,} rows on the right)."
        )
    if len(matches_df) >= len(step2_df):
        raise ValueError(
            "Revelio matches are not a strict subset of PatentsView inventors "
            f"({len(matches_df):,} >= {len(step2_df):,})."
        )
    print("[OK] Revelio inventor matches form a strict subset of PatentsView inventor pairs.")
    if output_path:
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        combo.to_parquet(output_path, index=False)
        print(f"[DONE] Saved merged coverage snapshot to {output_path}")
    return combo


def main():
    inventor_tsv = ensure_tsv(INVENTOR_ZIP)
    inventor_parquet, inventor_cached_df = ensure_parquet(inventor_tsv)
    patent_tsv = ensure_tsv(PATENT_ZIP)
    patent_parquet, patent_cached_df = ensure_parquet(patent_tsv)

    inventor_df = load_inventor(inventor_parquet, cached_df=inventor_cached_df)
    patent_df = load_patent(patent_parquet, cached_df=patent_cached_df)
    inventor_with_patents = merge_inventor_patent(inventor_df, patent_df)

    revelio_df = load_revelio_matches(MATCHES_DIR)
    coverage_df = merge_revelio_uspto(inventor_with_patents, revelio_df, output_path=COVERAGE_OUTPUT)
    print(
        f"[INFO] Materialized coverage DataFrame with {len(coverage_df):,} rows "
        f"and wrote it to {COVERAGE_OUTPUT}"
    )


if __name__ == "__main__":
    main()
