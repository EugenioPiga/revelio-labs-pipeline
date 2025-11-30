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


def load_inventor(tsv_path: str) -> pd.DataFrame:
    """Load inventor data and assert uniqueness on (patent_id, inventor_id)."""
    cols = ["patent_id", "inventor_id"]
    df = pd.read_csv(
        tsv_path,
        sep="\t",
        usecols=cols,
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )
    duplicates = df.duplicated(subset=cols).sum()
    if duplicates:
        raise ValueError(f"Inventor file has {duplicates:,} duplicate (patent_id, inventor_id) pairs.")
    print(f"[OK] Loaded {len(df):,} inventor rows (unique on patent_id + inventor_id).")
    return df


def load_patent(tsv_path: str) -> pd.DataFrame:
    """Load patent data and assert uniqueness on patent_id."""
    df = pd.read_csv(
        tsv_path,
        sep="\t",
        usecols=["patent_id"],
        dtype=str,
        keep_default_na=False,
        low_memory=False,
    )
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
    missing_patents = (merged["_merge"] == "left_only").sum()
    if missing_patents:
        raise ValueError(f"{missing_patents:,} inventor rows do not have a matching patent_id in g_patent.")
    merged = merged.drop(columns="_merge")
    print(f"[OK] Inventor ↔ patent merge validated ({len(merged):,} rows).")
    return merged


def load_revelio_matches(directory: str) -> pd.DataFrame:
    """Load Revelio inventor matches and assert uniqueness on (patent_id, pv_inventor_id)."""
    shard_paths = sorted(glob(os.path.join(directory, "*.parquet")))
    if not shard_paths:
        raise FileNotFoundError(f"No parquet shards found under {directory}")

    frames = []
    for path in shard_paths:
        part = pd.read_parquet(path, columns=["patent_id", "pv_inventor_id"])
        frames.append(part)
    df = pd.concat(frames, ignore_index=True)

    duplicates = df.duplicated(subset=["patent_id", "pv_inventor_id"]).sum()
    if duplicates:
        raise ValueError(f"Revelio matches contain {duplicates:,} duplicate (patent_id, pv_inventor_id) pairs.")
    print(f"[OK] Loaded {len(df):,} Revelio inventor matches (unique pairs).")
    return df


def compare_matches(step2_df: pd.DataFrame, matches_df: pd.DataFrame) -> None:
    """Merge Step 2 output with Revelio matches and report coverage statistics."""
    combo = step2_df.merge(
        matches_df,
        how="outer",
        left_on=["patent_id", "inventor_id"],
        right_on=["patent_id", "pv_inventor_id"],
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


def main():
    inventor_tsv = ensure_tsv(INVENTOR_ZIP)
    patent_tsv = ensure_tsv(PATENT_ZIP)

    inventor_df = load_inventor(inventor_tsv)
    patent_df = load_patent(patent_tsv)
    inventor_with_patents = merge_inventor_patent(inventor_df, patent_df)

    matches_df = load_revelio_matches(MATCHES_DIR)
    compare_matches(inventor_with_patents, matches_df)


if __name__ == "__main__":
    main()
