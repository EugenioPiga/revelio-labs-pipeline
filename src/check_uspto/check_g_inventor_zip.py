#!/usr/bin/env python3
"""Utility snippet to quickly sanity-check PatentsView zip files."""

import csv
import io
import os
import zipfile

DATASETS = {
    "g_inventor_disambiguated": "/labs/khanna/USPTO_202511/g_inventor_disambiguated.tsv.zip",
    "g_patent": "/labs/khanna/USPTO_202511/g_patent.tsv.zip",
}


def inspect_zip(zip_path: str, sample_rows: int = 5):
    """
    Validate a PatentsView zip and return a lightweight preview.

    Returns a dict with file metadata plus a handful of TSV rows for visual inspection.
    """
    if not os.path.isfile(zip_path):
        raise FileNotFoundError(f"Zip file not found: {zip_path}")

    with zipfile.ZipFile(zip_path) as zf:
        bad_member = zf.testzip()
        if bad_member:
            raise zipfile.BadZipFile(f"Corrupted member detected: {bad_member}")

        tsv_members = [name for name in zf.namelist() if name.endswith(".tsv")]
        if not tsv_members:
            raise RuntimeError("No TSV payload found inside the zip archive.")
        target = tsv_members[0]

        with zf.open(target) as raw_member:
            wrapper = io.TextIOWrapper(raw_member, encoding="utf-8", newline="")
            reader = csv.reader(wrapper, delimiter="\t")
            header = next(reader)

            rows = []
            for _ in range(sample_rows):
                try:
                    rows.append(next(reader))
                except StopIteration:
                    break

    return {
        "zip_path": zip_path,
        "file_size": os.path.getsize(zip_path),
        "inner_file": target,
        "header": header,
        "sample_rows": rows,
    }


def main():
    for name, path in DATASETS.items():
        print(f"\n[INFO] Checking {name} ...")
        info = inspect_zip(path)
        print(f"[OK] Zip: {info['zip_path']} ({info['file_size']:,} bytes)")
        print(f"[OK] Contained TSV: {info['inner_file']}")
        print(f"[OK] Header ({len(info['header'])} columns): {info['header']}")
        if info["sample_rows"]:
            print("[OK] Sample rows:")
            for idx, row in enumerate(info["sample_rows"], 1):
                print(f"  {idx}: {row}")
        else:
            print("[WARN] No data rows found in TSV (file may be empty).")


if __name__ == "__main__":
    main()
