#!/usr/bin/env python3
import pandas as pd
import dask.dataframe as dd
from dask.diagnostics import ProgressBar

# =========================
# CONFIG
# =========================
IN_GLOB = "/labs/khanna/compustat_202601/compustat/funda/funda_*.parquet"
OUT_DIR = "/labs/khanna/compustat_202601/funda_panel.parquet"  # output dataset dir (no year= partitions)

MAKE_UNIQUE_GVKEY_YEAR = True   # shuffle + keep latest datadate per (gvkey,year)
# =========================


def _find_col(cols, target: str):
    t = target.lower()
    for c in cols:
        if c.lower() == t:
            return c
    return None


def _dedupe_keep_latest(pdf: pd.DataFrame) -> pd.DataFrame:
    # After shuffle, all rows for a given (gvkey,year) are co-located
    if "datadate" in pdf.columns:
        pdf = pdf.sort_values("datadate")
    return pdf.drop_duplicates(subset=["gvkey", "year"], keep="last")


def main():
    print(f"[INFO] Reading: {IN_GLOB}")
    ddf = dd.read_parquet(IN_GLOB, engine="pyarrow")

    cols = list(ddf.columns)
    gvkey_c = _find_col(cols, "gvkey")
    fyear_c = _find_col(cols, "fyear")
    datadate_c = _find_col(cols, "datadate")

    if gvkey_c is None:
        raise ValueError(f"Could not find gvkey in columns. First cols: {cols[:30]}")
    if fyear_c is None and datadate_c is None:
        raise ValueError("Need fyear (preferred) or datadate to construct year.")

    # Rename keys to canonical names BEFORE shuffle
    rename_map = {}
    if gvkey_c != "gvkey":
        rename_map[gvkey_c] = "gvkey"
    if fyear_c is not None:
        rename_map[fyear_c] = "year"   # fiscal year
    else:
        if datadate_c != "datadate":
            rename_map[datadate_c] = "datadate"

    if datadate_c is not None and datadate_c != "datadate":
        rename_map[datadate_c] = "datadate"

    if rename_map:
        ddf = ddf.rename(columns=rename_map)

    # If we didn't have fyear, create year from datadate
    if "year" not in ddf.columns:
        ddf["datadate"] = dd.to_datetime(ddf["datadate"], errors="coerce")
        ddf["year"] = ddf["datadate"].dt.year

    # ---- Normalize dtypes (THIS FIXES YOUR ERROR)
    # Keys
    ddf["gvkey"] = dd.to_numeric(ddf["gvkey"], errors="coerce")
    ddf["year"] = dd.to_numeric(ddf["year"], errors="coerce")

    # Dates
    if "datadate" in ddf.columns:
        ddf["datadate"] = dd.to_datetime(ddf["datadate"], errors="coerce")

    # Numeric columns that sometimes come as strings in some yearly files
    numeric_cols = [
        "at", "sale", "revt", "capx", "xrd", "ppent", "emp",
        "oibdp", "ib", "ni", "csho", "prcc_f", "mkvalt"
    ]
    for c in numeric_cols:
        if c in ddf.columns:
            ddf[c] = dd.to_numeric(ddf[c], errors="coerce").astype("float64")

    # Optional: keep only the columns you care about (reduces size + future schema headaches)
    keep_cols = [
        "gvkey", "year", "datadate", "fyr", "indfmt", "consol", "datafmt", "popsrc", "curcd",
        "tic", "cusip", "cik", "conm",
        "at", "sale", "revt", "capx", "xrd", "ppent", "emp", "oibdp", "ib", "ni", "csho", "prcc_f", "mkvalt",
        "sic", "naics"
    ]
    keep_cols = [c for c in keep_cols if c in ddf.columns]
    ddf = ddf[keep_cols]

    # Drop missing keys then cast keys to clean ints (now safe)
    ddf = ddf.dropna(subset=["gvkey", "year"])
    ddf["gvkey"] = ddf["gvkey"].astype("int64")
    ddf["year"] = ddf["year"].astype("int32")

    print("[INFO] Ready. Columns:", ddf.columns.tolist()[:30], "...")
    print("[INFO] Keys exist:", "gvkey" in ddf.columns, "year" in ddf.columns)

    # Enforce uniqueness
    if MAKE_UNIQUE_GVKEY_YEAR:
        print("[INFO] Shuffling on (gvkey, year) to enforce uniqueness...")
        ddf = ddf.shuffle(on=["gvkey", "year"], shuffle="tasks")
        ddf = ddf.map_partitions(_dedupe_keep_latest, meta=ddf._meta)

    # Write (NO year= partitions)
    print("[INFO] Writing to:", OUT_DIR)
    with ProgressBar():
        ddf.to_parquet(
            OUT_DIR,
            engine="pyarrow",
            compression="snappy",
            write_index=False,
            overwrite=True,
        )

    print("[DONE] Wrote merge-ready Compustat panel to:", OUT_DIR)
    print("       Merge keys are exactly: gvkey, year")


if __name__ == "__main__":
    main()
