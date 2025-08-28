#!/usr/bin/env python
# append_examples_to_tabs_report.py
#
# Appends 5 non-missing examples per column (one parquet per dataset)
# to the end of an existing tabs_report.txt.
#
# Usage:
#   python append_examples_to_tabs_report.py \
#       --report ~/revelio_labs/report/tabs_report.txt \
#       --max-examples 5
#
# Requires: pyarrow

import os
import glob
import math
import argparse
from datetime import datetime

import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.compute as pc

# ---- Edit dataset -> glob pattern here if needed ----
DATASETS = {
    "academic_company_ref":             "academic_company_ref/*.parquet",
    "academic_individual_position":     "academic_individual_position/*.parquet",
    "academic_individual_user":         "academic_individual_user/*.parquet",
    "academic_individual_user_education":"academic_individual_user_education/*.parquet",
    "academic_individual_user_skill":   "academic_individual_user_skill/*.parquet",
    "revelio_patents_assignee_matches": "revelio_patents_assignee_matches/*.parquet",
    "revelio_patents_inventor_matches": "revelio_patents_inventor_matches/*.parquet",
}

def is_missing_py(v):
    """Treat None/NaN/empty-string as missing."""
    if v is None:
        return True
    if isinstance(v, float) and math.isnan(v):
        return True
    if isinstance(v, str) and v.strip() == "":
        return True
    return False

def to_python_values(arr: pa.Array, max_needed: int):
    """Convert a PyArrow Array to up to max_needed non-missing Python values."""
    out = []
    non_null = pc.drop_null(arr)
    for v in non_null.to_pylist():
        if is_missing_py(v):
            continue
        if isinstance(v, (bytes, bytearray)):
            try:
                v = v.decode("utf-8", "ignore")
            except Exception:
                v = str(v)
        out.append(v)
        if len(out) >= max_needed:
            break
    return out

def sample_column_examples(pf: pq.ParquetFile, col_name: str, max_examples: int = 5):
    """Collect up to max_examples non-missing examples for a column by scanning row groups."""
    examples = []
    for rg in range(pf.num_row_groups):
        if len(examples) >= max_examples:
            break
        tbl = pf.read_row_group(rg, columns=[col_name])
        col = tbl.column(0)  # ChunkedArray
        for chunk in col.chunks:
            if len(examples) >= max_examples:
                break
            examples.extend(to_python_values(chunk, max_examples - len(examples)))
    return examples

def pick_one_parquet(path_pattern: str):
    files = sorted(glob.glob(path_pattern))
    return files[0] if files else None

def fmt_examples(values, max_len_each=100):
    if not values:
        return "(no non-missing examples found)"
    out = []
    for v in values:
        s = str(v)
        if len(s) > max_len_each:
            s = s[: max_len_each - 3] + "..."
        out.append(s)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--report", required=True, help="Path to tabs_report.txt (will be appended)")
    ap.add_argument("--max-examples", type=int, default=5, help="Max examples per column")
    ap.add_argument("--header", default="EXAMPLES (auto-generated)", help="Section header title")
    args = ap.parse_args()

    report_path = os.path.expanduser(args.report)
    if not os.path.isfile(report_path):
        raise SystemExit(f"Report file not found: {report_path}")

    # Build the examples text to append
    lines = []
    lines.append("\n\n" + "=" * 80)
    lines.append(f"{args.header}")
    lines.append("=" * 80)
    lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    lines.append("Note: one parquet sampled per dataset; up to "
                 f"{args.max_examples} non-missing examples per column.\n")

    for ds_name, pattern in DATASETS.items():
        one = pick_one_parquet(pattern)
        lines.append("-" * 80)
        lines.append(f"DATASET: {ds_name}")
        if not one:
            lines.append(f"(no parquet files found for pattern: {pattern})\n")
            continue

        try:
            pf = pq.ParquetFile(one)
            schema = pf.schema_arrow
        except Exception as e:
            lines.append(f"(error opening parquet: {one}): {e}\n")
            continue

        lines.append(f"Sampled file: {one}\n")
        # Pretty block per column
        for field in schema:
            col_name = field.name
            dtype = str(field.type)
            try:
                ex = sample_column_examples(pf, col_name, args.max_examples)
                ex_fmt = fmt_examples(ex)
            except Exception as e:
                ex_fmt = [f"(error sampling: {e})"]

            lines.append(f"{col_name}  [dtype: {dtype}]")
            if ex_fmt and isinstance(ex_fmt, list):
                for i, v in enumerate(ex_fmt, 1):
                    lines.append(f"  {i}. {v}")
            else:
                lines.append(f"  {ex_fmt}")
            lines.append("")  # blank line after each column

    # Append to the report
    with open(report_path, "a", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[OK] Appended examples to {report_path}")

if __name__ == "__main__":
    main()
