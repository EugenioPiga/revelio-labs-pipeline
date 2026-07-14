#!/usr/bin/env python3
"""Read-only V3 audit for the LinkedIn--patent founder project.

This audit treats the validated spell-integrity V3 panel as the analytical source
of truth. It deliberately does not apply older V2 sample filters, does not use
V3 year-specific `first_*` variables as career starts, does not impute missing
patent/citation values as zeros, and does not create or overwrite any founder or
firm output. It writes only to output/founder_quality_v1/audit_v3/run_*.
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import json
import os
import re
import traceback
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

PROJECT_ROOT_DEFAULT = "/home/epiga/revelio_labs"
OUTPUT_ROOT_DEFAULT = "/home/epiga/revelio_labs/output/founder_quality_v1/audit_v3"
V3_ROOT = "/labs/khanna/linkedin_202507/processed/v3_spell_integrity"

# These paths are documented in the user's V3 source package. The raw-spell
# root is a candidate that must be validated by this audit before use.
CORE_INPUT = f"{V3_ROOT}/inventor_year_merged"
COLLEAGUE_INPUT = f"{V3_ROOT}/inventor_year_merged_colleague"
RAW_POSITION_CANDIDATE = f"{V3_ROOT}/step2_inventors_matched_positions"

FORMAL_IMMIGRATION_PRIORITY = [
    "immig_first_deg_or_job_nonUS",
    "immig1_post",
    "immig1_ever",
    "immig2_post",
    "immig2_ever",
]

EXACT_FIELDS: Dict[str, List[str]] = {
    "person_identifier": ["user_id", "founder_user_id", "founder_id", "inventor_id", "person_id"],
    "time": ["year", "startdate", "enddate", "first_startdate_pos", "edu_first_startdate", "edu_first_enddate"],
    "formal_immigration_indicator": FORMAL_IMMIGRATION_PRIORITY,
    "foreign_education_proxy": ["edu_first_university_country", "edu_last_university_country", "first_university_country", "last_university_country"],
    "first_job_or_work_country_proxy": ["first_country", "last_country", "current_country", "last_known_country"],
    "raw_position_spell": ["position_id", "rcid", "ultimate_parent_rcid", "country", "city", "metro_area", "startdate", "enddate", "naics_code", "onet_code", "seniority"],
    "v3_year_specific_first_fields_not_career_start": ["first_parent_rcid", "first_rcid", "first_city", "first_metro_area", "first_country", "first_startdate_pos"],
    "v3_current_or_last_known_position": ["current_parent_rcid", "current_rcid", "current_city", "current_metro_area", "current_country", "last_known_parent_rcid", "last_known_rcid", "last_known_city", "last_known_metro_area", "last_known_country"],
    "pre_founding_inventor_outcome_candidates": ["n_patents", "n_applications", "n_first_inventor", "n_us_citations"],
    "education": ["has_education", "n_educations", "edu_first_degree", "edu_last_degree", "edu_first_field", "edu_last_field", "edu_first_university_country", "edu_last_university_country"],
    "career_summary": ["n_positions", "n_unique_companies", "n_unique_parents", "n_unique_naics3", "n_unique_naics6", "n_unique_onet_codes", "avg_seniority", "min_seniority", "max_seniority", "career_modal_class", "career_modal_section"],
    "colleague_quality_candidates": ["parent_loo_patents", "firm_loo_patents", "city_loo_patents", "metro_loo_patents", "n_cite_links_total", "n_collab_links_total", "n_cited_patents_total"],
    "candidate_firm_identifier": ["rcid", "parent_rcid", "current_rcid", "current_parent_rcid", "first_rcid", "first_parent_rcid"],
    "possible_founder_flag_or_link": ["founder", "cofounder", "entrepreneur", "is_founder", "founder_user_id", "founder_id"],
    "possible_firm_birth_time": ["founding_date", "founding_year", "firm_birth_date", "firm_birth_year", "company_founding_date", "company_founding_year", "first_firm_year"],
    "possible_firm_outcome": ["patent_count", "patents", "n_patents", "citation_count", "citations", "n_us_citations"],
}

CODE_TERMS = [
    "submit_founder_v3", "founder_firm", "founder_firm_outcomes", "firm_year_outcomes",
    "founding_date", "founding_year", "firm_birth", "immig_first_deg_or_job_nonUS",
    "immig_deg_first_nonUS", "immig_job_first_nonUS", "first_startdate_pos",
    "step2_inventors_matched_positions", "inventor_year_merged_colleague",
]

SKIP_DIRS = {".git", "__pycache__", ".cache", ".venv", "node_modules", "logs", "output", "tmp"}


def js(value: Any) -> Any:
    if isinstance(value, (dt.date, dt.datetime)):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return str(value)


def dump_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(obj, indent=2, sort_keys=True, default=js) + "\n", encoding="utf-8")


def dump_csv(path: Path, rows: Sequence[Dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fields: List[str] = []
    seen = set()
    for row in rows:
        for key in row:
            if key not in seen:
                fields.append(key)
                seen.add(key)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: json.dumps(v, default=js) if isinstance(v, (dict, list, tuple)) else v for k, v in row.items()})


def compact_inventory(path_text: str, max_entries: int = 100000) -> Dict[str, Any]:
    path = Path(path_text)
    out: Dict[str, Any] = {
        "path": path_text,
        "exists": path.exists(),
        "is_directory": path.is_dir() if path.exists() else False,
        "part_files_observed": 0,
        "part_bytes_observed": 0,
        "hive_partition_columns": [],
        "scan_truncated": False,
    }
    if not out["exists"] or not out["is_directory"]:
        return out
    parts = set()
    entries = 0
    try:
        for root, dirs, files in os.walk(path):
            entries += len(dirs) + len(files)
            if entries > max_entries:
                out["scan_truncated"] = True
                break
            for d in dirs:
                if "=" in d and not d.startswith("."):
                    parts.add(d.split("=", 1)[0])
            for f in files:
                if f.startswith("part-") or f.endswith(".parquet"):
                    out["part_files_observed"] += 1
                    try:
                        out["part_bytes_observed"] += os.path.getsize(os.path.join(root, f))
                    except OSError:
                        pass
    except Exception as exc:
        out["inventory_error"] = repr(exc)
    out["hive_partition_columns"] = sorted(parts)
    return out


def exact_columns(columns: Sequence[str], names: Sequence[str]) -> List[str]:
    lower = {c.lower(): c for c in columns}
    return [lower[n.lower()] for n in names if n.lower() in lower]


def first_exact(columns: Sequence[str], names: Sequence[str]) -> str | None:
    got = exact_columns(columns, names)
    return got[0] if got else None


def missing_expr(df, col_name: str):
    expr = F.col(col_name).isNull()
    if isinstance(df.schema[col_name].dataType, StringType):
        expr = expr | (F.length(F.trim(F.col(col_name))) == 0)
    return expr


def is_text_equal(col_name: str, value: str):
    return F.trim(F.col(col_name).cast("string")) == F.lit(value)


def profile_parquet(spark: SparkSession, label: str, path_text: str, count_rows: bool = True) -> Dict[str, Any]:
    inv = compact_inventory(path_text)
    base: Dict[str, Any] = {"dataset": label, "path": path_text, "inventory": inv}
    if not inv["exists"]:
        base["status"] = "path_absent"
        return base
    try:
        df = spark.read.option("mergeSchema", "false").parquet(path_text)
    except Exception as exc:
        base.update({"status": "spark_read_failed", "error": repr(exc)})
        return base

    columns = df.columns
    base.update({
        "status": "readable",
        "n_columns": len(columns),
        "columns": columns,
        "schema": json.loads(df.schema.json()),
        "exact_field_families": {family: exact_columns(columns, names) for family, names in EXACT_FIELDS.items()},
    })
    if not count_rows:
        return base

    selected = list(dict.fromkeys(
        col for vals in base["exact_field_families"].values() for col in vals
    ))
    selected = selected[:130]
    outcome_cols = exact_columns(columns, EXACT_FIELDS["pre_founding_inventor_outcome_candidates"] + EXACT_FIELDS["possible_firm_outcome"])
    time_cols = [c for c in columns if re.search(r"(^year$|year$|date$|startdate|enddate|found|birth|created)", c, re.I)][:50]
    immig_cols = exact_columns(columns, FORMAL_IMMIGRATION_PRIORITY)

    exprs: List[Any] = [F.count(F.lit(1)).alias("rows")]
    for i, c in enumerate(selected):
        exprs.append(F.sum(F.when(missing_expr(df, c), 1).otherwise(0)).alias(f"miss_{i}"))
    for i, c in enumerate(outcome_cols):
        val = F.col(c).cast("double")
        exprs += [
            F.sum(F.when(val == 0, 1).otherwise(0)).alias(f"zero_{i}"),
            F.sum(F.when(val < 0, 1).otherwise(0)).alias(f"neg_{i}"),
        ]
    for i, c in enumerate(time_cols):
        exprs += [
            F.min(F.col(c).cast("string")).alias(f"time_min_{i}"),
            F.max(F.col(c).cast("string")).alias(f"time_max_{i}"),
        ]
    for i, c in enumerate(immig_cols):
        val = F.col(c).cast("double")
        exprs += [
            F.sum(F.when(missing_expr(df, c), 1).otherwise(0)).alias(f"imm_miss_{i}"),
            F.sum(F.when(val == 1, 1).otherwise(0)).alias(f"imm_one_{i}"),
            F.sum(F.when(val == 0, 1).otherwise(0)).alias(f"imm_zero_{i}"),
            F.sum(F.when((~val.isNull()) & (~val.isin(0.0, 1.0)), 1).otherwise(0)).alias(f"imm_other_{i}"),
        ]

    # Coverage only: these legacy definitions are never imposed by the audit.
    first_country = first_exact(columns, ["first_country"])
    last_country = first_exact(columns, ["last_country", "last_known_country", "current_country"])
    last_uni = first_exact(columns, ["last_university_country", "edu_last_university_country"])
    if first_country:
        exprs.append(F.sum(F.when(is_text_equal(first_country, "United States"), 1).otherwise(0)).alias("legacy_us_first_country"))
    if last_uni:
        exprs.append(F.sum(F.when(is_text_equal(last_uni, "United States"), 1).otherwise(0)).alias("legacy_us_last_uni"))
    if first_country and last_uni:
        exprs.append(F.sum(F.when(is_text_equal(first_country, "United States") & is_text_equal(last_uni, "United States"), 1).otherwise(0)).alias("legacy_us_position_education"))
    if first_country and last_country and last_uni:
        exprs.append(F.sum(F.when(is_text_equal(first_country, "United States") & is_text_equal(last_country, "United States") & is_text_equal(last_uni, "United States"), 1).otherwise(0)).alias("legacy_tenure_us"))

    # Exact row count plus all diagnostics are intentionally one aggregate action.
    values = df.agg(*exprs).first().asDict()
    rows = int(values["rows"])
    base["exact_row_count"] = rows
    base["missingness"] = [{
        "variable": c,
        "missing_or_blank": int(values.get(f"miss_{i}") or 0),
        "missing_share": (values.get(f"miss_{i}") or 0) / rows if rows else None,
        "spark_type": df.schema[c].dataType.simpleString(),
    } for i, c in enumerate(selected)]
    base["zero_negative_profiles"] = [{
        "variable": c,
        "zero_values": int(values.get(f"zero_{i}") or 0),
        "negative_values": int(values.get(f"neg_{i}") or 0),
        "note": "No missing value is recoded to zero by this audit.",
    } for i, c in enumerate(outcome_cols)]
    base["time_bounds"] = [{
        "variable": c,
        "minimum_string_value": values.get(f"time_min_{i}"),
        "maximum_string_value": values.get(f"time_max_{i}"),
        "caveat": "String-bound audit only; event-date semantics must be verified in creator code.",
    } for i, c in enumerate(time_cols)]
    base["formal_immigration_profile"] = [{
        "variable": c,
        "priority_rank": FORMAL_IMMIGRATION_PRIORITY.index(c) + 1,
        "missing_or_blank": int(values.get(f"imm_miss_{i}") or 0),
        "value_one": int(values.get(f"imm_one_{i}") or 0),
        "value_zero": int(values.get(f"imm_zero_{i}") or 0),
        "other_nonmissing": int(values.get(f"imm_other_{i}") or 0),
        "interpretation": (
            "Primary formal candidate only after creator-code verification."
            if c == "immig_first_deg_or_job_nonUS" else
            "Fallback/proxy field only; do not substitute for the formal indicator."
        ),
    } for i, c in enumerate(immig_cols)]
    base["legacy_filter_coverage"] = {
        "legacy_us_position_first_country_field": first_country,
        "legacy_us_last_university_field": last_uni,
        "legacy_us_position_rows": int(values.get("legacy_us_first_country") or 0) if first_country else None,
        "legacy_us_last_university_rows": int(values.get("legacy_us_last_uni") or 0) if last_uni else None,
        "legacy_us_position_and_education_rows": int(values.get("legacy_us_position_education") or 0) if first_country and last_uni else None,
        "legacy_us_tenure_rows": int(values.get("legacy_tenure_us") or 0) if first_country and last_country and last_uni else None,
        "caveat": "Coverage diagnostic only; V3 first_* fields are not used to define a founder sample.",
    }
    return base


def candidate_output_dirs(project_root: Path) -> List[Dict[str, Any]]:
    roots = [project_root / "output", project_root / "founder" / "output"]
    pat = re.compile(r"(founder.*firm|firm.*founder|firm.*outcome|outcome.*firm|firm[_-]?year)", re.I)
    rows: List[Dict[str, Any]] = []
    seen = set()
    for root in roots:
        if not root.is_dir():
            continue
        for p in root.rglob("*"):
            try:
                rel_depth = len(p.relative_to(root).parts)
            except ValueError:
                continue
            if rel_depth > 3 or not p.is_dir() or not pat.search(p.name):
                continue
            rp = str(p.resolve())
            if rp in seen:
                continue
            seen.add(rp)
            rows.append({"path": rp, "source_root": str(root), "name": p.name, **compact_inventory(rp)})
    return sorted(rows, key=lambda x: x["path"])


def v3_source_dirs() -> List[Dict[str, Any]]:
    root = Path(V3_ROOT)
    rows: List[Dict[str, Any]] = []
    if not root.is_dir():
        return [{"path": str(root), "status": "absent_or_unreadable"}]
    for p in sorted(root.iterdir(), key=lambda x: x.name):
        if re.search(r"(position|education|inventor|spell|patent|firm|founder|company)", p.name, re.I):
            rows.append({"path": str(p), "name": p.name, **compact_inventory(str(p), max_entries=5000)})
    return rows


def source_scan(project_root: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    search_roots = [project_root / n for n in ["src", "jobs", "founder", "pipeline_v3_spell_integrity", "scripts"]]
    n_files = 0
    for root in search_roots:
        if not root.is_dir():
            continue
        for base, dirs, files in os.walk(root):
            dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
            for fn in files:
                if not fn.endswith((".py", ".sh", ".sbatch", ".R", ".md", ".yaml", ".yml", ".json")):
                    continue
                n_files += 1
                if n_files > 7000:
                    return rows + [{"status": "scan_truncated", "reason": "file_cap_7000"}]
                path = Path(base) / fn
                try:
                    for lineno, line in enumerate(path.read_text(encoding="utf-8", errors="replace").splitlines(), start=1):
                        matched = [t for t in CODE_TERMS if t.lower() in line.lower()]
                        if matched:
                            rows.append({"file": str(path), "line": lineno, "matched_terms": ";".join(matched), "text": line.strip()[:1000]})
                except OSError as exc:
                    rows.append({"file": str(path), "status": "read_error", "error": repr(exc)})
    return rows


def output_profiles(spark: SparkSession, candidates: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    # At most the first 12 candidate roots are read. More candidates are recorded
    # but not opened to prevent an accidental audit-wide scan of irrelevant outputs.
    selected = [r for r in candidates if r.get("exists")][:12]
    return {f"candidate_output_{i:02d}_{Path(row['path']).name}": profile_parquet(spark, f"candidate_output_{i:02d}", row["path"], True) for i, row in enumerate(selected, start=1)}


def data_dictionary(profiles: Dict[str, Dict[str, Any]]) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    caveats = {
        "formal_immigration_indicator": "Distinct from foreign education, first-job-country, and work-history proxies; construction must match creator code.",
        "v3_year_specific_first_fields_not_career_start": "V3 first_* values are first observed within an inventor-year and must not be treated as career starts for founder analysis.",
        "raw_position_spell": "A raw dated spell source is required for pre-founding mobility/tenure; this field inventory alone does not validate a spell's timing semantics.",
        "pre_founding_inventor_outcome_candidates": "Must be lagged/cumulated strictly through the founding date before use as a founder characteristic.",
        "possible_firm_outcome": "Requires creator-code validation of firm-year universe, zero coverage, and fixed post-founding horizon construction.",
    }
    for dataset, p in profiles.items():
        if p.get("status") != "readable":
            continue
        missing = {r["variable"]: r for r in p.get("missingness", [])}
        for concept, fields in p.get("exact_field_families", {}).items():
            for field in fields:
                m = missing.get(field, {})
                rows.append({
                    "Concept": concept,
                    "Exact variable(s)": field,
                    "Source dataset": dataset,
                    "Unit": "Not inferred from schema alone.",
                    "Construction": "Exact V3 schema field. Verify creator-code construction in code_evidence.csv.",
                    "Timing relative to founding": "Unverified until linked to a founder event and raw dates.",
                    "Missingness/caveat": f"Missing or blank: {m.get('missing_or_blank', 'not profiled')} of {p.get('exact_row_count', 'unknown')}. {caveats.get(concept, 'Semantic meaning is not inferred from the name.')}",
                })
    return rows


def report(run_dir: Path, core: Dict[str, Any], colleague: Dict[str, Any], raw_positions: Dict[str, Any], candidates: List[Dict[str, Any]], output_ps: Dict[str, Dict[str, Any]]) -> None:
    lines = [
        "# Founder-quality Phase-0 audit — V3-only",
        "",
        f"Run directory: `{run_dir}`",
        "",
        "## Scope and non-negotiable timing rules",
        "",
        "- Analytical inputs audited here are V3 spell-integrity inputs, not the older V2 panel.",
        "- The audit does not use `first_parent_rcid`, `first_rcid`, `first_city`, `first_metro_area`, `first_country`, or `first_startdate_pos` as career-start values.",
        "- Pre-founding founder characteristics require raw dated employment spells and data observed no later than the verified founding date.",
        "- The formal project immigrant measure is kept separate from foreign education, first job outside the US, and foreign work-history proxies.",
        "- Missing patent/citation values are never recoded to zero by this audit.",
        "",
        "## V3 input status",
        "",
        "| Dataset | Status | Exact rows | Columns |",
        "|---|---|---:|---:|",
        f"| V3 core inventor-year | {core.get('status')} | {core.get('exact_row_count','')} | {core.get('n_columns','')} |",
        f"| V3 colleague inventor-year | {colleague.get('status')} | {colleague.get('exact_row_count','')} | {colleague.get('n_columns','')} |",
        f"| Candidate V3 raw positions | {raw_positions.get('status')} | {raw_positions.get('exact_row_count','')} | {raw_positions.get('n_columns','')} |",
        "",
        f"Candidate founder/firm output directories discovered: {len(candidates)}.",
        f"Candidate founder/firm outputs read as Parquet: {sum(1 for p in output_ps.values() if p.get('status') == 'readable')}.",
        "",
        "## Mandatory gate before any founder-quality analysis",
        "",
        "1. Use `code_evidence.csv` to identify the exact V3 founder-link creator script, firm-birth/founding-date construction, and firm-year outcome construction.",
        "2. Use `candidate_output_profiles.json` to determine whether an existing founder--firm or firm-year output is readable and complete. Do not infer this from a directory name.",
        "3. Validate the raw V3 dated spell source before calculating career age, tenure, moves, job/country history, or prior-employer quality.",
        "4. Confirm that patent/citation flows are firm-year observations with an explicit universe including zeros before building 0–3, 0–5, or 0–10 post-founding outcomes.",
        "5. Preserve legacy US position/education rules as comparison-sample coverage only. Choose the founder sample after the founder event is defined.",
        "",
        "## Files",
        "",
        "- `AUDIT_REPORT.md` — this report",
        "- `v3_input_profiles.json` / `v3_schema_catalog.json` — exact V3 schemas, row counts, missingness, zero profiles, and time bounds",
        "- `v3_raw_source_inventory.json` — V3 source candidates, including dated-spell candidates",
        "- `candidate_output_inventory.json` / `candidate_output_profiles.json` — existing founder/firm output evidence",
        "- `code_evidence.csv` — exact source code lines related to V3 founder links, outcomes, founding dates, immigration, and raw spells",
        "- `formal_immigration_profile.csv` — formal field and fallback fields reported separately",
        "- `legacy_filter_coverage.json` — legacy US-restriction coverage only; not imposed",
        "- `data_dictionary.csv` — requested dictionary with exact V3 schema fields and timing caveats",
        "",
    ]
    (run_dir / "AUDIT_REPORT.md").write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--project-root", default=PROJECT_ROOT_DEFAULT)
    parser.add_argument("--output-root", default=OUTPUT_ROOT_DEFAULT)
    parser.add_argument("--threads", type=int, default=max(1, int(os.environ.get("SLURM_CPUS_PER_TASK", "1"))))
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    stamp = dt.datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    run_dir = Path(args.output_root) / f"run_{stamp}"
    run_dir.mkdir(parents=True, exist_ok=False)
    project_root = Path(args.project_root)

    spark = None
    try:
        spark = (
            SparkSession.builder
            .appName(f"founder_quality_phase0_v3_{stamp}")
            .master(f"local[{max(1, args.threads)}]")
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.shuffle.partitions", str(max(16, args.threads * 8)))
            .config("spark.sql.parquet.mergeSchema", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("WARN")

        core = profile_parquet(spark, "v3_inventor_year_core", CORE_INPUT, True)
        colleague = profile_parquet(spark, "v3_inventor_year_colleague", COLLEAGUE_INPUT, True)
        raw_positions = profile_parquet(spark, "v3_raw_positions_candidate", RAW_POSITION_CANDIDATE, True)
        candidates = candidate_output_dirs(project_root)
        output_ps = output_profiles(spark, candidates)
        evidence = source_scan(project_root)
        all_profiles = {"v3_core": core, "v3_colleague": colleague, "v3_raw_positions_candidate": raw_positions, **output_ps}

        dump_json(run_dir / "manifest.json", {
            "created_utc": dt.datetime.utcnow().isoformat() + "Z",
            "project_root": str(project_root),
            "v3_root": V3_ROOT,
            "core_input": CORE_INPUT,
            "colleague_input": COLLEAGUE_INPUT,
            "raw_position_candidate": RAW_POSITION_CANDIDATE,
            "source_data_modified": False,
            "existing_outputs_modified": False,
            "note": "V3-only audit. It does not create an analysis sample or empirical result.",
        })
        dump_json(run_dir / "v3_input_profiles.json", {"core": core, "colleague": colleague, "raw_positions_candidate": raw_positions})
        dump_json(run_dir / "v3_schema_catalog.json", {k: v.get("schema") for k, v in all_profiles.items()})
        dump_json(run_dir / "v3_raw_source_inventory.json", v3_source_dirs())
        dump_json(run_dir / "candidate_output_inventory.json", candidates)
        dump_json(run_dir / "candidate_output_profiles.json", output_ps)
        dump_csv(run_dir / "code_evidence.csv", evidence)
        dump_json(run_dir / "legacy_filter_coverage.json", {k: v.get("legacy_filter_coverage") for k, v in {"core": core, "colleague": colleague, "raw_positions_candidate": raw_positions}.items()})
        immig = []
        for name, p in {"v3_core": core, "v3_colleague": colleague, "v3_raw_positions_candidate": raw_positions}.items():
            for r in p.get("formal_immigration_profile", []):
                immig.append({"dataset": name, **r})
        dump_csv(run_dir / "formal_immigration_profile.csv", immig)
        missing_zero = []
        for name, p in all_profiles.items():
            for r in p.get("missingness", []):
                missing_zero.append({"dataset": name, "kind": "missingness", **r})
            for r in p.get("zero_negative_profiles", []):
                missing_zero.append({"dataset": name, "kind": "zero_negative", **r})
        dump_csv(run_dir / "missingness_and_zero_profiles.csv", missing_zero)
        dump_csv(run_dir / "data_dictionary.csv", data_dictionary(all_profiles))
        report(run_dir, core, colleague, raw_positions, candidates, output_ps)
        print(f"AUDIT_DIR={run_dir}")
        print(f"AUDIT_REPORT={run_dir / 'AUDIT_REPORT.md'}")
    except Exception:
        (run_dir / "FATAL_EXCEPTION.txt").write_text(traceback.format_exc(), encoding="utf-8")
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
