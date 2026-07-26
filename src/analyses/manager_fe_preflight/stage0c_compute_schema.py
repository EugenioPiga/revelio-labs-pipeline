#!/usr/bin/env python3

from __future__ import annotations

import argparse
import hashlib
import json
import os
import traceback
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F


DATASETS = {
    "corrected_colleague_panel": (
        "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
        "inventor_year_merged_colleague_origin_any_education"
    ),
    "corrected_core_panel": (
        "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
        "inventor_year_merged_origin_any_education"
    ),
    "position_spells": (
        "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
        "step2_inventors_matched_positions"
    ),
}


TARGET_GROUPS = {
    "identifiers": [
        "user_id",
        "position_id",
        "year",
    ],
    "firm_and_parent": [
        "rcid",
        "current_rcid",
        "first_rcid",
        "last_rcid",
        "ultimate_parent_rcid",
        "current_parent_rcid",
        "first_parent_rcid",
        "last_parent_rcid",
        "company_name",
        "current_company_name",
        "ultimate_parent_company_name",
        "current_parent_company_name",
    ],
    "spell_timing": [
        "startdate",
        "enddate",
        "start_year",
        "end_year",
        "position_number",
        "position_weight",
        "weight",
        "pos_obs",
        "position_observed",
        "synthetic_position_year",
    ],
    "location": [
        "city",
        "current_city",
        "first_city",
        "last_city",
        "state",
        "current_state",
        "country",
        "current_country",
        "region",
        "metro_area",
        "current_metro_area",
        "first_metro_area",
        "last_metro_area",
        "msa",
        "location_raw",
        "current_location_raw",
    ],
    "seniority_and_tenure": [
        "seniority",
        "current_seniority",
        "avg_seniority",
        "min_seniority",
        "max_seniority",
        "career_age",
        "firm_tenure",
        "first_job_year",
        "first_position_startdate",
    ],
    "titles_and_occupations": [
        "title_raw",
        "title_translated",
        "current_title_raw",
        "current_title_translated",
        "job_category_v2",
        "current_job_category_v2",
        "mapped_role_v3",
        "current_mapped_role_v3",
        "role_k150_v2",
        "role_k150_v3",
        "onet_code",
        "onet_title",
        "current_onet_code",
        "current_onet_title",
    ],
    "patents_and_citations": [
        "n_patents",
        "n_us_citations",
        "n_first_inventor",
        "modal_section_y",
        "modal_class_y",
        "career_modal_section",
        "career_modal_class",
    ],
    "colleague_and_collaboration": [
        "n_colleagues",
        "n_coworkers",
        "n_collaborators",
        "n_coauthors",
        "same_parent",
        "same_parent_metro",
    ],
    "founder_and_origin": [
        "founder",
        "is_founder",
        "ever_founder",
        "country_of_origin",
        "country_of_origin_key",
        "country_of_origin_source",
        "country_of_origin_nonUS",
        "immigration_origin_observed",
    ],
}


HASH_COLUMNS = {
    "user_id",
    "position_id",
    "rcid",
    "current_rcid",
    "first_rcid",
    "last_rcid",
    "ultimate_parent_rcid",
    "current_parent_rcid",
    "first_parent_rcid",
    "last_parent_rcid",
}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def hash_value(value: Any) -> str | None:
    if value is None:
        return None

    return hashlib.sha256(
        str(value).encode("utf-8")
    ).hexdigest()[:12]


def existing_columns(df: DataFrame, requested: list[str]) -> list[str]:
    available = set(df.columns)
    return [column for column in requested if column in available]


def target_column_map(df: DataFrame) -> dict[str, list[str]]:
    return {
        group: existing_columns(df, columns)
        for group, columns in TARGET_GROUPS.items()
    }


def anonymize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    output = []

    for row in rows:
        cleaned = {}

        for key, value in row.items():
            if key in HASH_COLUMNS:
                cleaned[key] = hash_value(value)
            elif isinstance(value, (datetime,)):
                cleaned[key] = value.isoformat()
            else:
                cleaned[key] = value

        output.append(cleaned)

    return output


def dataset_report(
    spark: SparkSession,
    label: str,
    path: str,
) -> dict[str, Any]:

    report: dict[str, Any] = {
        "label": label,
        "path": path,
        "checked_utc": utc_now(),
        "path_exists_from_python": os.path.exists(path),
        "path_readable_from_python": os.access(path, os.R_OK),
        "path_traversable_from_python": os.access(path, os.X_OK),
        "success_marker_exists": os.path.isfile(
            os.path.join(path, "_SUCCESS")
        ),
    }

    try:
        df = (
            spark.read
            .option("mergeSchema", "false")
            .option("recursiveFileLookup", "true")
            .parquet(path)
        )

        report["spark_open_status"] = "success"
        report["n_schema_columns"] = len(df.columns)
        report["columns"] = df.columns
        report["schema"] = json.loads(df.schema.json())
        report["target_columns"] = target_column_map(df)

        sample_columns = []

        for columns in report["target_columns"].values():
            for column in columns:
                if column not in sample_columns:
                    sample_columns.append(column)

        sample_columns = sample_columns[:35]

        if sample_columns:
            sample_rows = (
                df.select(*sample_columns)
                .limit(25)
                .collect()
            )

            sample_dicts = [
                row.asDict(recursive=True)
                for row in sample_rows
            ]

            report["sample_columns"] = sample_columns
            report["sample_rows_anonymized"] = anonymize_rows(
                sample_dicts
            )
        else:
            report["sample_columns"] = []
            report["sample_rows_anonymized"] = []

        # These are sample-only diagnostics. They do not claim full-data counts.
        if "user_id" in df.columns and "year" in df.columns:
            sample_key_rows = (
                df.select("user_id", "year")
                .limit(10000)
                .groupBy("user_id", "year")
                .count()
                .filter(F.col("count") > 1)
                .limit(20)
                .collect()
            )

            report["sample_duplicate_user_year_keys"] = [
                {
                    "user_id_hash": hash_value(row["user_id"]),
                    "year": row["year"],
                    "count": row["count"],
                }
                for row in sample_key_rows
            ]

        return report

    except Exception as exc:
        report["spark_open_status"] = "failed"
        report["exception_type"] = type(exc).__name__
        report["exception"] = str(exc)
        report["traceback"] = traceback.format_exc()
        return report


def render_text(reports: dict[str, dict[str, Any]]) -> str:
    lines = []

    lines.append("=" * 100)
    lines.append("MANAGER-FE STAGE 0C: COMPUTE-NODE SCHEMA PREFLIGHT")
    lines.append("=" * 100)
    lines.append(f"Created UTC: {utc_now()}")
    lines.append("")

    for label, report in reports.items():
        lines.append("-" * 100)
        lines.append(label)
        lines.append("-" * 100)
        lines.append(f"Path: {report['path']}")
        lines.append(
            "Python access: "
            f"exists={report['path_exists_from_python']}, "
            f"readable={report['path_readable_from_python']}, "
            f"traversable={report['path_traversable_from_python']}"
        )
        lines.append(
            f"_SUCCESS: {report['success_marker_exists']}"
        )
        lines.append(
            f"Spark open: {report.get('spark_open_status')}"
        )

        if report.get("spark_open_status") != "success":
            lines.append(
                f"Error: {report.get('exception_type')}: "
                f"{report.get('exception')}"
            )
            lines.append("")
            continue

        lines.append(
            f"Schema columns: {report['n_schema_columns']}"
        )
        lines.append("")

        for group, columns in report["target_columns"].items():
            lines.append(f"[{group}]")
            if columns:
                lines.extend(f"  - {column}" for column in columns)
            else:
                lines.append("  NONE FOUND")
            lines.append("")

        lines.append("All columns:")
        lines.extend(
            f"  {index:4d}. {column}"
            for index, column in enumerate(
                report["columns"],
                start=1,
            )
        )
        lines.append("")

        lines.append("Anonymized sample:")
        for row in report["sample_rows_anonymized"][:10]:
            lines.append(json.dumps(row, default=str))
        lines.append("")

        duplicate_keys = report.get(
            "sample_duplicate_user_year_keys",
            [],
        )

        lines.append(
            "Duplicate user-year keys in first 10,000 sampled rows: "
            f"{len(duplicate_keys)} displayed"
        )

        for row in duplicate_keys:
            lines.append(json.dumps(row))

        lines.append("")

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .appName("manager_fe_stage0c_schema_preflight")
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )
        .config(
            "spark.sql.parquet.datetimeRebaseModeInRead",
            "LEGACY",
        )
        .config(
            "spark.sql.parquet.int96RebaseModeInRead",
            "LEGACY",
        )
        .config("spark.sql.parquet.mergeSchema", "false")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        reports = {
            label: dataset_report(spark, label, path)
            for label, path in DATASETS.items()
        }

        manifest = {
            "created_utc": utc_now(),
            "host": os.uname().nodename,
            "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
            "spark_version": spark.version,
            "full_data_counts_performed": False,
            "estimation_performed": False,
            "supervisor_assignment_performed": False,
            "datasets": reports,
        }

        (out_dir / "schema_preflight.json").write_text(
            json.dumps(
                manifest,
                indent=2,
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )

        (out_dir / "schema_preflight.txt").write_text(
            render_text(reports),
            encoding="utf-8",
        )

        all_open = all(
            report.get("spark_open_status") == "success"
            for report in reports.values()
        )

        status = {
            "created_utc": utc_now(),
            "all_datasets_opened": all_open,
            "main_program_ready": False,
            "next_step": (
                "Review schema and decide organizational-unit definition."
                if all_open
                else "Resolve dataset access before further processing."
            ),
        }

        (out_dir / "status.json").write_text(
            json.dumps(status, indent=2),
            encoding="utf-8",
        )

        print(render_text(reports))
        print("=" * 100)
        print(f"Output directory: {out_dir}")
        print(f"All datasets opened: {all_open}")
        print("No full-data counts or estimations were performed.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
