#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


INPUT_PANEL = (
    "/labs/khanna/linkedin_202507/processed/v3_spell_integrity/"
    "inventor_year_merged_colleague_origin_any_education"
)

YEAR_MIN = 2000
YEAR_MAX = 2019

COLUMNS = [
    "user_id",
    "year",
    "n_patents",
    "current_position_id",
    "current_position_observed",
    "current_position_overlap_days",
    "position_carried_forward",
    "position_measures_observed",
    "current_rcid",
    "current_parent_rcid",
    "current_metro_area",
    "current_country",
    "current_seniority",
    "current_title_raw",
    "current_title_translated",
    "last_university_country",
]

# Deliberately excludes generic "lead" and "principal", which often denote
# senior individual contributors rather than supervisors.
MANAGER_PATTERN = (
    r"(^|[^a-z])("
    r"supervisor|manager|director|head|chief|"
    r"vice[ -]?president|vp|president|"
    r"group[ -]?leader|team[ -]?leader|"
    r"department[ -]?(chair|head)|"
    r"principal[ -]?investigator|"
    r"lab(oratory)?[ -]?manager"
    r")([^a-z]|$)"
)


def now_utc() -> str:
    return datetime.now(timezone.utc).isoformat()


def normalized_country(column_name: str):
    return F.lower(
        F.regexp_replace(
            F.trim(F.coalesce(F.col(column_name), F.lit(""))),
            r"[^A-Za-z]",
            "",
        )
    )


def is_us(column_name: str):
    return normalized_country(column_name).isin(
        "us",
        "usa",
        "unitedstates",
        "unitedstatesofamerica",
    )


def is_one(column_name: str):
    return F.coalesce(F.col(column_name).cast("int"), F.lit(0)) == 1


def valid_cell_expression(cell_columns: list[str]):
    result = F.lit(True)

    for column in cell_columns:
        if column == "year":
            result = result & F.col(column).isNotNull()
        else:
            result = (
                result
                & F.col(column).isNotNull()
                & (F.trim(F.col(column).cast("string")) != "")
            )

    return result


def summarize_rule(
    focal: DataFrame,
    candidate_source: DataFrame,
    rule_name: str,
    cell_columns: list[str],
    require_manager_title: bool,
) -> tuple[list[dict], list[dict], list[dict]]:
    candidate_condition = F.col("prior_patents") >= 1

    if require_manager_title:
        candidate_condition = (
            candidate_condition & F.col("manager_title")
        )

    candidate_levels = (
        candidate_source
        .filter(candidate_condition)
        .groupBy(
            *cell_columns,
            "current_seniority",
        )
        .agg(
            F.countDistinct("user_id").alias(
                "n_candidates_at_level"
            )
        )
    )

    array_column = f"candidate_levels_{rule_name}"

    candidate_arrays = (
        candidate_levels
        .groupBy(*cell_columns)
        .agg(
            F.sort_array(
                F.collect_list(
                    F.struct(
                        F.col("current_seniority").alias(
                            "candidate_seniority"
                        ),
                        F.col("n_candidates_at_level").alias(
                            "candidate_count"
                        ),
                    )
                )
            ).alias(array_column)
        )
    )

    closest_expression = (
        f"element_at("
        f"filter("
        f"{array_column}, "
        f"x -> x.candidate_seniority > current_seniority"
        f"), 1)"
    )

    audited = (
        focal
        .join(
            candidate_arrays,
            on=cell_columns,
            how="left",
        )
        .withColumn(
            "valid_rule_cell",
            valid_cell_expression(cell_columns),
        )
        .withColumn(
            "closest_higher_level",
            F.expr(closest_expression),
        )
        .withColumn(
            "closest_higher_seniority",
            F.col(
                "closest_higher_level.candidate_seniority"
            ),
        )
        .withColumn(
            "n_candidates_at_closest_level",
            F.col("closest_higher_level.candidate_count"),
        )
        .withColumn(
            "has_candidate",
            F.col("closest_higher_seniority").isNotNull(),
        )
        .withColumn(
            "unique_candidate",
            F.col("n_candidates_at_closest_level") == 1,
        )
        .withColumn(
            "candidate_tie",
            F.col("n_candidates_at_closest_level") > 1,
        )
        .withColumn(
            "seniority_gap",
            F.col("closest_higher_seniority")
            - F.col("current_seniority"),
        )
        .cache()
    )

    overall_row = (
        audited
        .agg(
            F.count("*").alias("focal_inventor_years"),
            F.sum(
                F.col("valid_rule_cell").cast("long")
            ).alias("valid_cell_inventor_years"),
            F.sum(
                F.col("has_candidate").cast("long")
            ).alias("with_candidate"),
            F.sum(
                F.col("unique_candidate").cast("long")
            ).alias("with_unique_candidate"),
            F.sum(
                F.col("candidate_tie").cast("long")
            ).alias("with_candidate_tie"),
            F.avg(
                F.when(
                    F.col("has_candidate"),
                    F.col("seniority_gap"),
                )
            ).alias("mean_seniority_gap"),
            F.max("n_candidates_at_closest_level").alias(
                "maximum_candidates_at_closest_level"
            ),
        )
        .collect()[0]
        .asDict()
    )

    total = int(overall_row["focal_inventor_years"] or 0)
    valid = int(overall_row["valid_cell_inventor_years"] or 0)
    candidate = int(overall_row["with_candidate"] or 0)
    unique = int(overall_row["with_unique_candidate"] or 0)
    tie = int(overall_row["with_candidate_tie"] or 0)

    overall = [{
        "rule": rule_name,
        "cell_definition": " x ".join(cell_columns),
        "managerial_title_required": require_manager_title,
        "focal_inventor_years": total,
        "valid_cell_inventor_years": valid,
        "with_candidate": candidate,
        "without_candidate": total - candidate,
        "with_unique_candidate": unique,
        "with_candidate_tie": tie,
        "coverage_rate_all_focals": (
            candidate / total if total else None
        ),
        "coverage_rate_valid_cells": (
            candidate / valid if valid else None
        ),
        "unique_rate_all_focals": (
            unique / total if total else None
        ),
        "unique_rate_valid_cells": (
            unique / valid if valid else None
        ),
        "unique_share_conditional_on_candidate": (
            unique / candidate if candidate else None
        ),
        "tie_share_conditional_on_candidate": (
            tie / candidate if candidate else None
        ),
        "mean_seniority_gap": overall_row[
            "mean_seniority_gap"
        ],
        "maximum_candidates_at_closest_level": overall_row[
            "maximum_candidates_at_closest_level"
        ],
    }]

    by_year_df = (
        audited
        .groupBy("year")
        .agg(
            F.count("*").alias("focal_inventor_years"),
            F.sum(
                F.col("valid_rule_cell").cast("long")
            ).alias("valid_cell_inventor_years"),
            F.sum(
                F.col("has_candidate").cast("long")
            ).alias("with_candidate"),
            F.sum(
                F.col("unique_candidate").cast("long")
            ).alias("with_unique_candidate"),
            F.sum(
                F.col("candidate_tie").cast("long")
            ).alias("with_candidate_tie"),
        )
        .orderBy("year")
    )

    by_year = []

    for row in by_year_df.collect():
        values = row.asDict()
        total_y = int(values["focal_inventor_years"] or 0)
        valid_y = int(values["valid_cell_inventor_years"] or 0)
        candidate_y = int(values["with_candidate"] or 0)
        unique_y = int(values["with_unique_candidate"] or 0)
        tie_y = int(values["with_candidate_tie"] or 0)

        by_year.append({
            "rule": rule_name,
            **values,
            "coverage_rate_all_focals": (
                candidate_y / total_y if total_y else None
            ),
            "coverage_rate_valid_cells": (
                candidate_y / valid_y if valid_y else None
            ),
            "unique_rate_all_focals": (
                unique_y / total_y if total_y else None
            ),
            "tie_share_conditional_on_candidate": (
                tie_y / candidate_y if candidate_y else None
            ),
        })

    by_seniority_df = (
        audited
        .groupBy("current_seniority")
        .agg(
            F.count("*").alias("focal_inventor_years"),
            F.sum(
                F.col("valid_rule_cell").cast("long")
            ).alias("valid_cell_inventor_years"),
            F.sum(
                F.col("has_candidate").cast("long")
            ).alias("with_candidate"),
            F.sum(
                F.col("unique_candidate").cast("long")
            ).alias("with_unique_candidate"),
            F.sum(
                F.col("candidate_tie").cast("long")
            ).alias("with_candidate_tie"),
        )
        .orderBy("current_seniority")
    )

    by_seniority = []

    for row in by_seniority_df.collect():
        values = row.asDict()
        total_s = int(values["focal_inventor_years"] or 0)
        valid_s = int(values["valid_cell_inventor_years"] or 0)
        candidate_s = int(values["with_candidate"] or 0)
        unique_s = int(values["with_unique_candidate"] or 0)
        tie_s = int(values["with_candidate_tie"] or 0)

        by_seniority.append({
            "rule": rule_name,
            **values,
            "coverage_rate_all_focals": (
                candidate_s / total_s if total_s else None
            ),
            "coverage_rate_valid_cells": (
                candidate_s / valid_s if valid_s else None
            ),
            "unique_rate_all_focals": (
                unique_s / total_s if total_s else None
            ),
            "tie_share_conditional_on_candidate": (
                tie_s / candidate_s if candidate_s else None
            ),
        })

    audited.unpersist()

    return overall, by_year, by_seniority


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .appName("manager_fe_stage0e_rule_comparison")
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
        .config("spark.sql.shuffle.partitions", "192")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"[START] {now_utc()}")
        print(f"[INPUT] {INPUT_PANEL}")
        print(f"[OUTPUT] {out_dir}")

        source = spark.read.parquet(INPUT_PANEL)

        missing = [
            column
            for column in COLUMNS
            if column not in source.columns
        ]

        if missing:
            raise RuntimeError(
                "Missing required columns: " + ", ".join(missing)
            )

        panel = (
            source
            .select(*COLUMNS)
            .withColumn("year", F.col("year").cast("int"))
            .withColumn(
                "n_patents",
                F.coalesce(
                    F.col("n_patents").cast("double"),
                    F.lit(0.0),
                ),
            )
            .withColumn(
                "current_seniority",
                F.col("current_seniority").cast("double"),
            )
            .withColumn(
                "current_position_overlap_days",
                F.col(
                    "current_position_overlap_days"
                ).cast("double"),
            )
            .withColumn(
                "combined_title",
                F.lower(
                    F.concat_ws(
                        " ",
                        F.coalesce(
                            F.col("current_title_raw").cast(
                                "string"
                            ),
                            F.lit(""),
                        ),
                        F.coalesce(
                            F.col(
                                "current_title_translated"
                            ).cast("string"),
                            F.lit(""),
                        ),
                    )
                ),
            )
            .withColumn(
                "manager_title",
                F.col("combined_title").rlike(
                    MANAGER_PATTERN
                ),
            )
        )

        history_window = (
            Window.partitionBy("user_id")
            .orderBy("year")
            .rowsBetween(Window.unboundedPreceding, -1)
        )

        panel = panel.withColumn(
            "prior_patents",
            F.coalesce(
                F.sum("n_patents").over(history_window),
                F.lit(0.0),
            ),
        )

        period = panel.filter(
            F.col("year").between(YEAR_MIN, YEAR_MAX)
        )

        observed_us_pool = (
            period
            .filter(is_one("current_position_observed"))
            .filter(~is_one("position_carried_forward"))
            .filter(is_one("position_measures_observed"))
            .filter(F.col("current_position_overlap_days") > 0)
            .filter(is_us("current_country"))
            .filter(F.col("current_position_id").isNotNull())
            .filter(F.col("current_parent_rcid").isNotNull())
            .filter(
                F.col("current_metro_area").isNotNull()
                & (F.trim(F.col("current_metro_area")) != "")
            )
            .filter(F.col("current_seniority").isNotNull())
            .cache()
        )

        # Candidate pool: all observed U.S. inventors, without imposing the
        # focal sample's education restriction.
        candidate_pool = observed_us_pool

        # Focal regression sample.
        focal = (
            observed_us_pool
            .filter(is_us("last_university_country"))
            .cache()
        )

        candidate_pool_count = candidate_pool.count()
        focal_count = focal.count()

        population_summary_row = (
            candidate_pool
            .agg(
                F.count("*").alias(
                    "candidate_pool_inventor_years"
                ),
                F.countDistinct("user_id").alias(
                    "candidate_pool_inventors"
                ),
                F.sum(
                    is_us("last_university_country").cast(
                        "long"
                    )
                ).alias(
                    "candidate_rows_in_focal_education_sample"
                ),
                F.sum(
                    (F.col("prior_patents") >= 1).cast(
                        "long"
                    )
                ).alias(
                    "candidate_rows_with_prior_patent"
                ),
                F.sum(
                    F.col("manager_title").cast("long")
                ).alias(
                    "candidate_rows_with_manager_title"
                ),
                F.sum(
                    (
                        (F.col("prior_patents") >= 1)
                        & F.col("manager_title")
                    ).cast("long")
                ).alias(
                    "prior_patenting_manager_title_rows"
                ),
                F.sum(
                    F.col("current_rcid").isNotNull().cast(
                        "long"
                    )
                ).alias(
                    "candidate_rows_with_current_rcid"
                ),
            )
            .collect()[0]
            .asDict()
        )

        population_summary_row[
            "focal_sample_inventor_years"
        ] = focal_count

        pd.DataFrame([population_summary_row]).to_csv(
            out_dir / "population_summary.csv",
            index=False,
        )

        rules = [
            {
                "name": "parent_all_broad_candidates",
                "candidate_source": candidate_pool,
                "cells": [
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                ],
                "manager_title": False,
            },
            {
                "name": "parent_all_focal_education_candidates",
                "candidate_source": focal,
                "cells": [
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                ],
                "manager_title": False,
            },
            {
                "name": "rcid_all_broad_candidates",
                "candidate_source": candidate_pool,
                "cells": [
                    "current_rcid",
                    "current_metro_area",
                    "year",
                ],
                "manager_title": False,
            },
            {
                "name": "parent_manager_title_broad_candidates",
                "candidate_source": candidate_pool,
                "cells": [
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                ],
                "manager_title": True,
            },
            {
                "name": "rcid_manager_title_broad_candidates",
                "candidate_source": candidate_pool,
                "cells": [
                    "current_rcid",
                    "current_metro_area",
                    "year",
                ],
                "manager_title": True,
            },
        ]

        overall_rows: list[dict] = []
        year_rows: list[dict] = []
        seniority_rows: list[dict] = []

        for rule in rules:
            print(f"[RULE] {rule['name']}")

            overall, by_year, by_seniority = summarize_rule(
                focal=focal,
                candidate_source=rule["candidate_source"],
                rule_name=rule["name"],
                cell_columns=rule["cells"],
                require_manager_title=rule[
                    "manager_title"
                ],
            )

            overall_rows.extend(overall)
            year_rows.extend(by_year)
            seniority_rows.extend(by_seniority)

        overall_frame = pd.DataFrame(overall_rows)
        overall_frame.to_csv(
            out_dir / "rule_comparison_overall.csv",
            index=False,
        )

        pd.DataFrame(year_rows).to_csv(
            out_dir / "rule_comparison_by_year.csv",
            index=False,
        )

        pd.DataFrame(seniority_rows).to_csv(
            out_dir / "rule_comparison_by_seniority.csv",
            index=False,
        )

        lines = [
            "MANAGER-FE STAGE 0E RULE COMPARISON",
            "=" * 78,
            f"Focal sample: {focal_count:,} inventor-years",
            (
                "Broad U.S. candidate pool: "
                f"{candidate_pool_count:,} inventor-years"
            ),
            "",
        ]

        for row in overall_rows:
            lines.extend([
                row["rule"],
                (
                    "  Candidate coverage: "
                    f"{row['coverage_rate_all_focals']:.2%}"
                ),
                (
                    "  Unique assignment: "
                    f"{row['unique_rate_all_focals']:.2%}"
                ),
                (
                    "  Unique among candidate cases: "
                    f"{row['unique_share_conditional_on_candidate']:.2%}"
                    if row[
                        "unique_share_conditional_on_candidate"
                    ] is not None
                    else "  Unique among candidate cases: NA"
                ),
                (
                    "  Tied among candidate cases: "
                    f"{row['tie_share_conditional_on_candidate']:.2%}"
                    if row[
                        "tie_share_conditional_on_candidate"
                    ] is not None
                    else "  Tied among candidate cases: NA"
                ),
                "",
            ])

        lines.extend([
            "No manager identity was assigned.",
            "No exact spell-overlap test was performed.",
            "No fixed-effects model was estimated.",
        ])

        (out_dir / "decision_gate.txt").write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )

        metadata = {
            "created_utc": now_utc(),
            "input_panel": INPUT_PANEL,
            "focal_definition": (
                "Observed U.S. current position, last university "
                "country U.S., 2000-2019"
            ),
            "candidate_pool_definition": (
                "All observed U.S. inventor-years regardless of "
                "education, 2000-2019"
            ),
            "candidate_patent_requirement": (
                "Cumulative patents through t-1 >= 1"
            ),
            "strictly_higher_seniority_required": True,
            "rules": [rule["name"] for rule in rules],
            "manager_identity_assigned": False,
            "exact_spell_overlap_tested": False,
            "fixed_effects_estimated": False,
        }

        (out_dir / "metadata.json").write_text(
            json.dumps(metadata, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        print()
        print("\n".join(lines))
        print(f"[COMPLETE] {out_dir}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
