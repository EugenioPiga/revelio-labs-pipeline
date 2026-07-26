#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import os
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

REQUIRED_COLUMNS = [
    "user_id",
    "year",
    "n_patents",
    "current_position_id",
    "current_position_observed",
    "current_position_overlap_days",
    "position_carried_forward",
    "position_measures_observed",
    "current_parent_rcid",
    "current_metro_area",
    "current_country",
    "current_seniority",
    "last_university_country",
    "edu_last_university_country",
]


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


def is_us_country(column_name: str):
    return normalized_country(column_name).isin(
        "us",
        "usa",
        "unitedstates",
        "unitedstatesofamerica",
    )


def bool_one(column_name: str):
    return F.coalesce(F.col(column_name).cast("int"), F.lit(0)) == 1


def write_small_csv(df: DataFrame, path: Path) -> None:
    frame = df.toPandas()
    frame.to_csv(path, index=False)


def cell_size_bin():
    return (
        F.when(F.col("cell_size") == 1, "1")
        .when(F.col("cell_size") == 2, "2")
        .when(F.col("cell_size").between(3, 5), "3-5")
        .when(F.col("cell_size").between(6, 10), "6-10")
        .when(F.col("cell_size").between(11, 25), "11-25")
        .when(F.col("cell_size").between(26, 50), "26-50")
        .when(F.col("cell_size").between(51, 100), "51-100")
        .when(F.col("cell_size").between(101, 250), "101-250")
        .when(F.col("cell_size").between(251, 500), "251-500")
        .otherwise("501+")
    )


def add_flow(
    records: list[dict],
    step: int,
    label: str,
    df: DataFrame,
) -> None:
    n_rows = df.count()
    records.append(
        {
            "step": step,
            "restriction": label,
            "inventor_years": n_rows,
        }
    )
    print(f"[FLOW] {step:02d} | {label}: {n_rows:,}")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--out-dir", required=True)
    args = parser.parse_args()

    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=False)

    spark = (
        SparkSession.builder
        .appName("manager_fe_stage0d_sample_candidate_audit")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.shuffle.partitions", "192")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        print(f"[START] {now_utc()}")
        print(f"[INPUT] {INPUT_PANEL}")
        print(f"[OUTPUT] {out_dir}")

        schema_df = spark.read.parquet(INPUT_PANEL)
        missing = [
            column
            for column in REQUIRED_COLUMNS
            if column not in schema_df.columns
        ]

        if missing:
            raise RuntimeError(
                "Missing required columns: " + ", ".join(missing)
            )

        panel = (
            schema_df
            .select(*REQUIRED_COLUMNS)
            .withColumn("year", F.col("year").cast("int"))
            .withColumn(
                "n_patents",
                F.coalesce(F.col("n_patents").cast("double"), F.lit(0.0)),
            )
            .withColumn(
                "current_seniority",
                F.col("current_seniority").cast("double"),
            )
            .withColumn(
                "current_position_overlap_days",
                F.col("current_position_overlap_days").cast("double"),
            )
        )

        # Predetermined patent history. History before 2000 is retained when
        # constructing this variable.
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

        flow: list[dict] = []

        add_flow(flow, 1, "Years 2000-2019", period)

        observed = period.filter(
            bool_one("current_position_observed")
        )
        add_flow(flow, 2, "Current position directly observed", observed)

        not_carried = observed.filter(
            ~bool_one("position_carried_forward")
        )
        add_flow(flow, 3, "Position not carried forward", not_carried)

        measures_observed = not_carried.filter(
            bool_one("position_measures_observed")
        )
        add_flow(
            flow,
            4,
            "Current position measures observed",
            measures_observed,
        )

        positive_overlap = measures_observed.filter(
            F.col("current_position_overlap_days") > 0
        )
        add_flow(
            flow,
            5,
            "Positive current-position overlap within year",
            positive_overlap,
        )

        us_year = positive_overlap.filter(
            is_us_country("current_country")
        )
        add_flow(flow, 6, "Current employment country is U.S.", us_year)

        us_education = us_year.filter(
            is_us_country("last_university_country")
        )
        add_flow(
            flow,
            7,
            "Last observed university country is U.S.",
            us_education,
        )

        valid_position = us_education.filter(
            F.col("current_position_id").isNotNull()
        )
        add_flow(flow, 8, "Valid current position ID", valid_position)

        valid_parent = valid_position.filter(
            F.col("current_parent_rcid").isNotNull()
        )
        add_flow(flow, 9, "Valid current parent firm", valid_parent)

        valid_metro = valid_parent.filter(
            F.col("current_metro_area").isNotNull()
            & (F.trim(F.col("current_metro_area")) != "")
        )
        add_flow(flow, 10, "Valid current metro", valid_metro)

        valid_seniority = valid_metro.filter(
            F.col("current_seniority").isNotNull()
        )
        add_flow(flow, 11, "Valid current seniority", valid_seniority)

        sample = valid_seniority.cache()
        n_sample = sample.count()

        pd.DataFrame(flow).to_csv(
            out_dir / "sample_flow.csv",
            index=False,
        )

        # Check the alternative education variable without using it to define
        # the baseline sample.
        education_check = (
            us_year
            .select(
                is_us_country("last_university_country")
                .alias("last_university_country_us"),
                is_us_country("edu_last_university_country")
                .alias("edu_last_university_country_us"),
                F.col("last_university_country").isNull()
                .alias("last_university_country_missing"),
                F.col("edu_last_university_country").isNull()
                .alias("edu_last_university_country_missing"),
            )
            .agg(
                F.count("*").alias("us_employment_years"),
                F.sum(
                    F.col("last_university_country_us").cast("long")
                ).alias("main_last_education_us"),
                F.sum(
                    F.col("edu_last_university_country_us").cast("long")
                ).alias("alternative_last_education_us"),
                F.sum(
                    (
                        F.col("last_university_country_us")
                        != F.col("edu_last_university_country_us")
                    ).cast("long")
                ).alias("education_us_indicator_disagreements"),
                F.sum(
                    F.col("last_university_country_missing").cast("long")
                ).alias("main_last_education_missing"),
                F.sum(
                    F.col(
                        "edu_last_university_country_missing"
                    ).cast("long")
                ).alias("alternative_last_education_missing"),
            )
        )

        write_small_csv(
            education_check,
            out_dir / "education_variable_check.csv",
        )

        # Full uniqueness check in the final audit sample.
        duplicate_user_years = (
            sample
            .groupBy("user_id", "year")
            .count()
            .filter(F.col("count") > 1)
            .count()
        )

        # Parent × metro × year is the agreed organizational cell.
        cell_counts = (
            sample
            .groupBy(
                "current_parent_rcid",
                "current_metro_area",
                "year",
            )
            .agg(
                F.count("*").alias("cell_rows"),
                F.countDistinct("user_id").alias("cell_size"),
            )
            .cache()
        )

        sample_cells = (
            sample
            .join(
                cell_counts.select(
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                    "cell_size",
                ),
                on=[
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                ],
                how="left",
            )
            .withColumn("cell_size_bin", cell_size_bin())
            .cache()
        )

        cell_distribution = (
            cell_counts
            .withColumn("cell_size_bin", cell_size_bin())
            .groupBy("cell_size_bin")
            .agg(
                F.count("*").alias("parent_metro_year_cells"),
                F.sum("cell_size").alias("inventor_years"),
                F.avg("cell_size").alias("mean_cell_size"),
                F.expr("percentile_approx(cell_size, 0.5)")
                .alias("median_cell_size"),
                F.max("cell_size").alias("maximum_cell_size"),
            )
            .orderBy(
                F.when(F.col("cell_size_bin") == "1", 1)
                .when(F.col("cell_size_bin") == "2", 2)
                .when(F.col("cell_size_bin") == "3-5", 3)
                .when(F.col("cell_size_bin") == "6-10", 4)
                .when(F.col("cell_size_bin") == "11-25", 5)
                .when(F.col("cell_size_bin") == "26-50", 6)
                .when(F.col("cell_size_bin") == "51-100", 7)
                .when(F.col("cell_size_bin") == "101-250", 8)
                .when(F.col("cell_size_bin") == "251-500", 9)
                .otherwise(10)
            )
        )

        write_small_csv(
            cell_distribution,
            out_dir / "workplace_cell_distribution.csv",
        )

        yearly_sample = (
            sample_cells
            .groupBy("year")
            .agg(
                F.count("*").alias("inventor_years"),
                F.countDistinct("user_id").alias("inventors"),
                F.countDistinct(
                    F.struct(
                        "current_parent_rcid",
                        "current_metro_area",
                    )
                ).alias("parent_metro_cells"),
                F.avg("cell_size").alias("mean_cell_size"),
                F.sum((F.col("cell_size") == 1).cast("long"))
                .alias("inventor_years_in_singleton_cells"),
                F.sum((F.col("prior_patents") >= 1).cast("long"))
                .alias("inventor_years_with_prior_patent"),
            )
            .orderBy("year")
        )

        write_small_csv(
            yearly_sample,
            out_dir / "yearly_sample_coverage.csv",
        )

        seniority_distribution = (
            sample_cells
            .groupBy("current_seniority")
            .agg(
                F.count("*").alias("inventor_years"),
                F.countDistinct("user_id").alias("inventors"),
                F.avg("prior_patents").alias("mean_prior_patents"),
                F.sum((F.col("prior_patents") >= 1).cast("long"))
                .alias("inventor_years_eligible_as_manager"),
            )
            .orderBy("current_seniority")
        )

        write_small_csv(
            seniority_distribution,
            out_dir / "seniority_distribution.csv",
        )

        # Eligible manager candidates have at least one patent before year t.
        eligible_by_level = (
            sample_cells
            .filter(F.col("prior_patents") >= 1)
            .groupBy(
                "current_parent_rcid",
                "current_metro_area",
                "year",
                "current_seniority",
            )
            .agg(
                F.countDistinct("user_id")
                .alias("n_eligible_at_level")
            )
        )

        manager_level_arrays = (
            eligible_by_level
            .groupBy(
                "current_parent_rcid",
                "current_metro_area",
                "year",
            )
            .agg(
                F.sort_array(
                    F.collect_list(
                        F.struct(
                            F.col("current_seniority").alias(
                                "candidate_seniority"
                            ),
                            F.col("n_eligible_at_level").alias(
                                "candidate_count"
                            ),
                        )
                    )
                ).alias("eligible_manager_levels")
            )
        )

        candidate_audit = (
            sample_cells
            .join(
                manager_level_arrays,
                on=[
                    "current_parent_rcid",
                    "current_metro_area",
                    "year",
                ],
                how="left",
            )
            .withColumn(
                "closest_higher_level",
                F.expr(
                    """
                    element_at(
                      filter(
                        eligible_manager_levels,
                        x -> x.candidate_seniority > current_seniority
                      ),
                      1
                    )
                    """
                ),
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
                "has_eligible_candidate",
                F.col("closest_higher_seniority").isNotNull(),
            )
            .withColumn(
                "unique_candidate_at_level",
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

        overall_candidate = (
            candidate_audit
            .agg(
                F.count("*").alias("inventor_years"),
                F.sum(
                    F.col("has_eligible_candidate").cast("long")
                ).alias("with_eligible_candidate"),
                F.sum(
                    (~F.col("has_eligible_candidate")).cast("long")
                ).alias("without_eligible_candidate"),
                F.sum(
                    F.col("unique_candidate_at_level").cast("long")
                ).alias("with_exactly_one_candidate_at_closest_level"),
                F.sum(
                    F.col("candidate_tie").cast("long")
                ).alias("with_multiple_candidates_at_closest_level"),
                F.avg(
                    F.when(
                        F.col("has_eligible_candidate"),
                        F.col("seniority_gap"),
                    )
                ).alias("mean_seniority_gap"),
                F.expr(
                    """
                    percentile_approx(
                      CASE
                        WHEN has_eligible_candidate THEN seniority_gap
                      END,
                      0.5
                    )
                    """
                ).alias("median_seniority_gap"),
                F.max("n_candidates_at_closest_level")
                .alias("maximum_candidates_at_closest_level"),
            )
        )

        write_small_csv(
            overall_candidate,
            out_dir / "candidate_coverage_overall.csv",
        )

        candidate_by_year = (
            candidate_audit
            .groupBy("year")
            .agg(
                F.count("*").alias("inventor_years"),
                F.sum(
                    F.col("has_eligible_candidate").cast("long")
                ).alias("with_eligible_candidate"),
                F.sum(
                    F.col("unique_candidate_at_level").cast("long")
                ).alias("with_unique_candidate"),
                F.sum(
                    F.col("candidate_tie").cast("long")
                ).alias("with_candidate_tie"),
                F.avg("seniority_gap").alias("mean_seniority_gap"),
            )
            .withColumn(
                "candidate_coverage_rate",
                F.col("with_eligible_candidate")
                / F.col("inventor_years"),
            )
            .withColumn(
                "unique_candidate_rate",
                F.col("with_unique_candidate")
                / F.col("inventor_years"),
            )
            .withColumn(
                "tie_rate",
                F.col("with_candidate_tie")
                / F.col("inventor_years"),
            )
            .orderBy("year")
        )

        write_small_csv(
            candidate_by_year,
            out_dir / "candidate_coverage_by_year.csv",
        )

        candidate_by_cell_size = (
            candidate_audit
            .groupBy("cell_size_bin")
            .agg(
                F.count("*").alias("inventor_years"),
                F.sum(
                    F.col("has_eligible_candidate").cast("long")
                ).alias("with_eligible_candidate"),
                F.sum(
                    F.col("unique_candidate_at_level").cast("long")
                ).alias("with_unique_candidate"),
                F.sum(
                    F.col("candidate_tie").cast("long")
                ).alias("with_candidate_tie"),
                F.avg("n_candidates_at_closest_level")
                .alias("mean_candidates_at_closest_level"),
            )
            .withColumn(
                "candidate_coverage_rate",
                F.col("with_eligible_candidate")
                / F.col("inventor_years"),
            )
            .withColumn(
                "unique_candidate_rate",
                F.col("with_unique_candidate")
                / F.col("inventor_years"),
            )
            .withColumn(
                "tie_rate",
                F.col("with_candidate_tie")
                / F.col("inventor_years"),
            )
        )

        write_small_csv(
            candidate_by_cell_size,
            out_dir / "candidate_coverage_by_cell_size.csv",
        )

        candidate_by_seniority = (
            candidate_audit
            .groupBy("current_seniority")
            .agg(
                F.count("*").alias("inventor_years"),
                F.sum(
                    F.col("has_eligible_candidate").cast("long")
                ).alias("with_eligible_candidate"),
                F.sum(
                    F.col("unique_candidate_at_level").cast("long")
                ).alias("with_unique_candidate"),
                F.sum(
                    F.col("candidate_tie").cast("long")
                ).alias("with_candidate_tie"),
                F.avg("seniority_gap").alias("mean_seniority_gap"),
            )
            .withColumn(
                "candidate_coverage_rate",
                F.col("with_eligible_candidate")
                / F.col("inventor_years"),
            )
            .withColumn(
                "tie_rate",
                F.col("with_candidate_tie")
                / F.col("inventor_years"),
            )
            .orderBy("current_seniority")
        )

        write_small_csv(
            candidate_by_seniority,
            out_dir / "candidate_coverage_by_seniority.csv",
        )

        overall_row = overall_candidate.collect()[0].asDict()

        n_with_candidate = int(
            overall_row.get("with_eligible_candidate") or 0
        )
        n_unique = int(
            overall_row.get(
                "with_exactly_one_candidate_at_closest_level"
            )
            or 0
        )
        n_ties = int(
            overall_row.get(
                "with_multiple_candidates_at_closest_level"
            )
            or 0
        )

        decision_summary = {
            "created_utc": now_utc(),
            "input_panel": INPUT_PANEL,
            "period": [YEAR_MIN, YEAR_MAX],
            "firm_definition": "current_parent_rcid",
            "location_definition": "current_metro_area",
            "organizational_cell": (
                "current_parent_rcid x current_metro_area x year"
            ),
            "position_definition": "current_position_id",
            "employment_restriction": (
                "directly observed current U.S. position; "
                "not carried forward"
            ),
            "education_restriction": (
                "last_university_country is United States"
            ),
            "manager_patent_requirement": (
                "cumulative patents through t-1 >= 1"
            ),
            "final_audit_sample_inventor_years": n_sample,
            "duplicate_user_year_keys": duplicate_user_years,
            "with_eligible_candidate": n_with_candidate,
            "with_unique_candidate_at_closest_level": n_unique,
            "with_candidate_tie": n_ties,
            "candidate_coverage_rate": (
                n_with_candidate / n_sample if n_sample else None
            ),
            "unique_candidate_rate": (
                n_unique / n_sample if n_sample else None
            ),
            "tie_rate": (
                n_ties / n_sample if n_sample else None
            ),
            "exact_spell_overlap_tested": False,
            "manager_identity_assigned": False,
            "fixed_effects_estimated": False,
            "next_gate": (
                "Review coverage and ties before constructing "
                "manager identities or testing exact spell overlap."
            ),
        }

        (out_dir / "decision_summary.json").write_text(
            json.dumps(decision_summary, indent=2, sort_keys=True),
            encoding="utf-8",
        )

        lines = [
            "MANAGER-FE STAGE 0D DECISION GATE",
            "=" * 72,
            f"Final audit sample: {n_sample:,} inventor-years",
            f"Duplicate user-year keys: {duplicate_user_years:,}",
            (
                "Eligible higher-seniority candidate: "
                f"{n_with_candidate:,} "
                f"({decision_summary['candidate_coverage_rate']:.2%})"
                if n_sample
                else "Eligible higher-seniority candidate: unavailable"
            ),
            (
                "Exactly one candidate at closest level: "
                f"{n_unique:,} "
                f"({decision_summary['unique_candidate_rate']:.2%})"
                if n_sample
                else "Unique candidate rate: unavailable"
            ),
            (
                "Multiple candidates at closest level: "
                f"{n_ties:,} "
                f"({decision_summary['tie_rate']:.2%})"
                if n_sample
                else "Tie rate: unavailable"
            ),
            "",
            "No manager identity was assigned.",
            "No exact pairwise spell-overlap test was run.",
            "No fixed-effects model was estimated.",
        ]

        (out_dir / "decision_gate.txt").write_text(
            "\n".join(lines) + "\n",
            encoding="utf-8",
        )

        print()
        print("\n".join(lines))
        print()
        print(f"[COMPLETE] Outputs written to {out_dir}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
