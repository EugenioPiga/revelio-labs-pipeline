#!/usr/bin/env python3
"""
Construct the revised person-level foreign-origin proxy for the selected
NAICS-722 food-service population.

Definition:
    any_education_abroad
        = at least one observed education record with a valid non-U.S. country

    first_job_abroad
        = the person's already constructed first observed job has a valid
          non-U.S. country

    country_of_origin_non_us_proxy
        = any_education_abroad OR first_job_abroad

The first-observed-job construction is inherited from the completed and
validated people_origin_v1 output. All education records are rescanned here.

The country variable attached to any_education_abroad is only a representative
country for descriptive use. It is selected from the earliest observed
non-U.S. education record. The binary proxy does not require that this
representative country be unique.

This is a LinkedIn-history proxy. It is not nationality, citizenship,
immigration status, or verified country of birth.
"""

import argparse
import datetime as dt
import json
import os
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


MISSING_COUNTRY_KEYS = [
    "",
    "empty",
    "null",
    "none",
    "nan",
    "na",
    "n a",
    "missing",
    "unknown",
    "not available",
    "not specified",
    "unspecified",
    "undisclosed",
    "other",
    "unavailable",
]

# Preserve the existing project convention that U.S. territories are treated
# as domestic rather than foreign for this proxy.
US_COUNTRY_KEYS = [
    "united states",
    "united states of america",
    "us",
    "u s",
    "usa",
    "u s a",
    "america",
    "puerto rico",
    "guam",
    "american samoa",
    "northern mariana islands",
    "commonwealth of the northern mariana islands",
    "united states virgin islands",
    "us virgin islands",
    "u s virgin islands",
    "virgin islands us",
    "us minor outlying islands",
]

EDUCATION_USER_CANDIDATES = [
    "user_id",
    "userid",
    "user",
]

EDUCATION_COUNTRY_CANDIDATES = [
    "university_country",
    "country",
    "school_country",
    "institution_country",
    "education_country",
    "location_country",
]

EDUCATION_START_CANDIDATES = [
    "startdate",
    "start_date",
    "education_startdate",
    "education_start_date",
    "start_year",
]

EDUCATION_END_CANDIDATES = [
    "enddate",
    "end_date",
    "education_enddate",
    "education_end_date",
    "end_year",
]

EDUCATION_ID_CANDIDATES = [
    "education_number",
    "education_id",
    "record_id",
    "id",
]


def choose_column(df, candidates, label, required=True):
    lookup = {c.lower(): c for c in df.columns}

    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]

    if required:
        raise RuntimeError(
            f"Could not find {label}. Tried {candidates}. "
            f"Available columns: {df.columns}"
        )

    return None


def optional_column(column_name):
    if column_name is None:
        return F.lit(None).cast("string")
    return F.col(column_name).cast("string")


def country_key(column):
    raw = F.lower(F.trim(column.cast("string")))
    key = F.regexp_replace(raw, r"[^a-z0-9]+", " ")
    key = F.trim(F.regexp_replace(key, r"\s+", " "))

    return (
        F.when(
            raw.isNull()
            | (F.length(raw) == 0)
            | key.isin(*MISSING_COUNTRY_KEYS),
            F.lit(None).cast("string"),
        )
        .otherwise(key)
    )


def parsed_date(column):
    raw = F.trim(column.cast("string"))

    return F.coalesce(
        F.to_date(column),
        F.to_date(raw, "yyyy-MM-dd"),
        F.to_date(raw, "yyyy/MM/dd"),
        F.to_date(raw, "MM/dd/yyyy"),
        F.when(
            raw.rlike(r"^[0-9]{4}$"),
            F.to_date(
                F.concat(raw, F.lit("-01-01")),
                "yyyy-MM-dd",
            ),
        ),
    )


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(
            payload,
            handle,
            indent=2,
            sort_keys=True,
            default=str,
        )


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--origin-v1", required=True)
    parser.add_argument("--education-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--diagnostics-dir", required=True)

    args = parser.parse_args()

    for output_path in [
        args.output_dir,
        args.diagnostics_dir,
    ]:
        if os.path.exists(output_path):
            raise RuntimeError(
                f"Refusing to overwrite existing output: {output_path}"
            )

    if not os.path.isfile(
        os.path.join(args.origin_v1, "_SUCCESS")
    ):
        raise RuntimeError(
            f"Origin-v1 input lacks _SUCCESS: {args.origin_v1}"
        )

    spark = (
        SparkSession.builder
        .appName("foodservice_722_origin_country_v4_anyedu_firstjob")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        .config("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        legacy_raw = (
            spark.read.parquet(args.origin_v1)
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        user_column = choose_column(
            legacy_raw,
            ["user_id"],
            "origin-v1 user identifier",
        )

        legacy_rows = legacy_raw.count()
        legacy_unique_people = (
            legacy_raw.select(user_column)
            .distinct()
            .count()
        )

        if legacy_rows != legacy_unique_people:
            raise RuntimeError(
                "Origin-v1 is not unique at the person level: "
                f"rows={legacy_rows:,}, "
                f"unique_people={legacy_unique_people:,}"
            )

        legacy = legacy_raw

        rename_map = {
            "last_education_country": "last_education_country_v1",
            "last_education_country_key": "last_education_country_key_v1",
            "last_education_observed_date": "last_education_observed_date_v1",
            "last_education_country_is_us": "last_education_country_is_us_v1",
            "country_of_origin": "country_of_origin_v1",
            "country_of_origin_key": "country_of_origin_key_v1",
            "country_of_origin_source": "country_of_origin_source_v1",
            "country_of_origin_non_us_proxy": (
                "country_of_origin_non_us_proxy_v1"
            ),
        }

        for old_name, new_name in rename_map.items():
            if old_name in legacy.columns:
                legacy = legacy.withColumnRenamed(
                    old_name,
                    new_name,
                )

        if "country_of_origin_non_us_proxy_v1" not in legacy.columns:
            raise RuntimeError(
                "Origin-v1 input does not contain the legacy proxy flag."
            )

        legacy = (
            legacy
            .withColumn(
                "_origin_user_id_key",
                F.col(user_column).cast("string"),
            )
            .withColumn(
                "country_of_origin_non_us_proxy_v1",
                F.coalesce(
                    F.col(
                        "country_of_origin_non_us_proxy_v1"
                    ).cast("int"),
                    F.lit(0),
                ),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        selected_people = (
            legacy.select("_origin_user_id_key")
            .distinct()
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        education_raw = spark.read.parquet(
            args.education_path
        )

        education_user = choose_column(
            education_raw,
            EDUCATION_USER_CANDIDATES,
            "education user identifier",
        )

        education_country = choose_column(
            education_raw,
            EDUCATION_COUNTRY_CANDIDATES,
            "education country",
        )

        education_start = choose_column(
            education_raw,
            EDUCATION_START_CANDIDATES,
            "education start date",
            required=False,
        )

        education_end = choose_column(
            education_raw,
            EDUCATION_END_CANDIDATES,
            "education end date",
            required=False,
        )

        education_id = choose_column(
            education_raw,
            EDUCATION_ID_CANDIDATES,
            "education record identifier",
            required=False,
        )

        education = (
            education_raw
            .select(
                F.col(education_user)
                .cast("string")
                .alias("_origin_user_id_key"),
                F.col(education_country)
                .cast("string")
                .alias("_education_country_raw"),
                optional_column(education_start).alias(
                    "_education_start_raw"
                ),
                optional_column(education_end).alias(
                    "_education_end_raw"
                ),
                optional_column(education_id).alias(
                    "_education_record_id"
                ),
            )
            .join(
                selected_people,
                "_origin_user_id_key",
                "inner",
            )
            .withColumn(
                "_education_country_key",
                country_key(
                    F.col("_education_country_raw")
                ),
            )
            .withColumn(
                "_education_country_observed",
                F.col(
                    "_education_country_key"
                ).isNotNull(),
            )
            .withColumn(
                "_education_abroad",
                F.col(
                    "_education_country_key"
                ).isNotNull()
                & (
                    ~F.col(
                        "_education_country_key"
                    ).isin(*US_COUNTRY_KEYS)
                ),
            )
            .withColumn(
                "_education_observed_date",
                F.coalesce(
                    parsed_date(
                        F.col("_education_start_raw")
                    ),
                    parsed_date(
                        F.col("_education_end_raw")
                    ),
                ),
            )
            .withColumn(
                "_education_tiebreaker",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.coalesce(
                            F.col(
                                "_education_record_id"
                            ),
                            F.lit(""),
                        ),
                        F.coalesce(
                            F.col(
                                "_education_country_raw"
                            ),
                            F.lit(""),
                        ),
                        F.coalesce(
                            F.col(
                                "_education_start_raw"
                            ),
                            F.lit(""),
                        ),
                        F.coalesce(
                            F.col(
                                "_education_end_raw"
                            ),
                            F.lit(""),
                        ),
                    ),
                    256,
                ),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        education_summary = (
            education.groupBy("_origin_user_id_key")
            .agg(
                F.count("*").alias(
                    "n_education_records"
                ),
                F.sum(
                    F.when(
                        F.col(
                            "_education_country_observed"
                        ),
                        1,
                    ).otherwise(0)
                ).alias(
                    "n_education_records_with_country"
                ),
                F.sum(
                    F.when(
                        F.col("_education_abroad"),
                        1,
                    ).otherwise(0)
                ).alias(
                    "n_education_records_abroad"
                ),
                F.countDistinct(
                    F.when(
                        F.col("_education_abroad"),
                        F.col(
                            "_education_country_key"
                        ),
                    )
                ).alias(
                    "n_distinct_education_abroad_countries"
                ),
            )
            .withColumn(
                "has_any_education_country_observed",
                F.col(
                    "n_education_records_with_country"
                )
                > 0,
            )
            .withColumn(
                "any_education_abroad",
                (
                    F.col(
                        "n_education_records_abroad"
                    )
                    > 0
                ).cast("int"),
            )
        )

        foreign_education_window = (
            Window.partitionBy(
                "_origin_user_id_key"
            )
            .orderBy(
                F.col(
                    "_education_observed_date"
                ).asc_nulls_last(),
                F.col(
                    "_education_start_raw"
                ).asc_nulls_last(),
                F.col(
                    "_education_end_raw"
                ).asc_nulls_last(),
                F.col(
                    "_education_tiebreaker"
                ).asc(),
            )
        )

        representative_foreign_education = (
            education.where(
                F.col("_education_abroad")
            )
            .withColumn(
                "_foreign_education_rank",
                F.row_number().over(
                    foreign_education_window
                ),
            )
            .where(
                F.col(
                    "_foreign_education_rank"
                )
                == 1
            )
            .select(
                "_origin_user_id_key",
                F.col(
                    "_education_country_raw"
                ).alias(
                    "representative_education_abroad_country"
                ),
                F.col(
                    "_education_country_key"
                ).alias(
                    "representative_education_abroad_country_key"
                ),
                F.col(
                    "_education_observed_date"
                ).alias(
                    "representative_education_abroad_date"
                ),
            )
        )

        result = (
            legacy
            .join(
                education_summary,
                "_origin_user_id_key",
                "left",
            )
            .join(
                representative_foreign_education,
                "_origin_user_id_key",
                "left",
            )
            .withColumn(
                "n_education_records",
                F.coalesce(
                    F.col("n_education_records"),
                    F.lit(0),
                ),
            )
            .withColumn(
                "n_education_records_with_country",
                F.coalesce(
                    F.col(
                        "n_education_records_with_country"
                    ),
                    F.lit(0),
                ),
            )
            .withColumn(
                "n_education_records_abroad",
                F.coalesce(
                    F.col(
                        "n_education_records_abroad"
                    ),
                    F.lit(0),
                ),
            )
            .withColumn(
                "n_distinct_education_abroad_countries",
                F.coalesce(
                    F.col(
                        "n_distinct_education_abroad_countries"
                    ),
                    F.lit(0),
                ),
            )
            .withColumn(
                "has_any_education_country_observed",
                F.coalesce(
                    F.col(
                        "has_any_education_country_observed"
                    ),
                    F.lit(False),
                ),
            )
            .withColumn(
                "any_education_abroad",
                F.coalesce(
                    F.col("any_education_abroad"),
                    F.lit(0),
                ).cast("int"),
            )
            .withColumn(
                "first_observed_job_country_key_v4",
                country_key(
                    F.col(
                        "first_observed_job_country"
                    )
                ),
            )
            .withColumn(
                "has_first_job_country_observed",
                F.col(
                    "first_observed_job_country_key_v4"
                ).isNotNull(),
            )
            .withColumn(
                "first_job_abroad",
                (
                    F.col(
                        "first_observed_job_country_key_v4"
                    ).isNotNull()
                    & (
                        ~F.col(
                            "first_observed_job_country_key_v4"
                        ).isin(*US_COUNTRY_KEYS)
                    )
                ).cast("int"),
            )
            .withColumn(
                "country_of_origin_non_us_proxy",
                (
                    (
                        F.col(
                            "any_education_abroad"
                        )
                        == 1
                    )
                    | (
                        F.col(
                            "first_job_abroad"
                        )
                        == 1
                    )
                ).cast("int"),
            )
            .withColumn(
                "country_of_origin_source",
                F.when(
                    (
                        F.col(
                            "any_education_abroad"
                        )
                        == 1
                    )
                    & (
                        F.col(
                            "first_job_abroad"
                        )
                        == 1
                    ),
                    F.lit(
                        "both_any_education_abroad_and_first_job_abroad"
                    ),
                )
                .when(
                    F.col(
                        "any_education_abroad"
                    )
                    == 1,
                    F.lit(
                        "any_education_abroad"
                    ),
                )
                .when(
                    F.col(
                        "first_job_abroad"
                    )
                    == 1,
                    F.lit(
                        "first_observed_job_abroad"
                    ),
                )
                .otherwise(
                    F.lit(
                        "no_observed_non_us_signal"
                    )
                ),
            )
            .withColumn(
                "country_of_origin",
                F.when(
                    F.col(
                        "any_education_abroad"
                    )
                    == 1,
                    F.col(
                        "representative_education_abroad_country"
                    ),
                )
                .when(
                    F.col(
                        "first_job_abroad"
                    )
                    == 1,
                    F.col(
                        "first_observed_job_country"
                    ),
                ),
            )
            .withColumn(
                "country_of_origin_key",
                F.when(
                    F.col(
                        "any_education_abroad"
                    )
                    == 1,
                    F.col(
                        "representative_education_abroad_country_key"
                    ),
                )
                .when(
                    F.col(
                        "first_job_abroad"
                    )
                    == 1,
                    F.col(
                        "first_observed_job_country_key_v4"
                    ),
                ),
            )
            .withColumn(
                "country_of_origin_country_source",
                F.when(
                    F.col(
                        "any_education_abroad"
                    )
                    == 1,
                    F.lit(
                        "representative_earliest_observed_foreign_education"
                    ),
                )
                .when(
                    F.col(
                        "first_job_abroad"
                    )
                    == 1,
                    F.lit(
                        "first_observed_job_country"
                    ),
                ),
            )
            .withColumn(
                "country_of_origin_proxy_rule",
                F.lit(
                    "any_education_abroad_or_first_observed_job_abroad"
                ),
            )
            .withColumn(
                "newly_positive_relative_to_v1",
                (
                    (
                        F.col(
                            "country_of_origin_non_us_proxy_v1"
                        )
                        == 0
                    )
                    & (
                        F.col(
                            "country_of_origin_non_us_proxy"
                        )
                        == 1
                    )
                ).cast("int"),
            )
            .withColumn(
                "lost_positive_relative_to_v1",
                (
                    (
                        F.col(
                            "country_of_origin_non_us_proxy_v1"
                        )
                        == 1
                    )
                    & (
                        F.col(
                            "country_of_origin_non_us_proxy"
                        )
                        == 0
                    )
                ).cast("int"),
            )
            .drop("_origin_user_id_key")
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        output_rows = result.count()
        output_unique_people = (
            result.select(user_column)
            .distinct()
            .count()
        )

        if output_rows != legacy_rows:
            raise RuntimeError(
                "Row-count integrity failure: "
                f"legacy={legacy_rows:,}, "
                f"output={output_rows:,}"
            )

        if output_rows != output_unique_people:
            raise RuntimeError(
                "Output is not unique by person: "
                f"rows={output_rows:,}, "
                f"unique_people={output_unique_people:,}"
            )

        summary_row = (
            result.agg(
                F.sum(
                    "any_education_abroad"
                ).alias(
                    "n_any_education_abroad"
                ),
                F.sum(
                    "first_job_abroad"
                ).alias(
                    "n_first_job_abroad"
                ),
                F.sum(
                    "country_of_origin_non_us_proxy"
                ).alias(
                    "n_corrected_proxy_positive"
                ),
                F.sum(
                    "country_of_origin_non_us_proxy_v1"
                ).alias(
                    "n_legacy_proxy_positive"
                ),
                F.sum(
                    "newly_positive_relative_to_v1"
                ).alias(
                    "n_newly_positive_relative_to_v1"
                ),
                F.sum(
                    "lost_positive_relative_to_v1"
                ).alias(
                    "n_lost_positive_relative_to_v1"
                ),
                F.sum(
                    F.when(
                        (
                            F.col(
                                "any_education_abroad"
                            )
                            == 1
                        )
                        & (
                            F.col(
                                "first_job_abroad"
                            )
                            == 1
                        ),
                        1,
                    ).otherwise(0)
                ).alias(
                    "n_both_education_and_first_job_abroad"
                ),
                F.sum(
                    F.when(
                        ~F.col(
                            "has_any_education_country_observed"
                        )
                        & ~F.col(
                            "has_first_job_country_observed"
                        ),
                        1,
                    ).otherwise(0)
                ).alias(
                    "n_no_country_evidence"
                ),
            )
            .first()
            .asDict()
        )

        print("[INFO] Writing corrected person-level origin output.")

        (
            result
            .repartition(256)
            .write
            .mode("errorifexists")
            .parquet(args.output_dir)
        )

        diagnostics = Path(args.diagnostics_dir)
        diagnostics.mkdir(parents=True)

        group_fields = [
            c
            for c in [
                "foodservice_person_group",
                "country_of_origin_source",
            ]
            if c in result.columns
        ]

        if group_fields:
            (
                result.groupBy(*group_fields)
                .agg(
                    F.count("*").alias("n_people"),
                    F.sum(
                        "country_of_origin_non_us_proxy"
                    ).alias(
                        "n_corrected_proxy_positive"
                    ),
                    F.avg(
                        F.col(
                            "country_of_origin_non_us_proxy"
                        ).cast("double")
                    ).alias(
                        "corrected_proxy_share"
                    ),
                )
                .write
                .mode("errorifexists")
                .parquet(
                    str(
                        diagnostics
                        / "proxy_counts_by_group"
                    )
                )
            )

        (
            result.groupBy(
                "country_of_origin_source",
                "country_of_origin_non_us_proxy_v1",
                "country_of_origin_non_us_proxy",
            )
            .agg(
                F.count("*").alias("n_people")
            )
            .write
            .mode("errorifexists")
            .parquet(
                str(
                    diagnostics
                    / "v1_v4_transition_counts"
                )
            )
        )

        (
            result.where(
                F.col(
                    "newly_positive_relative_to_v1"
                )
                == 1
            )
            .select(
                user_column,
                "last_education_country_v1",
                "first_observed_job_country",
                "representative_education_abroad_country",
                "representative_education_abroad_date",
                "n_education_records",
                "n_education_records_with_country",
                "n_education_records_abroad",
                "n_distinct_education_abroad_countries",
                "country_of_origin_source",
            )
            .limit(10000)
            .write
            .mode("errorifexists")
            .parquet(
                str(
                    diagnostics
                    / "newly_positive_sample"
                )
            )
        )

        (
            result.where(
                F.col(
                    "country_of_origin_non_us_proxy"
                )
                == 1
            )
            .groupBy(
                "country_of_origin",
                "country_of_origin_key",
                "country_of_origin_country_source",
            )
            .agg(
                F.count("*").alias("n_people")
            )
            .write
            .mode("errorifexists")
            .parquet(
                str(
                    diagnostics
                    / "country_counts"
                )
            )
        )

        report = {
            "created_utc": dt.datetime.now(
                dt.timezone.utc
            ).isoformat(),
            "input_origin_v1": args.origin_v1,
            "input_education": args.education_path,
            "output_dir": args.output_dir,
            "diagnostics_dir": args.diagnostics_dir,
            "selected_columns": {
                "legacy_user_id": user_column,
                "education_user_id": education_user,
                "education_country": education_country,
                "education_start": education_start,
                "education_end": education_end,
                "education_record_id": education_id,
            },
            "definition": {
                "any_education_abroad": (
                    "At least one observed education record has a valid "
                    "non-U.S. country."
                ),
                "first_job_abroad": (
                    "The previously constructed first observed job has a "
                    "valid non-U.S. country."
                ),
                "corrected_proxy": (
                    "any_education_abroad OR first_job_abroad"
                ),
                "representative_country": (
                    "Earliest observed foreign education country when any "
                    "foreign education exists; otherwise first observed "
                    "foreign job country. This representative country is "
                    "auxiliary and does not alter the binary definition."
                ),
            },
            "counts": {
                "input_rows": int(legacy_rows),
                "input_unique_people": int(
                    legacy_unique_people
                ),
                "output_rows": int(output_rows),
                "output_unique_people": int(
                    output_unique_people
                ),
                **{
                    key: int(value or 0)
                    for key, value in summary_row.items()
                },
            },
            "integrity": {
                "row_count_preserved": (
                    output_rows == legacy_rows
                ),
                "person_uniqueness_valid": (
                    output_rows
                    == output_unique_people
                ),
                "legacy_positive_people_lost": int(
                    summary_row.get(
                        "n_lost_positive_relative_to_v1"
                    )
                    or 0
                ),
            },
            "warning": (
                "This remains a LinkedIn-history proxy and should not be "
                "described as verified immigration status, nationality, "
                "citizenship, or country of birth."
            ),
        }

        write_json(
            diagnostics / "origin_country_v4_manifest.json",
            report,
        )

        (
            diagnostics
            / "_ORIGIN_COUNTRY_V4_DIAGNOSTICS_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

        (
            Path(args.output_dir)
            / "_ORIGIN_COUNTRY_V4_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

        print("[INFO] Corrected origin-v4 build completed.")
        print(json.dumps(report["counts"], indent=2))

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
