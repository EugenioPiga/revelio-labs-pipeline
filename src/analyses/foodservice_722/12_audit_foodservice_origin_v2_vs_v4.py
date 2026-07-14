#!/usr/bin/env python3

import argparse
import json
import os
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


def require_success(path, label):
    marker = os.path.join(path, "_SUCCESS")
    if not os.path.isfile(marker):
        raise RuntimeError(f"{label} lacks _SUCCESS: {marker}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--v2", required=True)
    parser.add_argument("--v4", required=True)
    parser.add_argument("--outdir", required=True)
    args = parser.parse_args()

    outdir = Path(args.outdir)

    if outdir.exists():
        raise RuntimeError(
            f"Refusing to overwrite existing output: {outdir}"
        )

    require_success(args.v2, "origin-v2")
    require_success(args.v4, "origin-v4")

    outdir.mkdir(parents=True)

    spark = (
        SparkSession.builder
        .appName("audit_foodservice_origin_v2_vs_v4")
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        v2 = (
            spark.read.parquet(args.v2)
            .select(
                F.col("user_id").cast("string").alias("user_id"),
                F.coalesce(
                    F.col(
                        "country_of_origin_non_us_proxy"
                    ).cast("int"),
                    F.lit(0),
                ).alias("proxy_v2"),
                F.col("country_of_origin")
                .cast("string")
                .alias("origin_country_v2"),
                F.col("country_of_origin_key")
                .cast("string")
                .alias("origin_country_key_v2"),
                F.col("country_of_origin_source")
                .cast("string")
                .alias("origin_source_v2"),
            )
        )

        v4 = (
            spark.read.parquet(args.v4)
            .select(
                F.col("user_id").cast("string").alias("user_id"),
                F.coalesce(
                    F.col(
                        "country_of_origin_non_us_proxy"
                    ).cast("int"),
                    F.lit(0),
                ).alias("proxy_v4"),
                F.col("any_education_abroad").cast("int"),
                F.col("first_job_abroad").cast("int"),
                F.col("country_of_origin")
                .cast("string")
                .alias("origin_country_v4"),
                F.col("country_of_origin_source")
                .cast("string")
                .alias("origin_source_v4"),
                F.col("first_observed_job_country")
                .cast("string"),
                F.col(
                    "representative_education_abroad_country"
                ).cast("string"),
                F.col(
                    "n_education_records_with_country"
                ).cast("long"),
                F.col(
                    "n_education_records_abroad"
                ).cast("long"),
            )
        )

        v2_rows = v2.count()
        v4_rows = v4.count()

        v2_unique = v2.select("user_id").distinct().count()
        v4_unique = v4.select("user_id").distinct().count()

        joined = (
            v2.join(
                v4,
                "user_id",
                "inner",
            )
            .cache()
        )

        joined_rows = joined.count()

        counts = (
            joined.agg(
                F.sum("proxy_v2").alias("positive_v2"),
                F.sum("proxy_v4").alias("positive_v4"),
                F.sum(
                    F.when(
                        (F.col("proxy_v2") == 1)
                        & (F.col("proxy_v4") == 0),
                        1,
                    ).otherwise(0)
                ).alias("v2_positive_v4_negative"),
                F.sum(
                    F.when(
                        (F.col("proxy_v2") == 0)
                        & (F.col("proxy_v4") == 1),
                        1,
                    ).otherwise(0)
                ).alias("v4_new_positive_relative_to_v2"),
                F.sum(
                    F.when(
                        (F.col("proxy_v2") == 1)
                        & (F.col("proxy_v4") == 1),
                        1,
                    ).otherwise(0)
                ).alias("positive_in_both"),
                F.sum(
                    F.when(
                        (F.col("proxy_v2") == 0)
                        & (F.col("proxy_v4") == 0),
                        1,
                    ).otherwise(0)
                ).alias("negative_in_both"),
            )
            .first()
            .asDict()
        )

        transition_table = [
            row.asDict()
            for row in (
                joined.groupBy(
                    "proxy_v2",
                    "proxy_v4",
                )
                .count()
                .orderBy(
                    "proxy_v2",
                    "proxy_v4",
                )
                .collect()
            )
        ]

        lost = joined.where(
            (F.col("proxy_v2") == 1)
            & (F.col("proxy_v4") == 0)
        )

        gained = joined.where(
            (F.col("proxy_v2") == 0)
            & (F.col("proxy_v4") == 1)
        )

        lost_by_source = [
            row.asDict()
            for row in (
                lost.groupBy(
                    "origin_source_v2",
                    "origin_country_v2",
                    "origin_country_key_v2",
                )
                .count()
                .orderBy(F.desc("count"))
                .limit(100)
                .collect()
            )
        ]

        gained_by_evidence = [
            row.asDict()
            for row in (
                gained.groupBy(
                    "any_education_abroad",
                    "first_job_abroad",
                    "origin_source_v4",
                )
                .count()
                .orderBy(F.desc("count"))
                .collect()
            )
        ]

        report = {
            "rows": {
                "v2_rows": int(v2_rows),
                "v2_unique_people": int(v2_unique),
                "v4_rows": int(v4_rows),
                "v4_unique_people": int(v4_unique),
                "joined_rows": int(joined_rows),
            },
            "counts": {
                key: int(value or 0)
                for key, value in counts.items()
            },
            "transition_table": transition_table,
            "lost_v2_positives_by_source_and_country": (
                lost_by_source
            ),
            "new_v4_positives_by_evidence": (
                gained_by_evidence
            ),
            "validation": {
                "v2_person_uniqueness_valid": (
                    v2_rows == v2_unique
                ),
                "v4_person_uniqueness_valid": (
                    v4_rows == v4_unique
                ),
                "all_people_joined": (
                    joined_rows == v2_rows == v4_rows
                ),
                "v4_is_superset_of_cleaned_v2": (
                    int(
                        counts.get(
                            "v2_positive_v4_negative"
                        )
                        or 0
                    )
                    == 0
                ),
            },
        }

        (
            outdir
            / "origin_v2_vs_v4_audit.json"
        ).write_text(
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )

        (
            lost.limit(10000)
            .coalesce(1)
            .write.mode("errorifexists")
            .parquet(
                str(
                    outdir
                    / "v2_positive_v4_negative_sample"
                )
            )
        )

        (
            gained.limit(10000)
            .coalesce(1)
            .write.mode("errorifexists")
            .parquet(
                str(
                    outdir
                    / "v4_new_positive_sample"
                )
            )
        )

        (
            outdir
            / "_ORIGIN_V2_V4_AUDIT_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
