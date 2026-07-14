#!/usr/bin/env python3

import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


SINGLE_KEY_CANDIDATES = [
    "revelio_entity_location_key",
    "entity_location_key",
    "revelio_key",
    "entity_location_id",
    "restaurant_entity_location_key",
]

COMPOSITE_KEY_CANDIDATES = [
    ["rcid", "company_name", "city", "state"],
    [
        "rcid",
        "revelio_company_name",
        "revelio_city",
        "revelio_state",
    ],
    [
        "revelio_rcid",
        "revelio_company_name",
        "revelio_city",
        "revelio_state",
    ],
]

BACKBONE_ID_CANDIDATES = [
    "restaurant_backbone_id",
    "backbone_id",
]


def lookup(df):
    return {column.lower(): column for column in df.columns}


def choose_columns(df, candidates):
    cols = lookup(df)

    for candidate in candidates:
        actual = cols.get(candidate.lower())
        if actual is not None:
            return actual

    return None


def choose_common_key(left, right):
    left_lookup = lookup(left)
    right_lookup = lookup(right)

    for candidate in SINGLE_KEY_CANDIDATES:
        key = candidate.lower()

        if key in left_lookup and key in right_lookup:
            return [
                left_lookup[key],
                right_lookup[key],
            ], "single"

    for group in COMPOSITE_KEY_CANDIDATES:
        if all(
            item.lower() in left_lookup
            and item.lower() in right_lookup
            for item in group
        ):
            return [
                [left_lookup[item.lower()] for item in group],
                [right_lookup[item.lower()] for item in group],
            ], "composite"

    raise RuntimeError(
        "Could not identify a common Revelio key. "
        f"Left columns={left.columns}; right columns={right.columns}"
    )


def add_key(df, columns):
    if isinstance(columns, str):
        return df.withColumn(
            "_revelio_key",
            F.col(columns).cast("string"),
        )

    pieces = [
        F.coalesce(
            F.trim(F.col(column).cast("string")),
            F.lit("<NULL>"),
        )
        for column in columns
    ]

    return df.withColumn(
        "_revelio_key",
        F.concat_ws("||", *pieces),
    )


def schema_record(df):
    return {
        "columns": df.columns,
        "schema": df.schema.jsonValue(),
    }


def count_distinct_key(df):
    return df.select("_revelio_key").distinct().count()


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--old-crosswalk", required=True)
    parser.add_argument("--hier-accepted", required=True)
    parser.add_argument("--hier-ambiguous", required=True)
    parser.add_argument("--hier-unmatched", required=True)
    parser.add_argument("--backbone", required=True)
    parser.add_argument("--spells", required=True)
    parser.add_argument("--origin-v4", required=True)
    parser.add_argument("--outdir", required=True)

    args = parser.parse_args()

    outdir = Path(args.outdir)

    if outdir.exists():
        raise RuntimeError(
            f"Refusing to overwrite existing output: {outdir}"
        )

    outdir.mkdir(parents=True)

    spark = (
        SparkSession.builder
        .appName("food722_completion_preflight_reconcile")
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
        old = spark.read.parquet(args.old_crosswalk)
        accepted = spark.read.parquet(args.hier_accepted)
        ambiguous = spark.read.parquet(args.hier_ambiguous)
        unmatched = spark.read.parquet(args.hier_unmatched)
        backbone = spark.read.parquet(args.backbone)
        spells = spark.read.parquet(args.spells)
        origin = spark.read.parquet(args.origin_v4)

        schemas = {
            "old_crosswalk": schema_record(old),
            "hierarchical_accepted": schema_record(accepted),
            "hierarchical_ambiguous": schema_record(ambiguous),
            "hierarchical_unmatched": schema_record(unmatched),
            "backbone": schema_record(backbone),
            "spells": schema_record(spells),
            "origin_v4": schema_record(origin),
        }

        key_spec, key_type = choose_common_key(old, accepted)

        if key_type == "single":
            old_key_col, accepted_key_col = key_spec
        else:
            old_key_col, accepted_key_col = key_spec

        old = add_key(old, old_key_col)
        accepted = add_key(accepted, accepted_key_col)

        amb_key_spec, amb_key_type = choose_common_key(
            accepted,
            ambiguous,
        )

        if amb_key_type == "single":
            accepted_amb_key, ambiguous_key = amb_key_spec
        else:
            accepted_amb_key, ambiguous_key = amb_key_spec

        ambiguous = add_key(ambiguous, ambiguous_key)

        unmatched_key_spec, unmatched_key_type = choose_common_key(
            accepted,
            unmatched,
        )

        if unmatched_key_type == "single":
            accepted_unmatched_key, unmatched_key = (
                unmatched_key_spec
            )
        else:
            accepted_unmatched_key, unmatched_key = (
                unmatched_key_spec
            )

        unmatched = add_key(unmatched, unmatched_key)

        tier_order_col = choose_columns(
            accepted,
            ["match_tier_order", "tier_order"],
        )

        tier_name_col = choose_columns(
            accepted,
            ["match_tier", "tier"],
        )

        if tier_order_col is not None:
            census = accepted.where(
                F.col(tier_order_col).cast("int") <= 2
            )
        elif tier_name_col is not None:
            census = accepted.where(
                F.lower(F.col(tier_name_col)).contains(
                    "census"
                )
            )
        else:
            raise RuntimeError(
                "Could not identify match tier in hierarchical "
                "accepted crosswalk."
            )

        old_keys = old.select("_revelio_key").distinct()
        census_keys = census.select("_revelio_key").distinct()
        all_accepted_keys = accepted.select(
            "_revelio_key"
        ).distinct()
        ambiguous_keys = ambiguous.select(
            "_revelio_key"
        ).distinct()
        unmatched_keys = unmatched.select(
            "_revelio_key"
        ).distinct()

        old_only_census = old_keys.join(
            census_keys,
            "_revelio_key",
            "left_anti",
        )

        census_only_new = census_keys.join(
            old_keys,
            "_revelio_key",
            "left_anti",
        )

        old_only_detail = (
            old_only_census
            .join(
                old,
                "_revelio_key",
                "left",
            )
            .join(
                all_accepted_keys.withColumn(
                    "accepted_elsewhere",
                    F.lit(1),
                ),
                "_revelio_key",
                "left",
            )
            .join(
                ambiguous_keys.withColumn(
                    "hierarchical_ambiguous",
                    F.lit(1),
                ),
                "_revelio_key",
                "left",
            )
            .join(
                unmatched_keys.withColumn(
                    "hierarchical_unmatched",
                    F.lit(1),
                ),
                "_revelio_key",
                "left",
            )
            .fillna(
                0,
                [
                    "accepted_elsewhere",
                    "hierarchical_ambiguous",
                    "hierarchical_unmatched",
                ],
            )
        )

        old_only_detail.write.mode(
            "errorifexists"
        ).parquet(
            str(outdir / "old_only_census_detail")
        )

        census_only_new.join(
            census,
            "_revelio_key",
            "left",
        ).write.mode(
            "errorifexists"
        ).parquet(
            str(outdir / "new_census_only_detail")
        )

        backbone_id_old = choose_columns(
            old,
            BACKBONE_ID_CANDIDATES,
        )

        backbone_id_accepted = choose_columns(
            accepted,
            BACKBONE_ID_CANDIDATES,
        )

        summary = {
            "key_detection": {
                "type": key_type,
                "old_columns": old_key_col,
                "hierarchical_columns": accepted_key_col,
                "ambiguous_key_type": amb_key_type,
                "ambiguous_columns": ambiguous_key,
                "unmatched_key_type": unmatched_key_type,
                "unmatched_columns": unmatched_key,
            },
            "detected_columns": {
                "hierarchical_tier_order": tier_order_col,
                "hierarchical_tier_name": tier_name_col,
                "old_backbone_id": backbone_id_old,
                "hierarchical_backbone_id": (
                    backbone_id_accepted
                ),
            },
            "counts": {
                "old_exact_keys": count_distinct_key(old),
                "hierarchical_census_keys": (
                    count_distinct_key(census)
                ),
                "hierarchical_all_accepted_keys": (
                    count_distinct_key(accepted)
                ),
                "old_only_relative_to_hierarchical_census": (
                    old_only_census.count()
                ),
                "new_hierarchical_census_relative_to_old": (
                    census_only_new.count()
                ),
                "old_only_accepted_in_later_city_tier": (
                    old_only_detail.where(
                        F.col("accepted_elsewhere") == 1
                    ).select("_revelio_key").distinct().count()
                ),
                "old_only_now_ambiguous": (
                    old_only_detail.where(
                        F.col("hierarchical_ambiguous") == 1
                    ).select("_revelio_key").distinct().count()
                ),
                "old_only_now_unmatched": (
                    old_only_detail.where(
                        F.col("hierarchical_unmatched") == 1
                    ).select("_revelio_key").distinct().count()
                ),
            },
            "schemas": schemas,
        }

        (
            outdir / "completion_preflight_report.json"
        ).write_text(
            json.dumps(
                summary,
                indent=2,
                sort_keys=True,
                default=str,
            ),
            encoding="utf-8",
        )

        (
            outdir / "_COMPLETION_PREFLIGHT_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

        print(
            json.dumps(
                summary["counts"],
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
