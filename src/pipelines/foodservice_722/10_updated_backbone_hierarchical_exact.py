#!/usr/bin/env python3

import argparse
import datetime as dt
import json
import os
import unicodedata
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T


INVALID = [
    "",
    "empty",
    "null",
    "none",
    "nan",
    "na",
    "n a",
    "unknown",
    "missing",
    "not available",
    "unavailable",
]


@F.udf(T.StringType())
def unicode_nfkc(value):
    if value is None:
        return None

    return unicodedata.normalize(
        "NFKC",
        str(value),
    ).casefold()


def normalize_text(column):
    value = F.lower(
        F.trim(
            unicode_nfkc(
                column.cast("string")
            )
        )
    )

    value = F.regexp_replace(
        value,
        r"[^\p{L}\p{N}]+",
        " ",
    )

    value = F.trim(
        F.regexp_replace(
            value,
            r"\s+",
            " ",
        )
    )

    return (
        F.when(
            value.isNull(),
            F.lit(None).cast("string"),
        )
        .when(
            F.length(value) == 0,
            F.lit(None).cast("string"),
        )
        .when(
            value.isin(*INVALID),
            F.lit(None).cast("string"),
        )
        .otherwise(value)
    )


def strip_terminal_legal_suffix(column):
    value = F.regexp_replace(
        column,
        (
            r"(?i)(?:\s+(?:"
            r"llc|l l c|inc|incorporated|"
            r"ltd|limited|corp|corporation|"
            r"pllc|llp|lp"
            r"))+$"
        ),
        "",
    )

    value = F.trim(
        F.regexp_replace(
            value,
            r"\s+",
            " ",
        )
    )

    return F.when(
        F.length(value) > 0,
        value,
    ).otherwise(
        F.lit(None).cast("string")
    )


def valid(column_name):
    return (
        F.col(column_name).isNotNull()
        & (
            F.length(
                F.trim(
                    F.col(column_name).cast("string")
                )
            )
            > 0
        )
    )


def assert_success(path, label):
    marker = os.path.join(
        path,
        "_SUCCESS",
    )

    if not os.path.isfile(marker):
        raise RuntimeError(
            f"Missing _SUCCESS for {label}: {marker}"
        )


def write_json(path, payload):
    with open(
        path,
        "w",
        encoding="utf-8",
    ) as handle:
        json.dump(
            payload,
            handle,
            indent=2,
            sort_keys=True,
            default=str,
        )


def union_frames(frames):
    if not frames:
        raise RuntimeError(
            "No DataFrames supplied for union."
        )

    result = frames[0]

    for frame in frames[1:]:
        result = result.unionByName(
            frame,
            allowMissingColumns=True,
        )

    return result


def build_candidates(
    revelio,
    backbone_geography,
    revelio_name_field,
    backbone_name_field,
    stage_name,
    geography_source,
    stage_order,
):
    candidate_pairs = (
        revelio.alias("r")
        .join(
            backbone_geography.alias("b"),
            (
                F.col(
                    f"r.{revelio_name_field}"
                )
                == F.col(
                    f"b.{backbone_name_field}"
                )
            )
            & (
                F.col(
                    "r.revelio_state_canonical"
                )
                == F.col(
                    "b.backbone_state_canonical"
                )
            )
            & (
                F.col(
                    "r.revelio_normalized_city"
                )
                == F.col(
                    "b.backbone_geography_normalized"
                )
            ),
            "inner",
        )
        .select(
            F.col(
                "r.revelio_entity_location_key"
            ),
            F.col("r.revelio_rcid"),
            F.col(
                "r.revelio_raw_company_name"
            ),
            F.col(
                "r.revelio_normalized_company_name"
            ),
            F.col(
                "r.revelio_legal_suffix_normalized_company_name"
            ),
            F.col("r.revelio_city"),
            F.col("r.revelio_state"),
            F.col(
                "r.revelio_state_canonical"
            ),
            F.col(
                "r.revelio_normalized_city"
            ),
            F.col("r.revelio_msa"),
            F.col("r.n_foodservice_spells"),
            F.col("r.n_people"),
            F.col("r.n_core_founder_people"),
            F.col(
                "b.backbone_establishment_id"
            ),
            F.col("b.backbone_raw_name"),
            F.col(
                "b.backbone_normalized_name"
            ),
            F.col(
                "b.backbone_legal_suffix_normalized_name"
            ),
            F.col(
                "b.backbone_geography_raw"
            ),
            F.col(
                "b.backbone_geography_normalized"
            ),
            F.lit(
                geography_source
            ).alias(
                "matched_geography_source"
            ),
            F.lit(
                stage_name
            ).alias("match_tier"),
            F.lit(
                stage_order
            ).cast("int").alias(
                "match_tier_order"
            ),
            F.lit(1.0)
            .cast("double")
            .alias("match_score"),
        )
        .dropDuplicates(
            [
                "revelio_entity_location_key",
                "backbone_establishment_id",
            ]
        )
    )

    backbone_candidate_counts = (
        candidate_pairs.groupBy(
            "revelio_entity_location_key"
        )
        .agg(
            F.countDistinct(
                "backbone_establishment_id"
            ).alias(
                "number_of_backbone_candidates"
            )
        )
    )

    revelio_candidate_counts = (
        candidate_pairs.groupBy(
            "backbone_establishment_id"
        )
        .agg(
            F.countDistinct(
                "revelio_entity_location_key"
            ).alias(
                "number_of_revelio_candidates"
            )
        )
    )

    return (
        candidate_pairs
        .join(
            backbone_candidate_counts,
            "revelio_entity_location_key",
            "left",
        )
        .join(
            revelio_candidate_counts,
            "backbone_establishment_id",
            "left",
        )
        .withColumn(
            "ambiguity_reason",
            F.when(
                (
                    F.col(
                        "number_of_backbone_candidates"
                    )
                    > 1
                )
                & (
                    F.col(
                        "number_of_revelio_candidates"
                    )
                    > 1
                ),
                F.lit(
                    "multiple_backbone_candidates_and_multiple_revelio_keys"
                ),
            )
            .when(
                F.col(
                    "number_of_backbone_candidates"
                )
                > 1,
                F.lit(
                    "multiple_backbone_candidates_for_revelio_key"
                ),
            )
            .when(
                F.col(
                    "number_of_revelio_candidates"
                )
                > 1,
                F.lit(
                    "multiple_revelio_keys_for_backbone_establishment"
                ),
            )
            .otherwise(
                F.lit("not_ambiguous")
            ),
        )
    )


def summarize_stage(
    stage_name,
    geography_source,
    candidate_frame,
    accepted_frame,
    ambiguous_frame,
):
    candidate_count = candidate_frame.count()
    candidate_key_count = (
        candidate_frame.select(
            "revelio_entity_location_key"
        )
        .distinct()
        .count()
    )
    candidate_backbone_count = (
        candidate_frame.select(
            "backbone_establishment_id"
        )
        .distinct()
        .count()
    )

    accepted_count = accepted_frame.count()
    ambiguous_pair_count = ambiguous_frame.count()
    ambiguous_key_count = (
        ambiguous_frame.select(
            "revelio_entity_location_key"
        )
        .distinct()
        .count()
    )

    accepted_spells = (
        accepted_frame.agg(
            F.sum(
                "n_foodservice_spells"
            ).alias("n")
        )
        .first()["n"]
        or 0
    )

    ambiguous_spells = (
        ambiguous_frame.select(
            "revelio_entity_location_key",
            "n_foodservice_spells",
        )
        .dropDuplicates(
            ["revelio_entity_location_key"]
        )
        .agg(
            F.sum(
                "n_foodservice_spells"
            ).alias("n")
        )
        .first()["n"]
        or 0
    )

    return {
        "stage": stage_name,
        "geography_source": geography_source,
        "n_candidate_pairs": int(
            candidate_count
        ),
        "n_candidate_revelio_keys": int(
            candidate_key_count
        ),
        "n_candidate_backbone_establishments": int(
            candidate_backbone_count
        ),
        "n_accepted_pairs": int(
            accepted_count
        ),
        "n_accepted_foodservice_spells": int(
            accepted_spells
        ),
        "n_ambiguous_pairs": int(
            ambiguous_pair_count
        ),
        "n_ambiguous_revelio_keys": int(
            ambiguous_key_count
        ),
        "n_ambiguous_foodservice_spells": int(
            ambiguous_spells
        ),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--backbone",
        required=True,
    )
    parser.add_argument(
        "--revelio-keys",
        required=True,
    )
    parser.add_argument(
        "--geography-keys",
        required=True,
    )
    parser.add_argument(
        "--outdir",
        required=True,
    )
    parser.add_argument(
        "--run-id",
        required=True,
    )

    args = parser.parse_args()

    outdir = Path(args.outdir)

    if outdir.exists():
        raise RuntimeError(
            f"Refusing to overwrite existing output: {outdir}"
        )

    candidate_root = (
        outdir / "candidate_pairs"
    )
    accepted_root = (
        outdir / "accepted_crosswalk"
    )
    ambiguous_root = (
        outdir / "ambiguous_crosswalk"
    )
    unmatched_root = (
        outdir / "unmatched_revelio_keys"
    )
    tables_root = (
        outdir / "tables"
    )

    outdir.mkdir(parents=True)

    for directory in [
        candidate_root,
        accepted_root,
        ambiguous_root,
        unmatched_root,
        tables_root,
    ]:
        directory.mkdir()

    assert_success(
        args.revelio_keys,
        "Revelio entity-location keys",
    )
    assert_success(
        args.geography_keys,
        "backbone geography keys",
    )

    spark = (
        SparkSession.builder
        .appName(
            "foodservice_updated_backbone_hierarchical_exact"
        )
        .config(
            "spark.sql.adaptive.enabled",
            "true",
        )
        .config(
            "spark.sql.parquet.enableVectorizedReader",
            "false",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    try:
        revelio_all = (
            spark.read.parquet(
                args.revelio_keys
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        backbone_raw = (
            spark.read.parquet(
                args.backbone
            )
            .select(
                F.col(
                    "restaurant_backbone_id"
                )
                .cast("string")
                .alias(
                    "backbone_establishment_id"
                ),
                F.col("name")
                .cast("string")
                .alias(
                    "backbone_raw_name"
                ),
            )
            .withColumn(
                "backbone_normalized_name",
                normalize_text(
                    F.col(
                        "backbone_raw_name"
                    )
                ),
            )
            .withColumn(
                "backbone_legal_suffix_normalized_name",
                strip_terminal_legal_suffix(
                    F.col(
                        "backbone_normalized_name"
                    )
                ),
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        geography_all = (
            spark.read.parquet(
                args.geography_keys
            )
            .where(
                F.col(
                    "geography_source"
                ).isin(
                    "census_place_name",
                    "city_nonstandardized",
                )
            )
            .select(
                F.col(
                    "backbone_establishment_id"
                ).cast("string"),
                "backbone_state_canonical",
                "backbone_geography_raw",
                "backbone_geography_normalized",
                "geography_source",
            )
            .where(
                valid(
                    "backbone_establishment_id"
                )
                & valid(
                    "backbone_state_canonical"
                )
                & valid(
                    "backbone_geography_normalized"
                )
            )
            .dropDuplicates(
                [
                    "backbone_establishment_id",
                    "backbone_state_canonical",
                    "backbone_geography_normalized",
                    "geography_source",
                ]
            )
        )

        backbone_geography_all = (
            geography_all
            .join(
                backbone_raw,
                "backbone_establishment_id",
                "inner",
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        free_revelio = revelio_all
        free_backbone_ids = (
            backbone_raw.select(
                "backbone_establishment_id"
            )
            .distinct()
        )

        accepted_frames = []
        ambiguous_frames = []
        stage_summaries = []

        stage_definitions = [
            {
                "stage_name": (
                    "tier1_census_place_exact_name"
                ),
                "geography_source": (
                    "census_place_name"
                ),
                "revelio_name_field": (
                    "revelio_normalized_company_name"
                ),
                "backbone_name_field": (
                    "backbone_normalized_name"
                ),
                "order": 1,
            },
            {
                "stage_name": (
                    "tier2_census_place_legal_suffix"
                ),
                "geography_source": (
                    "census_place_name"
                ),
                "revelio_name_field": (
                    "revelio_legal_suffix_normalized_company_name"
                ),
                "backbone_name_field": (
                    "backbone_legal_suffix_normalized_name"
                ),
                "order": 2,
            },
            {
                "stage_name": (
                    "tier3_city_nonstandardized_exact_name"
                ),
                "geography_source": (
                    "city_nonstandardized"
                ),
                "revelio_name_field": (
                    "revelio_normalized_company_name"
                ),
                "backbone_name_field": (
                    "backbone_normalized_name"
                ),
                "order": 3,
            },
            {
                "stage_name": (
                    "tier4_city_nonstandardized_legal_suffix"
                ),
                "geography_source": (
                    "city_nonstandardized"
                ),
                "revelio_name_field": (
                    "revelio_legal_suffix_normalized_company_name"
                ),
                "backbone_name_field": (
                    "backbone_legal_suffix_normalized_name"
                ),
                "order": 4,
            },
        ]

        for definition in stage_definitions:
            stage_name = definition[
                "stage_name"
            ]
            geography_source = definition[
                "geography_source"
            ]

            stage_backbone = (
                backbone_geography_all
                .where(
                    F.col(
                        "geography_source"
                    )
                    == geography_source
                )
                .join(
                    free_backbone_ids,
                    "backbone_establishment_id",
                    "inner",
                )
            )

            candidates = (
                build_candidates(
                    revelio=free_revelio,
                    backbone_geography=stage_backbone,
                    revelio_name_field=definition[
                        "revelio_name_field"
                    ],
                    backbone_name_field=definition[
                        "backbone_name_field"
                    ],
                    stage_name=stage_name,
                    geography_source=geography_source,
                    stage_order=definition[
                        "order"
                    ],
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            candidates.write.mode(
                "errorifexists"
            ).parquet(
                str(
                    candidate_root
                    / stage_name
                )
            )

            accepted = (
                candidates.where(
                    (
                        F.col(
                            "number_of_backbone_candidates"
                        )
                        == 1
                    )
                    & (
                        F.col(
                            "number_of_revelio_candidates"
                        )
                        == 1
                    )
                )
                .withColumn(
                    "match_status",
                    F.lit(
                        "accepted_unique"
                    ),
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            ambiguous = (
                candidates.where(
                    ~(
                        (
                            F.col(
                                "number_of_backbone_candidates"
                            )
                            == 1
                        )
                        & (
                            F.col(
                                "number_of_revelio_candidates"
                            )
                            == 1
                        )
                    )
                )
                .withColumn(
                    "match_status",
                    F.lit(
                        "ambiguous_blocked_from_later_tiers"
                    ),
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            accepted_frames.append(
                accepted
            )
            ambiguous_frames.append(
                ambiguous
            )

            stage_summaries.append(
                summarize_stage(
                    stage_name=stage_name,
                    geography_source=geography_source,
                    candidate_frame=candidates,
                    accepted_frame=accepted,
                    ambiguous_frame=ambiguous,
                )
            )

            involved_revelio_keys = (
                candidates.select(
                    "revelio_entity_location_key"
                )
                .distinct()
            )

            involved_backbone_ids = (
                candidates.select(
                    "backbone_establishment_id"
                )
                .distinct()
            )

            # Any candidate involvement blocks a key or restaurant
            # from lower-priority tiers. This preserves the hierarchy
            # and prevents expanded geography from overturning a
            # higher-confidence Census-place candidate.
            free_revelio = (
                free_revelio.join(
                    involved_revelio_keys,
                    "revelio_entity_location_key",
                    "left_anti",
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

            free_backbone_ids = (
                free_backbone_ids.join(
                    involved_backbone_ids,
                    "backbone_establishment_id",
                    "left_anti",
                )
                .persist(
                    StorageLevel.MEMORY_AND_DISK
                )
            )

        accepted_all = (
            union_frames(
                accepted_frames
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        ambiguous_all = (
            union_frames(
                ambiguous_frames
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        unmatched_all = (
            free_revelio
            .withColumn(
                "match_status",
                F.lit(
                    "unmatched_after_hierarchical_exact"
                ),
            )
            .persist(
                StorageLevel.MEMORY_AND_DISK
            )
        )

        accepted_path = (
            accepted_root
            / "accepted_crosswalk"
        )
        ambiguous_path = (
            ambiguous_root
            / "ambiguous_crosswalk"
        )
        unmatched_path = (
            unmatched_root
            / "unmatched_revelio_keys"
        )

        accepted_all.write.mode(
            "errorifexists"
        ).parquet(
            str(accepted_path)
        )

        ambiguous_all.write.mode(
            "errorifexists"
        ).parquet(
            str(ambiguous_path)
        )

        unmatched_all.write.mode(
            "errorifexists"
        ).parquet(
            str(unmatched_path)
        )

        total_keys = revelio_all.count()

        total_rcids = (
            revelio_all.select(
                "revelio_rcid"
            )
            .distinct()
            .count()
        )

        total_spells = (
            revelio_all.agg(
                F.sum(
                    "n_foodservice_spells"
                ).alias("n")
            )
            .first()["n"]
            or 0
        )

        accepted_rows = accepted_all.count()

        accepted_keys = (
            accepted_all.select(
                "revelio_entity_location_key"
            )
            .distinct()
            .count()
        )

        accepted_backbone = (
            accepted_all.select(
                "backbone_establishment_id"
            )
            .distinct()
            .count()
        )

        accepted_rcids = (
            accepted_all.select(
                "revelio_rcid"
            )
            .distinct()
            .count()
        )

        accepted_spells = (
            accepted_all.agg(
                F.sum(
                    "n_foodservice_spells"
                ).alias("n")
            )
            .first()["n"]
            or 0
        )

        ambiguous_keys = (
            ambiguous_all.select(
                "revelio_entity_location_key"
            )
            .distinct()
            .count()
        )

        ambiguous_spells = (
            ambiguous_all.select(
                "revelio_entity_location_key",
                "n_foodservice_spells",
            )
            .dropDuplicates(
                [
                    "revelio_entity_location_key"
                ]
            )
            .agg(
                F.sum(
                    "n_foodservice_spells"
                ).alias("n")
            )
            .first()["n"]
            or 0
        )

        unmatched_keys = (
            unmatched_all.count()
        )

        unmatched_spells = (
            unmatched_all.agg(
                F.sum(
                    "n_foodservice_spells"
                ).alias("n")
            )
            .first()["n"]
            or 0
        )

        accepted_key_frame = (
            accepted_all.select(
                "revelio_entity_location_key"
            )
            .distinct()
        )

        ambiguous_key_frame = (
            ambiguous_all.select(
                "revelio_entity_location_key"
            )
            .distinct()
        )

        unmatched_key_frame = (
            unmatched_all.select(
                "revelio_entity_location_key"
            )
            .distinct()
        )

        accepted_ambiguous_overlap = (
            accepted_key_frame.join(
                ambiguous_key_frame,
                "revelio_entity_location_key",
                "inner",
            )
            .count()
        )

        accepted_unmatched_overlap = (
            accepted_key_frame.join(
                unmatched_key_frame,
                "revelio_entity_location_key",
                "inner",
            )
            .count()
        )

        ambiguous_unmatched_overlap = (
            ambiguous_key_frame.join(
                unmatched_key_frame,
                "revelio_entity_location_key",
                "inner",
            )
            .count()
        )

        partition_total = (
            accepted_keys
            + ambiguous_keys
            + unmatched_keys
        )

        accepted_by_tier = [
            row.asDict()
            for row in (
                accepted_all.groupBy(
                    "match_tier",
                    "match_tier_order",
                    "matched_geography_source",
                )
                .agg(
                    F.countDistinct(
                        "revelio_entity_location_key"
                    ).alias(
                        "n_revelio_keys"
                    ),
                    F.countDistinct(
                        "revelio_rcid"
                    ).alias(
                        "n_revelio_rcids"
                    ),
                    F.countDistinct(
                        "backbone_establishment_id"
                    ).alias(
                        "n_backbone_establishments"
                    ),
                    F.sum(
                        "n_foodservice_spells"
                    ).alias(
                        "n_foodservice_spells"
                    ),
                )
                .orderBy(
                    "match_tier_order"
                )
                .collect()
            )
        ]

        report = {
            "run_id": args.run_id,
            "created_utc": dt.datetime.now(
                dt.timezone.utc
            ).isoformat(),
            "design": {
                "description": (
                    "Sequential one-to-one exact matching. "
                    "Census-place tiers are resolved first. "
                    "city_nonstandardized tiers operate only on "
                    "Revelio keys and backbone establishments with "
                    "no candidate involvement in earlier tiers."
                ),
                "stage_order": [
                    definition["stage_name"]
                    for definition in stage_definitions
                ],
                "cbsa_used": False,
                "fuzzy_matching_used": False,
                "terminal_city_suffix_cleaning_used": False,
            },
            "counts": {
                "n_revelio_keys_total": int(
                    total_keys
                ),
                "n_revelio_rcids_total": int(
                    total_rcids
                ),
                "n_revelio_spells_total": int(
                    total_spells
                ),
                "n_accepted_rows": int(
                    accepted_rows
                ),
                "n_accepted_revelio_keys": int(
                    accepted_keys
                ),
                "n_accepted_backbone_establishments": int(
                    accepted_backbone
                ),
                "n_accepted_revelio_rcids": int(
                    accepted_rcids
                ),
                "n_accepted_foodservice_spells": int(
                    accepted_spells
                ),
                "n_ambiguous_revelio_keys": int(
                    ambiguous_keys
                ),
                "n_ambiguous_foodservice_spells": int(
                    ambiguous_spells
                ),
                "n_unmatched_revelio_keys": int(
                    unmatched_keys
                ),
                "n_unmatched_foodservice_spells": int(
                    unmatched_spells
                ),
            },
            "match_rates": {
                "accepted_revelio_key_rate": (
                    accepted_keys / total_keys
                    if total_keys
                    else None
                ),
                "accepted_revelio_rcid_rate": (
                    accepted_rcids / total_rcids
                    if total_rcids
                    else None
                ),
                "accepted_foodservice_spell_rate": (
                    accepted_spells / total_spells
                    if total_spells
                    else None
                ),
            },
            "structural_validation": {
                "accepted_key_uniqueness_valid": (
                    accepted_rows
                    == accepted_keys
                ),
                "accepted_backbone_uniqueness_valid": (
                    accepted_rows
                    == accepted_backbone
                ),
                "accepted_ambiguous_key_overlap": int(
                    accepted_ambiguous_overlap
                ),
                "accepted_unmatched_key_overlap": int(
                    accepted_unmatched_overlap
                ),
                "ambiguous_unmatched_key_overlap": int(
                    ambiguous_unmatched_overlap
                ),
                "partition_distinct_key_total": int(
                    partition_total
                ),
                "partition_total_valid": (
                    partition_total
                    == total_keys
                ),
            },
            "stage_summaries": stage_summaries,
            "accepted_by_tier": accepted_by_tier,
            "outputs": {
                "candidate_pairs_root": str(
                    candidate_root
                ),
                "accepted_crosswalk": str(
                    accepted_path
                ),
                "ambiguous_crosswalk": str(
                    ambiguous_path
                ),
                "unmatched_revelio_keys": str(
                    unmatched_path
                ),
            },
            "next_steps_not_yet_run": [
                "compare hierarchical crosswalk with prior baseline",
                "terminal city/place suffix diagnostic tier",
                "restricted fuzzy-name candidate diagnostics",
                "full backbone payload and chain assignment merge",
                "spell-level merge",
            ],
        }

        write_json(
            tables_root
            / "hierarchical_exact_summary.json",
            report,
        )

        (
            outdir
            / "_HIERARCHICAL_EXACT_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
