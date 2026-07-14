#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F


INVALID = [
    "", "empty", "null", "none", "nan", "na", "n a",
    "unknown", "missing", "not available", "unavailable"
]

STATE_MAP = {
    "ALABAMA": "AL", "ALASKA": "AK", "ARIZONA": "AZ", "ARKANSAS": "AR",
    "CALIFORNIA": "CA", "COLORADO": "CO", "CONNECTICUT": "CT",
    "DELAWARE": "DE", "DISTRICT OF COLUMBIA": "DC", "WASHINGTON DC": "DC",
    "FLORIDA": "FL", "GEORGIA": "GA", "HAWAII": "HI", "IDAHO": "ID",
    "ILLINOIS": "IL", "INDIANA": "IN", "IOWA": "IA", "KANSAS": "KS",
    "KENTUCKY": "KY", "LOUISIANA": "LA", "MAINE": "ME", "MARYLAND": "MD",
    "MASSACHUSETTS": "MA", "MICHIGAN": "MI", "MINNESOTA": "MN",
    "MISSISSIPPI": "MS", "MISSOURI": "MO", "MONTANA": "MT",
    "NEBRASKA": "NE", "NEVADA": "NV", "NEW HAMPSHIRE": "NH",
    "NEW JERSEY": "NJ", "NEW MEXICO": "NM", "NEW YORK": "NY",
    "NORTH CAROLINA": "NC", "NORTH DAKOTA": "ND", "OHIO": "OH",
    "OKLAHOMA": "OK", "OREGON": "OR", "PENNSYLVANIA": "PA",
    "RHODE ISLAND": "RI", "SOUTH CAROLINA": "SC", "SOUTH DAKOTA": "SD",
    "TENNESSEE": "TN", "TEXAS": "TX", "UTAH": "UT", "VERMONT": "VT",
    "VIRGINIA": "VA", "WASHINGTON": "WA", "WEST VIRGINIA": "WV",
    "WISCONSIN": "WI", "WYOMING": "WY",
    "PUERTO RICO": "PR", "GUAM": "GU", "AMERICAN SAMOA": "AS",
    "US VIRGIN ISLANDS": "VI", "U S VIRGIN ISLANDS": "VI",
    "NORTHERN MARIANA ISLANDS": "MP",
}


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)


def assert_success(path, label):
    marker = os.path.join(path, "_SUCCESS")
    if not os.path.isfile(marker):
        raise RuntimeError(f"Missing _SUCCESS for {label}: {marker}")


def valid(c):
    x = F.lower(F.trim(F.col(c).cast("string")))
    return (
        F.col(c).isNotNull()
        & (F.length(x) > 0)
        & (~x.isin(*INVALID))
    )


def norm_text(col):
    x = F.lower(F.trim(col.cast("string")))
    x = F.regexp_replace(x, r"[^\p{L}\p{N}]+", " ")
    x = F.trim(F.regexp_replace(x, r"\s+", " "))
    return (
        F.when(F.length(x) == 0, F.lit(None).cast("string"))
        .when(x.isin(*INVALID), F.lit(None).cast("string"))
        .otherwise(x)
    )


def canonical_state(col):
    cleaned = F.upper(F.trim(col.cast("string")))
    cleaned = F.regexp_replace(cleaned, r"[^A-Z0-9]+", " ")
    cleaned = F.trim(F.regexp_replace(cleaned, r"\s+", " "))

    mapping_items = []
    for key, value in STATE_MAP.items():
        mapping_items.extend([F.lit(key), F.lit(value)])
    mapping = F.create_map(*mapping_items)

    return (
        F.when(cleaned.isNull() | cleaned.isin(*[x.upper() for x in INVALID]), None)
        .when(mapping[cleaned].isNotNull(), mapping[cleaned])
        .when(
            (F.length(cleaned) == 2) & cleaned.rlike(r"^[A-Z]{2}$"),
            cleaned,
        )
        .otherwise(cleaned)
    )


def field_metric_aggregations(fields):
    expressions = []
    for c in fields:
        expressions.append(
            F.sum(F.when(valid(c), 1).otherwise(0)).alias(f"{c}__valid")
        )
        expressions.append(
            F.approx_count_distinct(
                F.when(valid(c), F.col(c).cast("string"))
            ).alias(f"{c}__distinct")
        )
    return expressions


def top_values(df, field, limit=30):
    if field not in df.columns:
        return []

    return [
        row.asDict()
        for row in (
            df.where(valid(field))
            .groupBy(F.col(field).cast("string").alias("value"))
            .agg(F.count("*").alias("n_rows"))
            .orderBy(F.desc("n_rows"), F.asc("value"))
            .limit(limit)
            .collect()
        )
    ]


def coverage_dict(df, label):
    row = (
        df.agg(
            F.countDistinct("revelio_entity_location_key").alias("n_keys"),
            F.countDistinct("revelio_rcid").alias("n_rcids"),
            F.sum("n_foodservice_spells").alias("n_spells"),
            F.sum("n_people").alias("sum_key_level_people"),
        )
        .first()
        .asDict()
    )
    row["coverage_definition"] = label
    return row


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backbone", required=True)
    ap.add_argument("--revelio-keys", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--run-id", required=True)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    if outdir.exists():
        raise RuntimeError(f"Refusing to overwrite existing output: {outdir}")
    outdir.mkdir(parents=True)

    geography_out = outdir / "backbone_geography_keys"
    chain_out = outdir / "backbone_chain_assignment"
    suffix_out = outdir / "terminal_geography_suffix_candidates"
    coverage_out = outdir / "revelio_geography_coverage"
    sample_out = outdir / "samples"
    sample_out.mkdir()

    assert_success(args.revelio_keys, "clean Revelio entity-location keys")

    spark = (
        SparkSession.builder
        .appName("foodservice_updated_backbone_geo_origin_chain_audit")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        backbone = (
            spark.read.parquet(args.backbone)
            .withColumn(
                "backbone_establishment_id",
                F.col("restaurant_backbone_id").cast("string"),
            )
            .withColumn(
                "backbone_state_canonical",
                canonical_state(F.col("state")),
            )
            .withColumn(
                "backbone_name_normalized",
                norm_text(F.col("name")),
            )
            .withColumn(
                "backbone_brand_normalized",
                norm_text(F.col("brand")),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        revelio = (
            spark.read.parquet(args.revelio_keys)
            .where(
                valid("revelio_state_canonical")
                & valid("revelio_normalized_city")
            )
            .select(
                "revelio_entity_location_key",
                "revelio_rcid",
                "revelio_state_canonical",
                "revelio_normalized_city",
                "n_foodservice_spells",
                "n_people",
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        n_backbone = backbone.count()
        n_revelio_keys = revelio.count()

        # --------------------------------------------------------------
        # 1. Long-form backbone geography keys
        # --------------------------------------------------------------
        city_nonstandardized = (
            backbone.where(
                valid("city_nonstandardized")
                & valid("backbone_state_canonical")
            )
            .select(
                "backbone_establishment_id",
                "backbone_state_canonical",
                F.lit("city_nonstandardized").alias("geography_source"),
                norm_text(F.col("city_nonstandardized")).alias(
                    "backbone_geography_normalized"
                ),
                F.col("city_nonstandardized").cast("string").alias(
                    "backbone_geography_raw"
                ),
                F.lit(1).alias("geography_confidence_tier"),
            )
        )

        census_place = (
            backbone.where(
                valid("census_place_name")
                & valid("backbone_state_canonical")
            )
            .select(
                "backbone_establishment_id",
                "backbone_state_canonical",
                F.lit("census_place_name").alias("geography_source"),
                norm_text(F.col("census_place_name")).alias(
                    "backbone_geography_normalized"
                ),
                F.col("census_place_name").cast("string").alias(
                    "backbone_geography_raw"
                ),
                F.lit(1).alias("geography_confidence_tier"),
            )
        )

        cbsa_base = (
            backbone.where(
                valid("cbsa_title")
                & valid("backbone_state_canonical")
            )
            .select(
                "backbone_establishment_id",
                "backbone_state_canonical",
                F.col("cbsa_title").cast("string").alias("cbsa_title"),
                F.regexp_extract(
                    F.col("cbsa_title").cast("string"),
                    r"^([^,]+)",
                    1,
                ).alias("cbsa_before_comma"),
            )
        )

        cbsa_full = (
            cbsa_base
            .select(
                "backbone_establishment_id",
                "backbone_state_canonical",
                F.lit("cbsa_title_full").alias("geography_source"),
                norm_text(F.col("cbsa_before_comma")).alias(
                    "backbone_geography_normalized"
                ),
                F.col("cbsa_before_comma").alias("backbone_geography_raw"),
                F.lit(3).alias("geography_confidence_tier"),
            )
        )

        cbsa_components = (
            cbsa_base
            .withColumn(
                "cbsa_component",
                F.explode(
                    F.split(
                        F.col("cbsa_before_comma"),
                        r"\s*[-–—]\s*",
                    )
                ),
            )
            .where(F.length(F.trim("cbsa_component")) > 0)
            .select(
                "backbone_establishment_id",
                "backbone_state_canonical",
                F.lit("cbsa_title_component").alias("geography_source"),
                norm_text(F.col("cbsa_component")).alias(
                    "backbone_geography_normalized"
                ),
                F.trim(F.col("cbsa_component")).alias(
                    "backbone_geography_raw"
                ),
                F.lit(3).alias("geography_confidence_tier"),
            )
        )

        geography = (
            city_nonstandardized
            .unionByName(census_place)
            .unionByName(cbsa_full)
            .unionByName(cbsa_components)
            .where(valid("backbone_geography_normalized"))
            .dropDuplicates(
                [
                    "backbone_establishment_id",
                    "backbone_state_canonical",
                    "geography_source",
                    "backbone_geography_normalized",
                ]
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        geography.write.mode("errorifexists").parquet(str(geography_out))

        geography_source_counts = [
            row.asDict()
            for row in (
                geography.groupBy(
                    "geography_source",
                    "geography_confidence_tier",
                )
                .agg(
                    F.countDistinct("backbone_establishment_id").alias(
                        "n_backbone_establishments"
                    ),
                    F.countDistinct(
                        F.struct(
                            "backbone_state_canonical",
                            "backbone_geography_normalized",
                        )
                    ).alias("n_unique_state_geography_blocks"),
                )
                .orderBy("geography_confidence_tier", "geography_source")
                .collect()
            )
        ]

        # --------------------------------------------------------------
        # 2. Exact Revelio geography overlap by each source and union
        # --------------------------------------------------------------
        distinct_blocks = (
            geography.select(
                "backbone_state_canonical",
                "backbone_geography_normalized",
                "geography_source",
                "geography_confidence_tier",
            )
            .dropDuplicates()
        )

        coverage_pairs = (
            revelio.alias("r")
            .join(
                distinct_blocks.alias("b"),
                (
                    F.col("r.revelio_state_canonical")
                    == F.col("b.backbone_state_canonical")
                )
                & (
                    F.col("r.revelio_normalized_city")
                    == F.col("b.backbone_geography_normalized")
                ),
                "inner",
            )
            .select(
                "r.*",
                "b.geography_source",
                "b.geography_confidence_tier",
            )
            .dropDuplicates(
                [
                    "revelio_entity_location_key",
                    "geography_source",
                ]
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        coverage_pairs.write.mode("errorifexists").parquet(str(coverage_out))

        coverage_by_source = [
            row.asDict()
            for row in (
                coverage_pairs.groupBy(
                    "geography_source",
                    "geography_confidence_tier",
                )
                .agg(
                    F.countDistinct(
                        "revelio_entity_location_key"
                    ).alias("n_revelio_keys"),
                    F.countDistinct("revelio_rcid").alias("n_revelio_rcids"),
                    F.sum("n_foodservice_spells").alias("n_revelio_spells"),
                    F.sum("n_people").alias("sum_key_level_people"),
                )
                .orderBy(
                    "geography_confidence_tier",
                    "geography_source",
                )
                .collect()
            )
        ]

        covered_city = (
            coverage_pairs.where(
                F.col("geography_source") == "city_nonstandardized"
            )
            .select("revelio_entity_location_key")
            .distinct()
        )

        covered_census = (
            coverage_pairs.where(
                F.col("geography_source") == "census_place_name"
            )
            .select("revelio_entity_location_key")
            .distinct()
        )

        covered_cbsa = (
            coverage_pairs.where(
                F.col("geography_source").isin(
                    "cbsa_title_full",
                    "cbsa_title_component",
                )
            )
            .select("revelio_entity_location_key")
            .distinct()
        )

        covered_city_or_census = (
            covered_city.unionByName(covered_census).distinct()
        )

        covered_all = (
            covered_city_or_census
            .unionByName(covered_cbsa)
            .distinct()
        )

        census_only_keys = revelio.join(
            covered_census,
            "revelio_entity_location_key",
            "inner",
        )

        city_beyond_census_keys = (
            revelio.join(
                covered_city,
                "revelio_entity_location_key",
                "inner",
            )
            .join(
                covered_census,
                "revelio_entity_location_key",
                "left_anti",
            )
        )

        cbsa_beyond_city_census_keys = (
            revelio.join(
                covered_cbsa,
                "revelio_entity_location_key",
                "inner",
            )
            .join(
                covered_city_or_census,
                "revelio_entity_location_key",
                "left_anti",
            )
        )

        all_covered_keys = revelio.join(
            covered_all,
            "revelio_entity_location_key",
            "inner",
        )

        uncovered_keys = (
            revelio.join(
                covered_all,
                "revelio_entity_location_key",
                "left_anti",
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        incremental_coverage = {
            "census_place_baseline": coverage_dict(
                census_only_keys,
                "Exact canonical state plus exact Census-place name",
            ),
            "city_nonstandardized_increment_beyond_census": coverage_dict(
                city_beyond_census_keys,
                "City-nonstandardized matches not already covered by Census place",
            ),
            "cbsa_increment_beyond_city_and_census": coverage_dict(
                cbsa_beyond_city_census_keys,
                "CBSA full/component matches not already covered by city or Census place",
            ),
            "union_city_census_cbsa": coverage_dict(
                all_covered_keys,
                "Union of city_nonstandardized, Census place, and parsed CBSA keys",
            ),
        }

        top_uncovered = [
            row.asDict()
            for row in (
                uncovered_keys.groupBy(
                    "revelio_state_canonical",
                    "revelio_normalized_city",
                )
                .agg(
                    F.countDistinct(
                        "revelio_entity_location_key"
                    ).alias("n_revelio_keys"),
                    F.sum("n_foodservice_spells").alias("n_spells"),
                    F.sum("n_people").alias("sum_key_level_people"),
                )
                .orderBy(F.desc("n_revelio_keys"))
                .limit(100)
                .collect()
            )
        ]

        # --------------------------------------------------------------
        # 3. Geography-field agreement within backbone
        # --------------------------------------------------------------
        geography_agreement = (
            backbone
            .withColumn(
                "city_nonstandardized_norm",
                norm_text(F.col("city_nonstandardized")),
            )
            .withColumn(
                "census_place_norm",
                norm_text(F.col("census_place_name")),
            )
            .withColumn(
                "cbsa_full_norm",
                norm_text(
                    F.regexp_extract(
                        F.col("cbsa_title").cast("string"),
                        r"^([^,]+)",
                        1,
                    )
                ),
            )
            .agg(
                F.sum(
                    F.when(
                        valid("city_nonstandardized")
                        & valid("census_place_name"),
                        1,
                    ).otherwise(0)
                ).alias("n_rows_with_city_and_census"),
                F.sum(
                    F.when(
                        valid("city_nonstandardized")
                        & valid("census_place_name")
                        & (
                            F.col("city_nonstandardized_norm")
                            == F.col("census_place_norm")
                        ),
                        1,
                    ).otherwise(0)
                ).alias("n_rows_city_equals_census"),
                F.sum(
                    F.when(
                        valid("city_nonstandardized")
                        | valid("census_place_name")
                        | valid("cbsa_title"),
                        1,
                    ).otherwise(0)
                ).alias("n_rows_with_any_geography"),
            )
            .first()
            .asDict()
        )

        # --------------------------------------------------------------
        # 4. Origin schema and coverage audit
        # --------------------------------------------------------------
        origin_fields = sorted(
            [
                c for c in backbone.columns
                if c.startswith("origin_")
                or c.startswith("origin_iso")
                or c.startswith("origin_harmonized")
            ]
        )

        origin_metric_row = (
            backbone.agg(
                *field_metric_aggregations(origin_fields)
            ).first().asDict()
        )

        origin_metrics = []
        for c in origin_fields:
            valid_count = int(origin_metric_row.get(f"{c}__valid") or 0)
            distinct_count = int(
                origin_metric_row.get(f"{c}__distinct") or 0
            )
            origin_metrics.append(
                {
                    "field": c,
                    "valid_count": valid_count,
                    "missing_count": int(n_backbone - valid_count),
                    "missing_rate": (
                        (n_backbone - valid_count) / n_backbone
                        if n_backbone
                        else None
                    ),
                    "approx_distinct_nonmissing": distinct_count,
                }
            )

        origin_pair_diagnostics = []
        for i in range(1, 10):
            label = f"origin_{i}"
            iso = f"origin_iso{i}"

            if label not in backbone.columns or iso not in backbone.columns:
                continue

            row = (
                backbone.agg(
                    F.sum(
                        F.when(valid(label) & valid(iso), 1).otherwise(0)
                    ).alias("both_nonmissing"),
                    F.sum(
                        F.when(valid(label) & ~valid(iso), 1).otherwise(0)
                    ).alias("label_only"),
                    F.sum(
                        F.when(~valid(label) & valid(iso), 1).otherwise(0)
                    ).alias("iso_only"),
                    F.approx_count_distinct(
                        F.when(
                            valid(label) & valid(iso),
                            F.struct(label, iso),
                        )
                    ).alias("approx_distinct_pairs"),
                )
                .first()
                .asDict()
            )
            row["slot"] = i
            row["label_field"] = label
            row["iso_field"] = iso
            origin_pair_diagnostics.append(row)

        origin_top_fields = [
            c for c in [
                "origin_country_count",
                "origin_1", "origin_2", "origin_3",
                "origin_iso1", "origin_iso2", "origin_iso3",
                "origin_harmonized_10",
                "origin_harmonized_11",
                "origin_harmonized_12",
                "origin_harmonized_13",
                "origin_harmonized_14",
            ]
            if c in backbone.columns
        ]

        origin_top_values = {
            c: top_values(backbone, c, 30)
            for c in origin_top_fields
        }

        origin_sample_cols = [
            c for c in (
                [
                    "restaurant_backbone_id",
                    "name",
                    "brand",
                    "state",
                    "city_nonstandardized",
                    "census_place_name",
                    "cbsa_title",
                    "osm_cuisines",
                ]
                + origin_fields
            )
            if c in backbone.columns
        ]

        (
            backbone.select(*origin_sample_cols)
            .orderBy(F.rand(20260710))
            .limit(1000)
            .coalesce(1)
            .write.mode("errorifexists")
            .option("header", True)
            .csv(str(sample_out / "origin_schema_sample_csv"))
        )

        # --------------------------------------------------------------
        # 5. Chain definitions
        # --------------------------------------------------------------
        brand_groups = (
            backbone.where(valid("backbone_brand_normalized"))
            .groupBy("backbone_brand_normalized")
            .agg(
                F.countDistinct(
                    "backbone_establishment_id"
                ).alias("n_restaurants_brand")
            )
        )

        name_groups = (
            backbone.where(valid("backbone_name_normalized"))
            .groupBy("backbone_name_normalized")
            .agg(
                F.countDistinct(
                    "backbone_establishment_id"
                ).alias("n_restaurants_name")
            )
        )

        chain_assignment = (
            backbone
            .join(
                brand_groups,
                "backbone_brand_normalized",
                "left",
            )
            .join(
                name_groups,
                "backbone_name_normalized",
                "left",
            )
            .withColumn(
                "chain_group_type_preferred",
                F.when(
                    valid("backbone_brand_normalized"),
                    F.lit("brand"),
                ).otherwise(F.lit("normalized_name_fallback")),
            )
            .withColumn(
                "chain_group_key_preferred",
                F.coalesce(
                    F.col("backbone_brand_normalized"),
                    F.col("backbone_name_normalized"),
                ),
            )
            .withColumn(
                "n_restaurants_preferred_group",
                F.when(
                    valid("backbone_brand_normalized"),
                    F.col("n_restaurants_brand"),
                ).otherwise(F.col("n_restaurants_name")),
            )
            .withColumn(
                "chain_ge100_preferred",
                F.col("n_restaurants_preferred_group") >= 100,
            )
            .withColumn(
                "chain_ge100_brand_only",
                F.col("n_restaurants_brand") >= 100,
            )
            .withColumn(
                "chain_ge100_name_only",
                F.col("n_restaurants_name") >= 100,
            )
            .select(
                "backbone_establishment_id",
                F.col("name").alias("backbone_name"),
                F.col("brand").alias("backbone_brand"),
                "backbone_name_normalized",
                "backbone_brand_normalized",
                "n_restaurants_brand",
                "n_restaurants_name",
                "chain_group_type_preferred",
                "chain_group_key_preferred",
                "n_restaurants_preferred_group",
                "chain_ge100_preferred",
                "chain_ge100_brand_only",
                "chain_ge100_name_only",
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        chain_assignment.write.mode("errorifexists").parquet(
            str(chain_out)
        )

        chain_summary = (
            chain_assignment.agg(
                F.count("*").alias("n_backbone_establishments"),
                F.sum(
                    F.when(valid("backbone_brand_normalized"), 1).otherwise(0)
                ).alias("n_establishments_with_brand"),
                F.sum(
                    F.when(
                        F.col("chain_ge100_preferred"),
                        1,
                    ).otherwise(0)
                ).alias("n_chain_establishments_preferred"),
                F.sum(
                    F.when(
                        F.col("chain_ge100_brand_only"),
                        1,
                    ).otherwise(0)
                ).alias("n_chain_establishments_brand_only"),
                F.sum(
                    F.when(
                        F.col("chain_ge100_name_only"),
                        1,
                    ).otherwise(0)
                ).alias("n_chain_establishments_name_only"),
                F.sum(
                    F.when(
                        F.col("chain_ge100_brand_only")
                        != F.col("chain_ge100_name_only"),
                        1,
                    ).otherwise(0)
                ).alias("n_brand_name_chain_disagreements"),
            )
            .first()
            .asDict()
        )

        top_brands = [
            row.asDict()
            for row in (
                brand_groups
                .orderBy(
                    F.desc("n_restaurants_brand"),
                    F.asc("backbone_brand_normalized"),
                )
                .limit(100)
                .collect()
            )
        ]

        # --------------------------------------------------------------
        # 6. Restaurant-name terminal geography suffix diagnostics
        # --------------------------------------------------------------
        suffix_candidates = (
            backbone.select(
                "backbone_establishment_id",
                F.col("name").alias("backbone_name"),
                "backbone_name_normalized",
                "backbone_state_canonical",
            )
            .join(
                geography.select(
                    "backbone_establishment_id",
                    "geography_source",
                    "backbone_geography_raw",
                    "backbone_geography_normalized",
                ),
                "backbone_establishment_id",
                "inner",
            )
            .where(
                F.expr(
                    """
                    backbone_name_normalized IS NOT NULL
                    AND backbone_geography_normalized IS NOT NULL
                    AND length(backbone_geography_normalized) >= 3
                    AND backbone_name_normalized <> backbone_geography_normalized
                    AND right(
                        backbone_name_normalized,
                        length(backbone_geography_normalized) + 1
                    ) = concat(' ', backbone_geography_normalized)
                    """
                )
            )
            .withColumn(
                "name_after_terminal_geography_removed",
                F.expr(
                    """
                    trim(
                        substring(
                            backbone_name_normalized,
                            1,
                            length(backbone_name_normalized)
                            - length(backbone_geography_normalized)
                            - 1
                        )
                    )
                    """
                ),
            )
            .where(
                F.length("name_after_terminal_geography_removed") >= 2
            )
            .dropDuplicates(
                [
                    "backbone_establishment_id",
                    "geography_source",
                    "backbone_geography_normalized",
                    "name_after_terminal_geography_removed",
                ]
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        suffix_candidates.write.mode("errorifexists").parquet(
            str(suffix_out)
        )

        suffix_summary = [
            row.asDict()
            for row in (
                suffix_candidates.groupBy("geography_source")
                .agg(
                    F.countDistinct(
                        "backbone_establishment_id"
                    ).alias("n_establishments_with_terminal_geo_suffix"),
                    F.count("*").alias("n_suffix_candidates"),
                    F.countDistinct(
                        "name_after_terminal_geography_removed"
                    ).alias("n_distinct_stripped_names"),
                )
                .orderBy("geography_source")
                .collect()
            )
        ]

        (
            suffix_candidates
            .orderBy(F.rand(20260711))
            .limit(1000)
            .coalesce(1)
            .write.mode("errorifexists")
            .option("header", True)
            .csv(str(sample_out / "terminal_geography_suffix_sample_csv"))
        )

        # --------------------------------------------------------------
        # Final report
        # --------------------------------------------------------------
        report = {
            "run_id": args.run_id,
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "backbone_path": args.backbone,
            "revelio_keys_path": args.revelio_keys,
            "counts": {
                "n_backbone_establishments": int(n_backbone),
                "n_revelio_entity_location_keys_valid_geo": int(
                    n_revelio_keys
                ),
            },
            "geography_source_counts": geography_source_counts,
            "coverage_by_geography_source": coverage_by_source,
            "incremental_revelio_geography_coverage": incremental_coverage,
            "backbone_geography_agreement": geography_agreement,
            "top_revelio_blocks_uncovered_by_any_geography": top_uncovered,
            "origin_fields": origin_fields,
            "origin_field_metrics": origin_metrics,
            "origin_label_iso_pair_diagnostics": origin_pair_diagnostics,
            "origin_top_values": origin_top_values,
            "chain_summary": chain_summary,
            "top_normalized_brands": top_brands,
            "terminal_geography_suffix_summary": suffix_summary,
            "important_interpretation": {
                "city_nonstandardized": (
                    "Address-like city label supplied in the backbone. "
                    "Treat as a high-confidence exact-geography key."
                ),
                "census_place_name": (
                    "Census place proxy. High-confidence when exact, but "
                    "does not partition the entire United States."
                ),
                "cbsa_title_component": (
                    "Broader and lower-confidence geography. Hyphen splitting "
                    "may also split genuinely hyphenated municipality names, "
                    "so CBSA-based matches should remain a separate tier."
                ),
                "chain_definition": (
                    "Preferred provisional chain flag uses normalized brand "
                    "when available and normalized restaurant name otherwise; "
                    "chain means at least 100 distinct backbone establishments."
                ),
            },
            "outputs": {
                "backbone_geography_keys": str(geography_out),
                "revelio_geography_coverage": str(coverage_out),
                "backbone_chain_assignment": str(chain_out),
                "terminal_geography_suffix_candidates": str(suffix_out),
                "samples": str(sample_out),
            },
        }

        write_json(
            outdir / "updated_backbone_geo_origin_chain_audit.json",
            report,
        )

        (outdir / "_UPDATED_BACKBONE_FOCUSED_AUDIT_COMPLETE").write_text(
            "complete\n",
            encoding="utf-8",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
