#!/usr/bin/env python3

import argparse
import json
import re
import unicodedata
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T


LEGAL_SUFFIX_PATTERN = re.compile(
    r"(?:\s+|^)"
    r"(?:"
    r"llc|l l c|inc|incorporated|ltd|limited|"
    r"corp|corporation|pllc|llp|l l p|lp|l p|"
    r"pc|p c|co|company"
    r")$",
    flags=re.IGNORECASE,
)

STATE_MAP = {
    "alabama": "AL",
    "alaska": "AK",
    "arizona": "AZ",
    "arkansas": "AR",
    "california": "CA",
    "colorado": "CO",
    "connecticut": "CT",
    "delaware": "DE",
    "district of columbia": "DC",
    "florida": "FL",
    "georgia": "GA",
    "hawaii": "HI",
    "idaho": "ID",
    "illinois": "IL",
    "indiana": "IN",
    "iowa": "IA",
    "kansas": "KS",
    "kentucky": "KY",
    "louisiana": "LA",
    "maine": "ME",
    "maryland": "MD",
    "massachusetts": "MA",
    "michigan": "MI",
    "minnesota": "MN",
    "mississippi": "MS",
    "missouri": "MO",
    "montana": "MT",
    "nebraska": "NE",
    "nevada": "NV",
    "new hampshire": "NH",
    "new jersey": "NJ",
    "new mexico": "NM",
    "new york": "NY",
    "north carolina": "NC",
    "north dakota": "ND",
    "ohio": "OH",
    "oklahoma": "OK",
    "oregon": "OR",
    "pennsylvania": "PA",
    "rhode island": "RI",
    "south carolina": "SC",
    "south dakota": "SD",
    "tennessee": "TN",
    "texas": "TX",
    "utah": "UT",
    "vermont": "VT",
    "virginia": "VA",
    "washington": "WA",
    "west virginia": "WV",
    "wisconsin": "WI",
    "wyoming": "WY",
    "puerto rico": "PR",
    "guam": "GU",
    "american samoa": "AS",
    "northern mariana islands": "MP",
    "us virgin islands": "VI",
    "united states virgin islands": "VI",
}


def normalize_text(value):
    if value is None:
        return None

    text = str(value).strip()

    if not text:
        return None

    text = unicodedata.normalize("NFKD", text)
    text = "".join(
        character
        for character in text
        if not unicodedata.combining(character)
    )

    text = text.lower()
    text = re.sub(r"[^a-z0-9]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text or None


def legal_normalize(value):
    text = normalize_text(value)

    if text is None:
        return None

    while True:
        cleaned = LEGAL_SUFFIX_PATTERN.sub("", text).strip()

        if cleaned == text:
            break

        text = cleaned

    return text or None


def normalize_state(value):
    text = normalize_text(value)

    if text is None:
        return None

    compact = text.replace(" ", "")

    if len(compact) == 2:
        return compact.upper()

    return STATE_MAP.get(text)


normalize_text_udf = F.udf(
    normalize_text,
    T.StringType(),
)

legal_normalize_udf = F.udf(
    legal_normalize,
    T.StringType(),
)

normalize_state_udf = F.udf(
    normalize_state,
    T.StringType(),
)


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--accepted", required=True)
    parser.add_argument("--ambiguous", required=True)
    parser.add_argument("--unmatched", required=True)
    parser.add_argument("--backbone", required=True)
    parser.add_argument("--outdir", required=True)
    parser.add_argument(
        "--max-distance-ratio",
        type=float,
        default=0.10,
    )
    parser.add_argument(
        "--minimum-margin",
        type=float,
        default=0.03,
    )
    parser.add_argument(
        "--minimum-name-length",
        type=int,
        default=6,
    )

    args = parser.parse_args()

    outdir = Path(args.outdir)

    if outdir.exists():
        raise RuntimeError(
            f"Refusing to overwrite existing output: {outdir}"
        )

    outdir.mkdir(parents=True)

    spark = (
        SparkSession.builder
        .appName("food722_restricted_fuzzy_match")
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
        accepted = spark.read.parquet(args.accepted)
        ambiguous = spark.read.parquet(args.ambiguous)
        unmatched = spark.read.parquet(args.unmatched)
        backbone = spark.read.parquet(args.backbone)

        accepted_ids = (
            accepted
            .select(
                F.col(
                    "backbone_establishment_id"
                ).cast("string").alias(
                    "backbone_establishment_id"
                )
            )
            .where(
                F.col(
                    "backbone_establishment_id"
                ).isNotNull()
            )
            .distinct()
        )

        ambiguous_ids = (
            ambiguous
            .select(
                F.col(
                    "backbone_establishment_id"
                ).cast("string").alias(
                    "backbone_establishment_id"
                )
            )
            .where(
                F.col(
                    "backbone_establishment_id"
                ).isNotNull()
            )
            .distinct()
        )

        blocked_backbone_ids = (
            accepted_ids
            .unionByName(ambiguous_ids)
            .distinct()
        )

        backbone_base = (
            backbone
            .select(
                F.col(
                    "restaurant_backbone_id"
                ).cast("string").alias(
                    "backbone_establishment_id"
                ),
                F.col("name").cast("string").alias(
                    "backbone_raw_name"
                ),
                F.col("state").cast("string").alias(
                    "backbone_state_raw"
                ),
                F.col(
                    "census_place_name"
                ).cast("string").alias(
                    "census_place_name"
                ),
                F.col(
                    "city_nonstandardized"
                ).cast("string").alias(
                    "city_nonstandardized"
                ),
            )
            .join(
                blocked_backbone_ids,
                "backbone_establishment_id",
                "left_anti",
            )
            .withColumn(
                "backbone_state_canonical",
                normalize_state_udf(
                    F.col("backbone_state_raw")
                ),
            )
            .withColumn(
                "backbone_legal_name",
                legal_normalize_udf(
                    F.col("backbone_raw_name")
                ),
            )
            .where(
                F.col(
                    "backbone_establishment_id"
                ).isNotNull()
                & F.col(
                    "backbone_state_canonical"
                ).isNotNull()
                & F.col(
                    "backbone_legal_name"
                ).isNotNull()
            )
        )

        backbone_census = (
            backbone_base
            .withColumn(
                "backbone_geography_raw",
                F.col("census_place_name"),
            )
            .withColumn(
                "backbone_geography_normalized",
                normalize_text_udf(
                    F.col("census_place_name")
                ),
            )
            .withColumn(
                "matched_geography_source",
                F.lit("census_place_name"),
            )
        )

        backbone_city = (
            backbone_base
            .withColumn(
                "backbone_geography_raw",
                F.col("city_nonstandardized"),
            )
            .withColumn(
                "backbone_geography_normalized",
                normalize_text_udf(
                    F.col("city_nonstandardized")
                ),
            )
            .withColumn(
                "matched_geography_source",
                F.lit("city_nonstandardized"),
            )
        )

        backbone_geo = (
            backbone_census
            .unionByName(
                backbone_city,
                allowMissingColumns=True,
            )
            .where(
                F.col(
                    "backbone_geography_normalized"
                ).isNotNull()
            )
            .select(
                "backbone_establishment_id",
                "backbone_raw_name",
                "backbone_legal_name",
                "backbone_state_canonical",
                "backbone_geography_raw",
                "backbone_geography_normalized",
                "matched_geography_source",
            )
            .dropDuplicates(
                [
                    "backbone_establishment_id",
                    "backbone_state_canonical",
                    "backbone_geography_normalized",
                ]
            )
            .withColumn(
                "backbone_name_length",
                F.length("backbone_legal_name"),
            )
            .withColumn(
                "name_prefix4",
                F.substring(
                    "backbone_legal_name",
                    1,
                    4,
                ),
            )
            .where(
                F.col("backbone_name_length")
                >= args.minimum_name_length
            )
        )

        unmatched_base = (
            unmatched
            .select(
                "*",
                F.col(
                    "revelio_legal_suffix_normalized_company_name"
                ).cast("string").alias(
                    "_revelio_legal_name"
                ),
            )
            .where(
                F.col(
                    "revelio_entity_location_key"
                ).isNotNull()
                & F.col(
                    "revelio_state_canonical"
                ).isNotNull()
                & F.col(
                    "revelio_normalized_city"
                ).isNotNull()
                & F.col(
                    "_revelio_legal_name"
                ).isNotNull()
            )
            .withColumn(
                "revelio_name_length",
                F.length("_revelio_legal_name"),
            )
            .withColumn(
                "name_prefix4",
                F.substring(
                    "_revelio_legal_name",
                    1,
                    4,
                ),
            )
            .where(
                F.col("revelio_name_length")
                >= args.minimum_name_length
            )
        )

        candidates = (
            unmatched_base.alias("r")
            .join(
                backbone_geo.alias("b"),
                (
                    F.col("r.revelio_state_canonical")
                    == F.col(
                        "b.backbone_state_canonical"
                    )
                )
                & (
                    F.col("r.revelio_normalized_city")
                    == F.col(
                        "b.backbone_geography_normalized"
                    )
                )
                & (
                    F.col("r.name_prefix4")
                    == F.col("b.name_prefix4")
                ),
                "inner",
            )
            .select(
                F.col("r.*"),
                F.col(
                    "b.backbone_establishment_id"
                ),
                F.col("b.backbone_raw_name"),
                F.col(
                    "b.backbone_legal_name"
                ).alias(
                    "backbone_legal_suffix_normalized_name"
                ),
                F.col(
                    "b.backbone_geography_raw"
                ),
                F.col(
                    "b.backbone_geography_normalized"
                ),
                F.col(
                    "b.matched_geography_source"
                ),
                F.col(
                    "b.backbone_name_length"
                ),
            )
            .dropDuplicates(
                [
                    "revelio_entity_location_key",
                    "backbone_establishment_id",
                ]
            )
            .withColumn(
                "name_max_length",
                F.greatest(
                    F.col("revelio_name_length"),
                    F.col("backbone_name_length"),
                ),
            )
            .withColumn(
                "name_length_difference",
                F.abs(
                    F.col("revelio_name_length")
                    - F.col("backbone_name_length")
                ),
            )
            .where(
                (
                    F.col("name_length_difference")
                    / F.col("name_max_length")
                )
                <= F.lit(args.max_distance_ratio)
            )
            .withColumn(
                "levenshtein_distance",
                F.levenshtein(
                    F.col("_revelio_legal_name"),
                    F.col(
                        "backbone_legal_suffix_normalized_name"
                    ),
                ),
            )
            .withColumn(
                "normalized_edit_distance",
                (
                    F.col("levenshtein_distance")
                    / F.col("name_max_length")
                ),
            )
            .where(
                F.col("normalized_edit_distance")
                <= F.lit(args.max_distance_ratio)
            )
            .cache()
        )

        key_window = Window.partitionBy(
            "revelio_entity_location_key"
        ).orderBy(
            F.col("normalized_edit_distance").asc(),
            F.col("levenshtein_distance").asc(),
            F.col("backbone_establishment_id").asc(),
        )

        backbone_window = Window.partitionBy(
            "backbone_establishment_id"
        ).orderBy(
            F.col("normalized_edit_distance").asc(),
            F.col("levenshtein_distance").asc(),
            F.col("revelio_entity_location_key").asc(),
        )

        ranked = (
            candidates
            .withColumn(
                "key_rank",
                F.row_number().over(key_window),
            )
            .withColumn(
                "key_second_distance",
                F.lead(
                    "normalized_edit_distance",
                    1,
                ).over(key_window),
            )
            .withColumn(
                "backbone_rank",
                F.row_number().over(backbone_window),
            )
            .withColumn(
                "backbone_second_distance",
                F.lead(
                    "normalized_edit_distance",
                    1,
                ).over(backbone_window),
            )
        )

        accepted_fuzzy = (
            ranked
            .where(
                (F.col("key_rank") == 1)
                & (F.col("backbone_rank") == 1)
                & (
                    F.col("key_second_distance").isNull()
                    | (
                        F.col("key_second_distance")
                        - F.col(
                            "normalized_edit_distance"
                        )
                        >= F.lit(args.minimum_margin)
                    )
                )
                & (
                    F.col(
                        "backbone_second_distance"
                    ).isNull()
                    | (
                        F.col(
                            "backbone_second_distance"
                        )
                        - F.col(
                            "normalized_edit_distance"
                        )
                        >= F.lit(args.minimum_margin)
                    )
                )
            )
            .withColumn(
                "match_method",
                F.lit("restricted_fuzzy"),
            )
            .withColumn(
                "match_tier",
                F.lit(
                    "tier5_restricted_fuzzy_name"
                ),
            )
            .withColumn(
                "match_tier_order",
                F.lit(5),
            )
            .withColumn(
                "match_score",
                F.lit(1.0)
                - F.col(
                    "normalized_edit_distance"
                ),
            )
            .withColumn(
                "match_status",
                F.lit("accepted"),
            )
            .withColumn(
                "ambiguity_reason",
                F.lit(None).cast("string"),
            )
            .drop(
                "_revelio_legal_name",
                "name_prefix4",
                "key_rank",
                "backbone_rank",
            )
            .cache()
        )

        accepted_output = (
            outdir
            / "accepted_fuzzy_crosswalk"
        )

        accepted_fuzzy.write.mode(
            "errorifexists"
        ).parquet(
            str(accepted_output)
        )

        candidate_pairs_count = candidates.count()
        accepted_rows = accepted_fuzzy.count()

        summary = {
            "design": {
                "baseline_status": (
                    "Robustness tier only; exact hierarchical "
                    "matches remain the preferred baseline."
                ),
                "maximum_normalized_edit_distance": (
                    args.max_distance_ratio
                ),
                "minimum_second_best_margin": (
                    args.minimum_margin
                ),
                "minimum_name_length": (
                    args.minimum_name_length
                ),
                "name_block": (
                    "Exact first four characters of the "
                    "legal-suffix-normalized name."
                ),
                "geography_block": (
                    "Exact canonical state and exact normalized "
                    "Census place or city_nonstandardized."
                ),
                "reciprocal_unique_best_required": True,
                "accepted_exact_backbone_excluded": True,
                "ambiguous_exact_backbone_excluded": True,
                "ambiguous_exact_revelio_keys_excluded": True,
            },
            "counts": {
                "eligible_unmatched_revelio_keys": (
                    unmatched_base.select(
                        "revelio_entity_location_key"
                    ).distinct().count()
                ),
                "eligible_unused_backbone_establishments": (
                    backbone_geo.select(
                        "backbone_establishment_id"
                    ).distinct().count()
                ),
                "candidate_pairs_after_distance_filter": (
                    candidate_pairs_count
                ),
                "accepted_fuzzy_rows": accepted_rows,
                "accepted_fuzzy_revelio_keys": (
                    accepted_fuzzy.select(
                        "revelio_entity_location_key"
                    ).distinct().count()
                ),
                "accepted_fuzzy_backbone_establishments": (
                    accepted_fuzzy.select(
                        "backbone_establishment_id"
                    ).distinct().count()
                ),
                "accepted_fuzzy_rcids": (
                    accepted_fuzzy.select(
                        "revelio_rcid"
                    ).distinct().count()
                ),
                "accepted_fuzzy_foodservice_spells": int(
                    accepted_fuzzy.agg(
                        F.sum(
                            F.coalesce(
                                F.col(
                                    "n_foodservice_spells"
                                ).cast("long"),
                                F.lit(0),
                            )
                        ).alias("n")
                    ).first()["n"]
                    or 0
                ),
            },
            "validation": {
                "revelio_key_uniqueness_valid": (
                    accepted_rows
                    == accepted_fuzzy.select(
                        "revelio_entity_location_key"
                    ).distinct().count()
                ),
                "backbone_uniqueness_valid": (
                    accepted_rows
                    == accepted_fuzzy.select(
                        "backbone_establishment_id"
                    ).distinct().count()
                ),
            },
            "outputs": {
                "accepted_fuzzy_crosswalk": str(
                    accepted_output
                )
            },
        }

        (
            outdir / "restricted_fuzzy_summary.json"
        ).write_text(
            json.dumps(
                summary,
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

        (
            outdir
            / "_RESTRICTED_FUZZY_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

        print(
            json.dumps(
                summary,
                indent=2,
                sort_keys=True,
            ),
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
