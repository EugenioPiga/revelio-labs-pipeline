#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
import unicodedata
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T


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
    "WISCONSIN": "WI", "WYOMING": "WY", "PUERTO RICO": "PR",
    "GUAM": "GU", "AMERICAN SAMOA": "AS",
    "U S VIRGIN ISLANDS": "VI", "US VIRGIN ISLANDS": "VI",
    "NORTHERN MARIANA ISLANDS": "MP",
}


@F.udf(T.StringType())
def unicode_nfkc(value):
    if value is None:
        return None
    return unicodedata.normalize("NFKC", str(value)).casefold()


@F.udf(T.StringType())
def canonical_state(value):
    if value is None:
        return None

    text = unicodedata.normalize("NFKC", str(value)).strip().upper()
    text = text.replace(".", " ")
    text = re.sub(r"[^A-Z0-9]+", " ", text)
    text = " ".join(text.split())

    invalid = {
        "", "EMPTY", "NULL", "NONE", "NAN", "NA", "N A", "N/A",
        "UNKNOWN", "MISSING", "NOT AVAILABLE", "UNAVAILABLE"
    }

    if text in invalid:
        return None

    if text in STATE_MAP:
        return STATE_MAP[text]

    if text in {"WASHINGTON DC", "WASHINGTON D C", "DC WASHINGTON"}:
        return "DC"

    if len(text) == 2 and text.isalpha():
        return text

    return text


def norm_text(col):
    x = F.lower(F.trim(unicode_nfkc(col.cast("string"))))
    x = F.regexp_replace(x, r"[^\p{L}\p{N}]+", " ")
    x = F.trim(F.regexp_replace(x, r"\s+", " "))

    invalid = [
        "empty", "null", "none", "nan", "na", "n a", "n/a",
        "unknown", "missing", "not available", "unavailable"
    ]

    return (
        F.when(F.length(x) == 0, F.lit(None).cast("string"))
         .when(x.isin(*invalid), F.lit(None).cast("string"))
         .otherwise(x)
    )


def norm_legal_suffix(col):
    x = F.regexp_replace(
        col,
        r"(?i)(?:\s+(?:llc|l l c|inc|incorporated|ltd|limited|corp|corporation|pllc|llp|lp))+$",
        "",
    )
    x = F.trim(F.regexp_replace(x, r"\s+", " "))
    return F.when(F.length(x) > 0, x).otherwise(F.lit(None).cast("string"))


def valid(name):
    return (
        F.col(name).isNotNull()
        & (F.length(F.trim(F.col(name).cast("string"))) > 0)
    )


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as handle:
        json.dump(payload, handle, indent=2, sort_keys=True, default=str)


def require_columns(df, columns, label):
    missing = [x for x in columns if x not in df.columns]
    if missing:
        raise RuntimeError(f"{label} missing required columns: {missing}")


def assert_success(path, label):
    marker = os.path.join(path, "_SUCCESS")
    if not os.path.isfile(marker):
        raise RuntimeError(f"Missing _SUCCESS for {label}: {marker}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--backbone", required=True)
    parser.add_argument("--spells", required=True)
    parser.add_argument("--key-out", required=True)
    parser.add_argument("--report-out", required=True)
    parser.add_argument("--run-id", required=True)
    args = parser.parse_args()

    key_out = Path(args.key_out)
    report_out = Path(args.report_out)

    if key_out.exists() or report_out.exists():
        raise RuntimeError(
            "Refusing to overwrite an existing run directory: "
            f"key_out={key_out}, report_out={report_out}"
        )

    key_out.mkdir(parents=True, exist_ok=False)
    report_out.mkdir(parents=True, exist_ok=False)

    assert_success(args.spells, "selected Revelio NAICS-722 spell input")

    spark = (
        SparkSession.builder
        .appName("foodservice_722_backbone_keys_geo_audit_v1")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        spells = spark.read.parquet(args.spells)
        backbone = spark.read.parquet(args.backbone)

        require_columns(
            spells,
            [
                "user_id", "rcid", "company_name", "city", "state",
                "msa", "restaurant_core_founder_flag",
            ],
            "Revelio selected spells",
        )
        require_columns(
            backbone,
            [
                "restaurant_backbone_id", "name", "state",
                "census_place_name", "census_place_namelsad",
                "address", "latitude", "longitude",
                "origin_harmonized_1", "origin_harmonized_iso1",
                "osm_cuisines",
            ],
            "restaurant backbone",
        )

        cuisine_fields = [
            x for x in backbone.columns
            if x == "osm_cuisines"
            or x.startswith("origin_harmonized_")
        ]

        # Census place fields are selected explicitly below as matching-block
        # geography proxies. Do not include them again here, or Spark receives
        # duplicate output aliases.
        backbone_audit_fields = [
            x for x in [
                "backbone_source_status", "brand_harmonized", "address",
                "latitude", "longitude", "zip", "county",
                "safegraph_placekey", "safegraph_opened_on",
                "safegraph_closed_on", "safegraph_naics_code",
                "safegraph_top_category", "safegraph_sub_category",
            ]
            if x in backbone.columns
        ]

        # Revelio: smallest operational entity-location unit.
        revelio_raw = spells.select(
            F.col("rcid").cast("string").alias("revelio_rcid"),
            F.col("company_name").cast("string").alias("revelio_raw_company_name"),
            F.col("city").cast("string").alias("revelio_city"),
            F.col("state").cast("string").alias("revelio_state"),
            F.col("msa").cast("string").alias("revelio_msa"),
            F.col("user_id").cast("string").alias("revelio_user_id"),
            F.col("restaurant_core_founder_flag").cast("int").alias("core_founder_flag"),
        )

        revelio_keys = (
            revelio_raw
            .groupBy(
                "revelio_rcid",
                "revelio_raw_company_name",
                "revelio_city",
                "revelio_state",
            )
            .agg(
                F.count(F.lit(1)).alias("n_foodservice_spells"),
                F.countDistinct("revelio_user_id").alias("n_people"),
                F.countDistinct(
                    F.when(
                        F.col("core_founder_flag") == 1,
                        F.col("revelio_user_id"),
                    )
                ).alias("n_core_founder_people"),
                F.first("revelio_msa", ignorenulls=True).alias("revelio_msa"),
                F.countDistinct("revelio_msa").alias("n_distinct_msa_values"),
            )
            .withColumn(
                "revelio_normalized_company_name",
                norm_text(F.col("revelio_raw_company_name")),
            )
            .withColumn(
                "revelio_legal_suffix_normalized_company_name",
                norm_legal_suffix(F.col("revelio_normalized_company_name")),
            )
            .withColumn("revelio_normalized_city", norm_text(F.col("revelio_city")))
            .withColumn(
                "revelio_state_canonical",
                canonical_state(F.col("revelio_state")),
            )
            .withColumn(
                "revelio_entity_location_key",
                F.sha2(
                    F.concat_ws(
                        "||",
                        F.col("revelio_rcid"),
                        F.coalesce(F.col("revelio_normalized_company_name"), F.lit("")),
                        F.coalesce(F.col("revelio_normalized_city"), F.lit("")),
                        F.coalesce(F.col("revelio_state_canonical"), F.lit("")),
                    ),
                    256,
                ),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        # Backbone: one row is one backbone establishment; Census place is retained
        # as a geographic proxy, never interpreted as an address-level match.
        backbone_keys = (
            backbone.select(
                F.col("restaurant_backbone_id").cast("string").alias(
                    "backbone_establishment_id"
                ),
                F.col("name").cast("string").alias("backbone_raw_restaurant_name"),
                F.col("state").cast("string").alias("backbone_state"),
                F.col("census_place_name").cast("string").alias(
                    "backbone_census_place_name"
                ),
                F.col("census_place_namelsad").cast("string").alias(
                    "backbone_census_place_namelsad"
                ),
                *[F.col(x).alias("backbone_" + x) for x in backbone_audit_fields],
                *[F.col(x).alias("backbone_" + x) for x in cuisine_fields],
            )
            .withColumn(
                "backbone_normalized_restaurant_name",
                norm_text(F.col("backbone_raw_restaurant_name")),
            )
            .withColumn(
                "backbone_legal_suffix_normalized_restaurant_name",
                norm_legal_suffix(F.col("backbone_normalized_restaurant_name")),
            )
            .withColumn(
                "backbone_normalized_place_proxy",
                norm_text(F.col("backbone_census_place_name")),
            )
            .withColumn(
                "backbone_state_canonical",
                canonical_state(F.col("backbone_state")),
            )
            .withColumn(
                "backbone_geography_proxy_type",
                F.lit("census_place_name"),
            )
            .persist(StorageLevel.MEMORY_AND_DISK)
        )

        n_revelio_keys = revelio_keys.count()
        n_backbone_keys = backbone_keys.count()

        n_unique_backbone_ids = (
            backbone_keys.select("backbone_establishment_id").distinct().count()
        )
        if n_unique_backbone_ids != n_backbone_keys:
            raise RuntimeError(
                "Backbone establishment ID is not unique in this run; "
                "stopping before candidate generation."
            )

        rkey_path = str(key_out / "revelio_entity_location_keys")
        bkey_path = str(key_out / "backbone_establishment_keys")

        revelio_keys.write.mode("errorifexists").parquet(rkey_path)
        backbone_keys.write.mode("errorifexists").parquet(bkey_path)

        # Exact geographic-block compatibility only. This does not compare names.
        rblocks = (
            revelio_keys
            .where(valid("revelio_state_canonical") & valid("revelio_normalized_city"))
            .groupBy("revelio_state_canonical", "revelio_normalized_city")
            .agg(
                F.count(F.lit(1)).alias("n_revelio_entity_location_keys"),
                F.sum("n_foodservice_spells").alias("n_revelio_spells"),
                F.sum("n_people").alias("n_revelio_people"),
            )
        )

        bblocks = (
            backbone_keys
            .where(
                valid("backbone_state_canonical")
                & valid("backbone_normalized_place_proxy")
            )
            .groupBy("backbone_state_canonical", "backbone_normalized_place_proxy")
            .agg(
                F.count(F.lit(1)).alias("n_backbone_establishments"),
                F.countDistinct(
                    "backbone_normalized_restaurant_name"
                ).alias("n_backbone_normalized_names"),
            )
        )

        geo_blocks = (
            rblocks.alias("r")
            .join(
                bblocks.alias("b"),
                (
                    (F.col("r.revelio_state_canonical") == F.col("b.backbone_state_canonical"))
                    & (F.col("r.revelio_normalized_city") == F.col("b.backbone_normalized_place_proxy"))
                ),
                "left",
            )
            .select(
                F.col("r.revelio_state_canonical").alias("state_canonical"),
                F.col("r.revelio_normalized_city").alias("normalized_city_or_place"),
                "n_revelio_entity_location_keys",
                "n_revelio_spells",
                "n_revelio_people",
                "n_backbone_establishments",
                "n_backbone_normalized_names",
            )
            .withColumn(
                "has_exact_geographic_block",
                F.col("n_backbone_establishments").isNotNull(),
            )
            .withColumn(
                "potential_all_pairs_name_comparisons",
                F.when(
                    F.col("n_backbone_establishments").isNotNull(),
                    F.col("n_revelio_entity_location_keys")
                    * F.col("n_backbone_establishments"),
                ).otherwise(F.lit(0)),
            )
        )

        geo_path = str(report_out / "geographic_block_summary")
        geo_blocks.write.mode("errorifexists").parquet(geo_path)

        unmatched_geo = (
            geo_blocks
            .where(~F.col("has_exact_geographic_block"))
            .orderBy(F.desc("n_revelio_entity_location_keys"))
        )
        unmatched_geo_path = str(report_out / "revelio_geographic_blocks_without_backbone_place")
        unmatched_geo.write.mode("errorifexists").parquet(unmatched_geo_path)

        rstate_map = (
            revelio_keys
            .groupBy("revelio_state", "revelio_state_canonical")
            .agg(F.count(F.lit(1)).alias("n_revelio_keys"))
        )
        bstate_map = (
            backbone_keys
            .groupBy("backbone_state", "backbone_state_canonical")
            .agg(F.count(F.lit(1)).alias("n_backbone_establishments"))
        )

        rstate_path = str(report_out / "revelio_state_mapping")
        bstate_path = str(report_out / "backbone_state_mapping")
        rstate_map.write.mode("errorifexists").parquet(rstate_path)
        bstate_map.write.mode("errorifexists").parquet(bstate_path)

        geo_summary = geo_blocks.agg(
            F.count(F.lit(1)).alias("n_revelio_geographic_blocks"),
            F.sum(F.when(F.col("has_exact_geographic_block"), 1).otherwise(0)).alias(
                "n_revelio_geographic_blocks_with_backbone_place"
            ),
            F.sum(
                F.when(
                    F.col("has_exact_geographic_block"),
                    F.col("n_revelio_entity_location_keys"),
                ).otherwise(0)
            ).alias("n_revelio_keys_in_overlapping_geo_blocks"),
            F.sum(
                F.when(
                    F.col("has_exact_geographic_block"),
                    F.col("n_revelio_spells"),
                ).otherwise(0)
            ).alias("n_revelio_spells_in_overlapping_geo_blocks"),
            F.sum(
                F.when(
                    F.col("has_exact_geographic_block"),
                    F.col("n_revelio_people"),
                ).otherwise(0)
            ).alias("n_revelio_people_in_overlapping_geo_blocks"),
            F.sum("potential_all_pairs_name_comparisons").alias(
                "potential_all_pairs_within_geo_blocks"
            ),
            F.max("potential_all_pairs_name_comparisons").alias(
                "max_potential_all_pairs_single_geo_block"
            ),
        ).first().asDict()

        backbone_geo_eligible = backbone_keys.where(
            valid("backbone_state_canonical")
            & valid("backbone_normalized_place_proxy")
        ).count()

        revelio_geo_eligible = revelio_keys.where(
            valid("revelio_state_canonical")
            & valid("revelio_normalized_city")
        ).count()

        top_uncovered = [
            x.asDict(recursive=True)
            for x in unmatched_geo.limit(50).collect()
        ]

        report = {
            "run_id": args.run_id,
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "purpose": (
                "Prepare unique entity-location keys and audit exact state-plus-city/"
                "Census-place compatibility. No name candidates or accepted matches "
                "are generated in this stage."
            ),
            "matching_unit": (
                "Revelio rcid × raw company name × city × canonical state. "
                "Backbone restaurant_backbone_id with name × census_place_name "
                "× canonical state."
            ),
            "geographic_rule": (
                "Candidate generation may use only exact agreement between "
                "canonical state and normalized Revelio city versus normalized "
                "backbone census_place_name. Census place is a geographic proxy, "
                "not an address or legally verified location."
            ),
            "normalization_rules": {
                "text": (
                    "Unicode NFKC normalization; lowercase; punctuation and "
                    "whitespace collapse; no removal of cuisine, food, chain, "
                    "or geographic words."
                ),
                "legal_suffix_variant": (
                    "Only terminal LLC, L.L.C., Inc., Incorporated, Ltd., Limited, "
                    "Corp., Corporation, PLLC, LLP, and LP may be removed."
                ),
                "state": (
                    "Canonical U.S. and territorial postal codes from full names "
                    "or abbreviations."
                ),
            },
            "counts": {
                "n_revelio_entity_location_keys": n_revelio_keys,
                "n_backbone_establishment_keys": n_backbone_keys,
                "n_unique_backbone_ids": n_unique_backbone_ids,
                "n_revelio_keys_with_valid_state_city": revelio_geo_eligible,
                "n_backbone_keys_with_valid_state_census_place": backbone_geo_eligible,
                **{
                    key: int(value or 0)
                    for key, value in geo_summary.items()
                },
            },
            "top_revelio_geo_blocks_without_backbone_place": top_uncovered,
            "outputs": {
                "revelio_entity_location_keys": rkey_path,
                "backbone_establishment_keys": bkey_path,
                "geographic_block_summary": geo_path,
                "unmatched_revelio_geographic_blocks": unmatched_geo_path,
                "revelio_state_mapping": rstate_path,
                "backbone_state_mapping": bstate_path,
            },
        }

        write_json(report_out / "keys_geo_audit.json", report)
        write_json(
            key_out / "manifest.json",
            {
                "run_id": args.run_id,
                "status": "keys_and_geography_complete",
                "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
                "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
                "inputs": {
                    "backbone": args.backbone,
                    "revelio_selected_spells": args.spells,
                },
                "outputs": report["outputs"],
                "normalization_rules": report["normalization_rules"],
            },
        )

        (key_out / "_KEYS_AND_GEO_COMPLETE").write_text(
            "keys and geographic audit complete\n",
            encoding="utf-8",
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
