#!/usr/bin/env python3

import argparse
import json
import re
import unicodedata
from pathlib import Path

from pyspark.sql import SparkSession, functions as F
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
        cleaned = LEGAL_SUFFIX_PATTERN.sub(
            "",
            text,
        ).strip()

        if cleaned == text:
            break

        text = cleaned

    return text or None


normalize_text_udf = F.udf(
    normalize_text,
    T.StringType(),
)

legal_normalize_udf = F.udf(
    legal_normalize,
    T.StringType(),
)


def null_column(data_type):
    return F.lit(None).cast(data_type)


def get_column(df, name, data_type):
    if name in df.columns:
        return F.col(name).cast(data_type).alias(name)

    return null_column(data_type).alias(name)


def standardize_crosswalk(df, method):
    return df.select(
        get_column(
            df,
            "backbone_establishment_id",
            "string",
        ),
        get_column(
            df,
            "revelio_entity_location_key",
            "string",
        ),
        get_column(
            df,
            "revelio_rcid",
            "string",
        ),
        get_column(
            df,
            "revelio_raw_company_name",
            "string",
        ),
        get_column(
            df,
            "revelio_city",
            "string",
        ),
        get_column(
            df,
            "revelio_state",
            "string",
        ),
        get_column(
            df,
            "revelio_state_canonical",
            "string",
        ),
        get_column(
            df,
            "revelio_normalized_city",
            "string",
        ),
        get_column(
            df,
            "n_foodservice_spells",
            "long",
        ),
        get_column(
            df,
            "n_people",
            "long",
        ),
        get_column(
            df,
            "n_core_founder_people",
            "long",
        ),
        get_column(
            df,
            "backbone_raw_name",
            "string",
        ),
        get_column(
            df,
            "backbone_geography_raw",
            "string",
        ),
        get_column(
            df,
            "backbone_geography_normalized",
            "string",
        ),
        get_column(
            df,
            "matched_geography_source",
            "string",
        ),
        get_column(
            df,
            "match_tier",
            "string",
        ),
        get_column(
            df,
            "match_tier_order",
            "int",
        ),
        get_column(
            df,
            "match_score",
            "double",
        ),
        get_column(
            df,
            "normalized_edit_distance",
            "double",
        ),
        get_column(
            df,
            "levenshtein_distance",
            "int",
        ),
        F.lit(method).alias("match_method"),
    )


def join_component(column):
    return F.coalesce(
        F.trim(column.cast("string")),
        F.lit("<NULL>"),
    )


def add_component_join_key(
    df,
    rcid_column,
    name_column,
    city_column,
    state_column,
):
    return df.withColumn(
        "_component_join_key",
        F.sha2(
            F.concat_ws(
                "||",
                join_component(F.col(rcid_column)),
                join_component(F.col(name_column)),
                join_component(F.col(city_column)),
                join_component(F.col(state_column)),
            ),
            256,
        ),
    )


def dataframe_rows(df):
    return [
        row.asDict(recursive=True)
        for row in df.collect()
    ]


def chain_summary(crosswalk, sample_name):
    summary = (
        crosswalk.agg(
            F.count("*").alias(
                "matched_establishments"
            ),
            F.sum(
                F.coalesce(
                    F.col("n_foodservice_spells"),
                    F.lit(0),
                )
            ).alias("matched_spells"),
            F.sum(
                F.when(
                    F.col("is_chain_preferred") == 1,
                    1,
                ).otherwise(0)
            ).alias(
                "preferred_chain_establishments"
            ),
            F.sum(
                F.when(
                    F.col("is_chain_preferred") == 1,
                    F.coalesce(
                        F.col("n_foodservice_spells"),
                        F.lit(0),
                    ),
                ).otherwise(0)
            ).alias(
                "preferred_chain_spells"
            ),
            F.sum(
                F.when(
                    F.col("is_chain_brand_only") == 1,
                    1,
                ).otherwise(0)
            ).alias(
                "brand_only_chain_establishments"
            ),
            F.sum(
                F.when(
                    F.col("is_chain_brand_only") == 1,
                    F.coalesce(
                        F.col("n_foodservice_spells"),
                        F.lit(0),
                    ),
                ).otherwise(0)
            ).alias(
                "brand_only_chain_spells"
            ),
            F.sum(
                F.when(
                    F.col("brand_key").isNotNull(),
                    1,
                ).otherwise(0)
            ).alias(
                "matched_establishments_with_brand"
            ),
            F.sum(
                F.when(
                    F.col("brand_key").isNotNull(),
                    F.coalesce(
                        F.col("n_foodservice_spells"),
                        F.lit(0),
                    ),
                ).otherwise(0)
            ).alias(
                "matched_spells_with_brand"
            ),
        )
        .withColumn(
            "sample",
            F.lit(sample_name),
        )
        .withColumn(
            "preferred_chain_establishment_share",
            F.col(
                "preferred_chain_establishments"
            )
            / F.col("matched_establishments"),
        )
        .withColumn(
            "preferred_chain_spell_share",
            F.col("preferred_chain_spells")
            / F.col("matched_spells"),
        )
        .withColumn(
            "brand_only_chain_establishment_share",
            F.col(
                "brand_only_chain_establishments"
            )
            / F.col("matched_establishments"),
        )
        .withColumn(
            "brand_only_chain_spell_share",
            F.col("brand_only_chain_spells")
            / F.col("matched_spells"),
        )
    )

    return summary


def build_sample_outputs(
    sample_name,
    crosswalk,
    spells,
    origin,
    outdir,
):
    expected_spells = int(
        crosswalk.agg(
            F.sum(
                F.coalesce(
                    F.col("n_foodservice_spells"),
                    F.lit(0),
                )
            ).alias("n")
        ).first()["n"]
        or 0
    )

    crosswalk_for_join = add_component_join_key(
        crosswalk,
        "revelio_rcid",
        "revelio_raw_company_name",
        "revelio_city",
        "revelio_state",
    )

    crosswalk_rows = crosswalk_for_join.count()

    crosswalk_join_keys = (
        crosswalk_for_join.select(
            "_component_join_key"
        )
        .distinct()
        .count()
    )

    if crosswalk_rows != crosswalk_join_keys:
        raise RuntimeError(
            f"{sample_name}: component join key is not "
            f"unique: rows={crosswalk_rows}, "
            f"keys={crosswalk_join_keys}"
        )

    match_columns = [
        "backbone_establishment_id",
        "revelio_entity_location_key",
        "match_method",
        "match_tier",
        "match_tier_order",
        "match_score",
        "normalized_edit_distance",
        "levenshtein_distance",
        "matched_geography_source",
        "backbone_raw_name",
        "backbone_geography_raw",
        "is_chain_preferred",
        "is_chain_brand_only",
        "chain_group_source",
        "chain_group_key",
        "chain_group_establishment_count",
        "brand",
        "brand_key",
        "brand_establishment_count",
    ]

    match_lookup = crosswalk_for_join.select(
        "_component_join_key",
        *match_columns,
    )

    matched_spells = (
        spells.join(
            match_lookup,
            "_component_join_key",
            "inner",
        )
        .withColumn(
            "analysis_sample",
            F.lit(sample_name),
        )
        .cache()
    )

    matched_spell_rows = matched_spells.count()

    if matched_spell_rows != expected_spells:
        raise RuntimeError(
            f"{sample_name}: spell reconciliation failed. "
            f"Crosswalk reports {expected_spells:,} spells, "
            f"but component merge produced "
            f"{matched_spell_rows:,} rows."
        )

    origin_rows = origin.count()
    origin_unique = (
        origin.select("user_id")
        .distinct()
        .count()
    )

    if origin_rows != origin_unique:
        raise RuntimeError(
            "Origin-v4 is not unique on user_id."
        )

    enriched_spells = (
        matched_spells.join(
            origin,
            "user_id",
            "left",
        )
        .cache()
    )

    enriched_spell_rows = enriched_spells.count()

    if enriched_spell_rows != matched_spell_rows:
        raise RuntimeError(
            f"{sample_name}: origin join changed spell "
            f"row count from {matched_spell_rows:,} to "
            f"{enriched_spell_rows:,}."
        )

    sample_root = outdir / sample_name

    enriched_spells.write.mode(
        "errorifexists"
    ).parquet(
        str(sample_root / "spells")
    )

    people_summary = (
        matched_spells.groupBy("user_id")
        .agg(
            F.count("*").alias(
                "n_matched_foodservice_spells"
            ),
            F.countDistinct(
                "position_id"
            ).alias(
                "n_distinct_matched_positions"
            ),
            F.countDistinct(
                "backbone_establishment_id"
            ).alias(
                "n_distinct_matched_restaurants"
            ),
            F.sum(
                F.when(
                    F.col("match_method")
                    == "hierarchical_exact",
                    1,
                ).otherwise(0)
            ).alias("n_exact_matched_spells"),
            F.sum(
                F.when(
                    F.col("match_method")
                    == "restricted_fuzzy",
                    1,
                ).otherwise(0)
            ).alias("n_fuzzy_matched_spells"),
            F.sum(
                F.when(
                    F.col("is_chain_preferred") == 1,
                    1,
                ).otherwise(0)
            ).alias(
                "n_preferred_chain_spells"
            ),
            F.sum(
                F.when(
                    F.col("is_chain_brand_only") == 1,
                    1,
                ).otherwise(0)
            ).alias(
                "n_brand_only_chain_spells"
            ),
            F.max(
                "is_chain_preferred"
            ).alias(
                "ever_preferred_chain"
            ),
            F.max(
                "is_chain_brand_only"
            ).alias(
                "ever_brand_only_chain"
            ),
            F.min("startdate").alias(
                "first_matched_spell_startdate"
            ),
            F.max("enddate").alias(
                "last_matched_spell_enddate"
            ),
        )
        .withColumn(
            "preferred_chain_spell_share_person",
            F.col("n_preferred_chain_spells")
            / F.col(
                "n_matched_foodservice_spells"
            ),
        )
        .withColumn(
            "brand_only_chain_spell_share_person",
            F.col("n_brand_only_chain_spells")
            / F.col(
                "n_matched_foodservice_spells"
            ),
        )
        .withColumn(
            "analysis_sample",
            F.lit(sample_name),
        )
        .join(
            origin,
            "user_id",
            "left",
        )
        .cache()
    )

    people_rows = people_summary.count()
    people_unique = (
        people_summary.select("user_id")
        .distinct()
        .count()
    )

    if people_rows != people_unique:
        raise RuntimeError(
            f"{sample_name}: final people output is "
            "not unique on user_id."
        )

    people_summary.write.mode(
        "errorifexists"
    ).parquet(
        str(sample_root / "people")
    )

    return {
        "expected_spells_from_crosswalk": (
            expected_spells
        ),
        "merged_spell_rows": matched_spell_rows,
        "merged_unique_people": people_rows,
        "component_join_key_uniqueness_valid": (
            crosswalk_rows
            == crosswalk_join_keys
        ),
        "spell_reconciliation_valid": (
            matched_spell_rows
            == expected_spells
        ),
        "origin_join_row_preservation_valid": (
            enriched_spell_rows
            == matched_spell_rows
        ),
        "final_people_uniqueness_valid": (
            people_rows
            == people_unique
        ),
        "spells_output": str(
            sample_root / "spells"
        ),
        "people_output": str(
            sample_root / "people"
        ),
    }


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--exact-crosswalk",
        required=True,
    )
    parser.add_argument(
        "--fuzzy-crosswalk",
        required=True,
    )
    parser.add_argument(
        "--backbone",
        required=True,
    )
    parser.add_argument(
        "--spells",
        required=True,
    )
    parser.add_argument(
        "--origin-v4",
        required=True,
    )
    parser.add_argument(
        "--outdir",
        required=True,
    )
    parser.add_argument(
        "--chain-threshold",
        type=int,
        default=100,
    )

    args = parser.parse_args()

    outdir = Path(args.outdir)

    if outdir.exists():
        raise RuntimeError(
            f"Refusing to overwrite existing output: "
            f"{outdir}"
        )

    outdir.mkdir(parents=True)

    spark = (
        SparkSession.builder
        .appName(
            "finalize_foodservice_backbone_linkage"
        )
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
        exact_raw = spark.read.parquet(
            args.exact_crosswalk
        )

        fuzzy_raw = spark.read.parquet(
            args.fuzzy_crosswalk
        )

        backbone = spark.read.parquet(
            args.backbone
        )

        spells_raw = spark.read.parquet(
            args.spells
        )

        origin = spark.read.parquet(
            args.origin_v4
        )

        exact = standardize_crosswalk(
            exact_raw,
            "hierarchical_exact",
        )

        fuzzy = standardize_crosswalk(
            fuzzy_raw,
            "restricted_fuzzy",
        )

        combined = (
            exact.unionByName(
                fuzzy,
                allowMissingColumns=True,
            )
            .cache()
        )

        exact_rows = exact.count()
        fuzzy_rows = fuzzy.count()
        combined_rows = combined.count()

        combined_key_count = (
            combined.select(
                "revelio_entity_location_key"
            )
            .distinct()
            .count()
        )

        combined_backbone_count = (
            combined.select(
                "backbone_establishment_id"
            )
            .distinct()
            .count()
        )

        if combined_rows != combined_key_count:
            raise RuntimeError(
                "Exact-plus-fuzzy crosswalk is not "
                "unique on Revelio key."
            )

        if combined_rows != combined_backbone_count:
            raise RuntimeError(
                "Exact-plus-fuzzy crosswalk is not "
                "unique on backbone establishment."
            )

        backbone_base = (
            backbone.select(
                F.col(
                    "restaurant_backbone_id"
                ).cast("string").alias(
                    "backbone_establishment_id"
                ),
                F.col("name").cast("string").alias(
                    "backbone_master_name"
                ),
                F.col("brand").cast("string").alias(
                    "brand"
                ),
            )
            .withColumn(
                "brand_key",
                normalize_text_udf(
                    F.col("brand")
                ),
            )
            .withColumn(
                "name_fallback_key",
                legal_normalize_udf(
                    F.col("backbone_master_name")
                ),
            )
            .withColumn(
                "chain_group_source",
                F.when(
                    F.col("brand_key").isNotNull(),
                    F.lit("brand"),
                ).otherwise(
                    F.lit(
                        "legal_normalized_name_fallback"
                    )
                ),
            )
            .withColumn(
                "chain_group_key",
                F.when(
                    F.col("brand_key").isNotNull(),
                    F.concat(
                        F.lit("brand::"),
                        F.col("brand_key"),
                    ),
                ).otherwise(
                    F.concat(
                        F.lit("name::"),
                        F.col("name_fallback_key"),
                    )
                ),
            )
            .where(
                F.col(
                    "backbone_establishment_id"
                ).isNotNull()
            )
            .cache()
        )

        preferred_group_counts = (
            backbone_base.where(
                F.col("chain_group_key").isNotNull()
            )
            .groupBy("chain_group_key")
            .agg(
                F.countDistinct(
                    "backbone_establishment_id"
                ).alias(
                    "chain_group_establishment_count"
                )
            )
        )

        brand_group_counts = (
            backbone_base.where(
                F.col("brand_key").isNotNull()
            )
            .groupBy("brand_key")
            .agg(
                F.countDistinct(
                    "backbone_establishment_id"
                ).alias(
                    "brand_establishment_count"
                )
            )
        )

        chain_lookup = (
            backbone_base.join(
                preferred_group_counts,
                "chain_group_key",
                "left",
            )
            .join(
                brand_group_counts,
                "brand_key",
                "left",
            )
            .withColumn(
                "is_chain_preferred",
                F.when(
                    F.col(
                        "chain_group_establishment_count"
                    )
                    >= args.chain_threshold,
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .withColumn(
                "is_chain_brand_only",
                F.when(
                    F.col("brand_key").isNotNull()
                    & (
                        F.col(
                            "brand_establishment_count"
                        )
                        >= args.chain_threshold
                    ),
                    F.lit(1),
                ).otherwise(F.lit(0)),
            )
            .select(
                "backbone_establishment_id",
                "backbone_master_name",
                "brand",
                "brand_key",
                "name_fallback_key",
                "chain_group_source",
                "chain_group_key",
                "chain_group_establishment_count",
                "brand_establishment_count",
                "is_chain_preferred",
                "is_chain_brand_only",
            )
            .cache()
        )

        exact_annotated = (
            exact.join(
                chain_lookup,
                "backbone_establishment_id",
                "left",
            )
            .cache()
        )

        combined_annotated = (
            combined.join(
                chain_lookup,
                "backbone_establishment_id",
                "left",
            )
            .cache()
        )

        missing_chain_exact = (
            exact_annotated.where(
                F.col("chain_group_key").isNull()
            ).count()
        )

        missing_chain_combined = (
            combined_annotated.where(
                F.col("chain_group_key").isNull()
            ).count()
        )

        exact_annotated.write.mode(
            "errorifexists"
        ).parquet(
            str(
                outdir
                / "crosswalks"
                / "exact_baseline"
            )
        )

        combined_annotated.write.mode(
            "errorifexists"
        ).parquet(
            str(
                outdir
                / "crosswalks"
                / "exact_plus_fuzzy"
            )
        )

        chain_sample_summary = (
            chain_summary(
                exact_annotated,
                "exact_baseline",
            )
            .unionByName(
                chain_summary(
                    combined_annotated,
                    "exact_plus_fuzzy",
                )
            )
        )

        chain_tier_summary = (
            combined_annotated.groupBy(
                "match_method",
                "match_tier",
                "match_tier_order",
            )
            .agg(
                F.count("*").alias(
                    "matched_establishments"
                ),
                F.sum(
                    F.coalesce(
                        F.col(
                            "n_foodservice_spells"
                        ),
                        F.lit(0),
                    )
                ).alias("matched_spells"),
                F.sum(
                    F.when(
                        F.col(
                            "is_chain_preferred"
                        )
                        == 1,
                        1,
                    ).otherwise(0)
                ).alias(
                    "preferred_chain_establishments"
                ),
                F.sum(
                    F.when(
                        F.col(
                            "is_chain_preferred"
                        )
                        == 1,
                        F.coalesce(
                            F.col(
                                "n_foodservice_spells"
                            ),
                            F.lit(0),
                        ),
                    ).otherwise(0)
                ).alias(
                    "preferred_chain_spells"
                ),
            )
            .withColumn(
                "preferred_chain_establishment_share",
                F.col(
                    "preferred_chain_establishments"
                )
                / F.col(
                    "matched_establishments"
                ),
            )
            .withColumn(
                "preferred_chain_spell_share",
                F.col(
                    "preferred_chain_spells"
                )
                / F.col("matched_spells"),
            )
            .orderBy("match_tier_order")
        )

        chain_sample_summary.write.mode(
            "errorifexists"
        ).parquet(
            str(
                outdir
                / "tables"
                / "chain_summary_by_sample"
            )
        )

        chain_tier_summary.write.mode(
            "errorifexists"
        ).parquet(
            str(
                outdir
                / "tables"
                / "chain_summary_by_tier"
            )
        )

        spells = add_component_join_key(
            spells_raw,
            "rcid",
            "company_name",
            "city",
            "state",
        ).cache()

        exact_outputs = build_sample_outputs(
            "exact_baseline",
            exact_annotated,
            spells,
            origin,
            outdir,
        )

        robustness_outputs = build_sample_outputs(
            "exact_plus_fuzzy",
            combined_annotated,
            spells,
            origin,
            outdir,
        )

        backbone_chain_counts = (
            chain_lookup.agg(
                F.count("*").alias(
                    "backbone_establishments"
                ),
                F.sum(
                    "is_chain_preferred"
                ).alias(
                    "preferred_chain_establishments"
                ),
                F.sum(
                    "is_chain_brand_only"
                ).alias(
                    "brand_only_chain_establishments"
                ),
            )
            .first()
            .asDict()
        )

        report = {
            "definition": {
                "preferred_chain": (
                    "Use normalized backbone brand when "
                    "available; otherwise use the "
                    "legal-suffix-normalized restaurant "
                    "name. A group is a chain when it has "
                    f"at least {args.chain_threshold} "
                    "backbone establishments."
                ),
                "brand_only_robustness": (
                    "Use normalized brand only. A brand is "
                    "a chain when it has at least "
                    f"{args.chain_threshold} backbone "
                    "establishments."
                ),
                "baseline_sample": (
                    "Sequential hierarchical exact matches "
                    "only."
                ),
                "robustness_sample": (
                    "Hierarchical exact plus the restricted "
                    "fuzzy tier."
                ),
                "origin_proxy": (
                    "Any valid non-U.S. education country "
                    "or first observed job abroad, from "
                    "people_origin_v4."
                ),
            },
            "crosswalk_counts": {
                "exact_rows": exact_rows,
                "fuzzy_rows": fuzzy_rows,
                "exact_plus_fuzzy_rows": combined_rows,
                "exact_plus_fuzzy_unique_revelio_keys": (
                    combined_key_count
                ),
                "exact_plus_fuzzy_unique_backbone_establishments": (
                    combined_backbone_count
                ),
            },
            "backbone_chain_counts": {
                key: int(value or 0)
                for key, value in (
                    backbone_chain_counts.items()
                )
            },
            "chain_summary_by_sample": dataframe_rows(
                chain_sample_summary
            ),
            "chain_summary_by_tier": dataframe_rows(
                chain_tier_summary
            ),
            "sample_outputs": {
                "exact_baseline": exact_outputs,
                "exact_plus_fuzzy": (
                    robustness_outputs
                ),
            },
            "validation": {
                "combined_revelio_key_uniqueness_valid": (
                    combined_rows
                    == combined_key_count
                ),
                "combined_backbone_uniqueness_valid": (
                    combined_rows
                    == combined_backbone_count
                ),
                "exact_matches_missing_chain_group": (
                    missing_chain_exact
                ),
                "combined_matches_missing_chain_group": (
                    missing_chain_combined
                ),
                "origin_v4_path": args.origin_v4,
            },
            "warning": (
                "The fuzzy tier is a robustness extension, "
                "not part of the preferred exact-match "
                "baseline. The country-of-origin variable "
                "is a LinkedIn-history proxy, not verified "
                "nationality, citizenship, birthplace, or "
                "immigration status."
            ),
        }

        (
            outdir
            / "final_linkage_manifest.json"
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
            outdir
            / "_FINAL_LINKAGE_COMPLETE"
        ).write_text(
            "complete\n",
            encoding="utf-8",
        )

        print(
            json.dumps(
                report,
                indent=2,
                sort_keys=True,
                default=str,
            ),
            flush=True,
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
