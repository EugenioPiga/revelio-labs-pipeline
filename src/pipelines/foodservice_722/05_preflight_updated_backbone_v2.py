#!/usr/bin/env python3
import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path

from pyspark.sql import SparkSession, functions as F


INVALID_STRINGS = {
    "", "empty", "null", "none", "nan", "na", "n/a",
    "unknown", "missing", "not available", "unavailable"
}


def write_json(path, payload):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(payload, f, indent=2, sort_keys=True, default=str)


def norm_name(s):
    return re.sub(r"[^a-z0-9]+", "_", s.lower()).strip("_")


def valid_expr(colname):
    x = F.lower(F.trim(F.col(colname).cast("string")))
    return F.col(colname).isNotNull() & (~x.isin(*sorted(INVALID_STRINGS))) & (F.length(x) > 0)


def classify_columns(columns):
    groups = {
        "id_fields": [],
        "name_fields": [],
        "city_place_fields": [],
        "cbsa_metro_fields": [],
        "state_fields": [],
        "address_coordinate_fields": [],
        "brand_chain_fields": [],
        "cuisine_origin_fields": [],
        "employment_size_fields": [],
        "date_time_fields": [],
        "possible_matching_fields": [],
    }

    for c in columns:
        l = c.lower()

        if any(k in l for k in ["id", "placekey"]):
            groups["id_fields"].append(c)

        if "name" in l or "restaurant" in l:
            groups["name_fields"].append(c)

        if any(k in l for k in ["city", "place", "town", "municipal"]):
            groups["city_place_fields"].append(c)

        if any(k in l for k in ["cbsa", "metro", "msa"]):
            groups["cbsa_metro_fields"].append(c)

        if l in {"state", "state_abbr", "state_name"} or l.endswith("_state") or "state" in l:
            groups["state_fields"].append(c)

        if any(k in l for k in ["address", "latitude", "longitude", "lat", "lon", "tract", "block", "zip", "county", "geoid"]):
            groups["address_coordinate_fields"].append(c)

        if any(k in l for k in ["brand", "chain", "parent", "company", "office", "location_count", "number_of_restaurants", "number_of_locations"]):
            groups["brand_chain_fields"].append(c)

        if any(k in l for k in ["cuisine", "origin", "iso"]):
            groups["cuisine_origin_fields"].append(c)

        if any(k in l for k in ["employee", "employment", "revenue", "visits", "spend"]):
            groups["employment_size_fields"].append(c)

        if any(k in l for k in ["date", "year", "opened", "closed", "observed"]):
            groups["date_time_fields"].append(c)

    possible = set()
    for g in [
        "id_fields",
        "name_fields",
        "city_place_fields",
        "cbsa_metro_fields",
        "state_fields",
        "brand_chain_fields",
        "cuisine_origin_fields",
    ]:
        possible.update(groups[g])
    groups["possible_matching_fields"] = sorted(possible)

    return {k: sorted(v) for k, v in groups.items()}


def field_metrics(df, fields, row_count):
    out = []
    for c in fields:
        try:
            valid_count = df.where(valid_expr(c)).count()
            missing_count = row_count - valid_count
            approx_distinct = df.where(valid_expr(c)).agg(F.approx_count_distinct(F.col(c)).alias("n")).first()["n"]
            out.append(
                {
                    "field": c,
                    "valid_count": int(valid_count),
                    "missing_count": int(missing_count),
                    "missing_rate": float(missing_count / row_count) if row_count else None,
                    "approx_distinct_nonmissing": int(approx_distinct) if approx_distinct is not None else None,
                    "data_type": str(dict(df.dtypes).get(c)),
                }
            )
        except Exception as e:
            out.append(
                {
                    "field": c,
                    "error": repr(e),
                    "data_type": str(dict(df.dtypes).get(c)),
                }
            )
    return out


def top_values(df, field, n=25):
    if field not in df.columns:
        return []
    rows = (
        df.where(valid_expr(field))
        .groupBy(F.col(field).cast("string").alias("value"))
        .agg(F.count("*").alias("n_rows"))
        .orderBy(F.desc("n_rows"), F.asc("value"))
        .limit(n)
        .collect()
    )
    return [r.asDict() for r in rows]


def cbsa_parse_preview(df, field):
    if field not in df.columns:
        return []

    # Conservative preview only: split text before comma on hyphen/en dash/em dash.
    rows = (
        df.where(valid_expr(field))
        .select(F.col(field).cast("string").alias("cbsa_title"))
        .dropDuplicates()
        .limit(1000)
        .collect()
    )

    out = []
    for r in rows[:200]:
        title = r["cbsa_title"]
        before_comma = title.split(",")[0]
        pieces = [
            p.strip()
            for p in re.split(r"\s*[-–—]\s*", before_comma)
            if p.strip()
        ]
        out.append(
            {
                "cbsa_title": title,
                "before_comma": before_comma,
                "parsed_components_preview": pieces,
            }
        )
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--backbone", required=True)
    ap.add_argument("--outdir", required=True)
    ap.add_argument("--run-id", required=True)
    args = ap.parse_args()

    outdir = Path(args.outdir)
    if outdir.exists():
        raise RuntimeError(f"Refusing to overwrite existing outdir: {outdir}")
    outdir.mkdir(parents=True)

    spark = (
        SparkSession.builder
        .appName("updated_restaurant_backbone_preflight_v2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        df = spark.read.parquet(args.backbone)
        row_count = df.count()
        columns = df.columns
        dtypes = [{"field": c, "type": t} for c, t in df.dtypes]

        groups = classify_columns(columns)

        required_core = ["restaurant_backbone_id", "name", "state"]
        required_status = {
            c: {
                "present": c in columns,
                "valid_count": int(df.where(valid_expr(c)).count()) if c in columns else None,
            }
            for c in required_core
        }

        unique_id_count = None
        duplicate_id_rows = None
        max_rows_per_id = None
        if "restaurant_backbone_id" in columns:
            id_counts = df.groupBy("restaurant_backbone_id").agg(F.count("*").alias("n_rows"))
            unique_id_count = id_counts.count()
            duplicate_id_rows = id_counts.where(F.col("n_rows") > 1).count()
            max_rows_per_id = id_counts.agg(F.max("n_rows").alias("m")).first()["m"]

        fields_to_measure = []
        for g in [
            "name_fields",
            "city_place_fields",
            "cbsa_metro_fields",
            "state_fields",
            "brand_chain_fields",
            "cuisine_origin_fields",
            "address_coordinate_fields",
            "employment_size_fields",
            "date_time_fields",
        ]:
            fields_to_measure.extend(groups[g])
        fields_to_measure = sorted(set(fields_to_measure))

        metrics = field_metrics(df, fields_to_measure, row_count)

        top_value_fields = []
        for preferred in [
            "city_nonstandardized",
            "city",
            "census_place_name",
            "census_place_namelsad",
            "cbsa_title",
            "state",
            "brand_harmonized",
            "name",
            "origin_harmonized_1",
            "osm_cuisines",
        ]:
            if preferred in columns:
                top_value_fields.append(preferred)

        # Also include any other field that looks central to city/place matching.
        for c in groups["city_place_fields"] + groups["cbsa_metro_fields"]:
            if c not in top_value_fields:
                top_value_fields.append(c)

        top_value_fields = top_value_fields[:25]

        top_values_payload = {
            c: top_values(df, c, 25)
            for c in top_value_fields
        }

        cbsa_preview = []
        for c in groups["cbsa_metro_fields"]:
            if "title" in c.lower() or "name" in c.lower() or c.lower() in {"cbsa", "cbsa_title"}:
                cbsa_preview.extend(cbsa_parse_preview(df, c))
        cbsa_preview = cbsa_preview[:200]

        # Chain diagnostics: brand_harmonized first, normalized name fallback later.
        chain_diagnostics = {}
        for brand_field in ["brand_harmonized", "name"]:
            if brand_field in columns:
                brand_counts = (
                    df.where(valid_expr(brand_field))
                    .groupBy(F.col(brand_field).cast("string").alias("brand_or_name"))
                    .agg(
                        F.countDistinct("restaurant_backbone_id").alias("n_restaurants")
                        if "restaurant_backbone_id" in columns
                        else F.count("*").alias("n_restaurants")
                    )
                )
                n_groups = brand_counts.count()
                n_chain_groups = brand_counts.where(F.col("n_restaurants") >= 100).count()
                n_chain_restaurants = (
                    brand_counts.where(F.col("n_restaurants") >= 100)
                    .agg(F.sum("n_restaurants").alias("n"))
                    .first()["n"]
                )
                top_chain_rows = (
                    brand_counts.orderBy(F.desc("n_restaurants"), F.asc("brand_or_name"))
                    .limit(50)
                    .collect()
                )
                chain_diagnostics[brand_field] = {
                    "n_groups": int(n_groups),
                    "n_groups_with_ge_100_restaurants": int(n_chain_groups),
                    "n_restaurants_in_ge_100_groups": int(n_chain_restaurants or 0),
                    "top_groups": [r.asDict() for r in top_chain_rows],
                }

        # Write schema and candidate-field reports.
        report = {
            "run_id": args.run_id,
            "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "backbone_path": args.backbone,
            "row_count": int(row_count),
            "n_columns": len(columns),
            "columns": columns,
            "dtypes": dtypes,
            "classified_columns": groups,
            "required_core_status": required_status,
            "id_diagnostics": {
                "unique_restaurant_backbone_id": int(unique_id_count) if unique_id_count is not None else None,
                "duplicate_restaurant_backbone_id_count": int(duplicate_id_rows) if duplicate_id_rows is not None else None,
                "max_rows_per_restaurant_backbone_id": int(max_rows_per_id) if max_rows_per_id is not None else None,
            },
            "field_metrics": metrics,
            "top_values": top_values_payload,
            "cbsa_parse_preview": cbsa_preview,
            "chain_diagnostics": chain_diagnostics,
            "recommended_next_step": (
                "Use this report to decide which backbone geography fields enter expanded "
                "city/place blocking: city_nonstandardized if present, census_place_name, "
                "and parsed CBSA title components if available."
            ),
        }

        write_json(outdir / "updated_backbone_preflight.json", report)

        # Write a compact markdown/data dictionary for quick reading.
        with open(outdir / "updated_backbone_data_dictionary.md", "w", encoding="utf-8") as f:
            f.write(f"# Updated restaurant backbone preflight\n\n")
            f.write(f"- Run ID: `{args.run_id}`\n")
            f.write(f"- Backbone: `{args.backbone}`\n")
            f.write(f"- Rows: {row_count:,}\n")
            f.write(f"- Columns: {len(columns):,}\n\n")

            f.write("## Classified fields\n\n")
            for group, vals in groups.items():
                f.write(f"### {group}\n")
                if vals:
                    for v in vals:
                        f.write(f"- `{v}`\n")
                else:
                    f.write("- none detected\n")
                f.write("\n")

            f.write("## Field metrics\n\n")
            f.write("| field | type | valid | missing | missing_rate | approx_distinct |\n")
            f.write("|---|---:|---:|---:|---:|---:|\n")
            for m in metrics:
                f.write(
                    f"| `{m.get('field')}` | {m.get('data_type')} | "
                    f"{m.get('valid_count')} | {m.get('missing_count')} | "
                    f"{m.get('missing_rate')} | {m.get('approx_distinct_nonmissing')} |\n"
                )

        # A small row sample with likely relevant fields only.
        sample_cols = []
        for c in [
            "restaurant_backbone_id",
            "name",
            "state",
            "city_nonstandardized",
            "census_place_name",
            "census_place_namelsad",
            "cbsa_title",
            "cbsa_code",
            "brand_harmonized",
            "address",
            "latitude",
            "longitude",
            "origin_harmonized_1",
            "origin_harmonized_iso1",
            "osm_cuisines",
        ]:
            if c in columns:
                sample_cols.append(c)

        if sample_cols:
            (
                df.select(*sample_cols)
                .orderBy(F.rand(722))
                .limit(1000)
                .coalesce(1)
                .write.mode("errorifexists")
                .option("header", True)
                .csv(str(outdir / "sample_rows_csv"))
            )

        (outdir / "_UPDATED_BACKBONE_PREFLIGHT_COMPLETE").write_text("complete\n", encoding="utf-8")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()
