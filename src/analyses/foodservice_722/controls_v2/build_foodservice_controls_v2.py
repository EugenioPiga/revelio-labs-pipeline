#!/usr/bin/env python3
"""Add conservative origin and LinkedIn demographic controls to final datasets.

The existing matched datasets are read-only inputs. This stage writes a new,
versioned output tree with identical row counts and matching assignments.

Preferred origin rule
---------------------
* Non-U.S.: origin-v4 provides a non-U.S. country.
* U.S.: education and employment histories are both present, every record in
  both histories has a valid country, and every country is the United States.
* Unassigned: workers with insufficient or incomplete country histories, plus
  the rare case of a non-U.S. signal that cannot be resolved to a country.

Robustness rule
---------------
Workers with no observed non-U.S. signal are assigned to the United States,
including workers with limited country coverage.

Gender, ethnicity, and race fields are copied from the Revelio user file and
remain explicitly labeled as inferred, not self-reported. Career timing
variables are proxies and should not be described as exact age.
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import os
import re
from pathlib import Path
from typing import Iterable, Optional

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

US_KEYS = {
    "united states", "united states of america", "usa", "u s a",
    "us", "u s", "america",
}
USER_IDS = ["user_id", "user_id_key", "individual_user_id", "individual_id", "id"]
COUNTRIES = [
    "country", "country_name", "position_country", "job_country",
    "location_country", "university_country", "school_country",
    "institution_country", "education_country",
]
START_DATES = ["startdate", "start_date", "start_year", "from_date", "date_start"]
END_DATES = [
    "enddate", "end_date", "end_year", "graduation_date",
    "graduation_year", "to_date", "date_end",
]
DEGREES = ["degree", "degree_name", "degree_raw", "education_degree", "degree_type", "qualification"]
ORIGIN_COUNTRIES = ["country_of_origin", "origin_country"]
ORIGIN_KEYS = ["country_of_origin_key", "origin_country_key"]
ORIGIN_SOURCES = ["country_of_origin_source", "origin_source"]


def args() -> argparse.Namespace:
    p = argparse.ArgumentParser()
    p.add_argument("--final-root", required=True)
    p.add_argument("--origin-v4", required=True)
    p.add_argument("--full-histories", required=True)
    p.add_argument("--education", required=True)
    p.add_argument("--users", required=True)
    p.add_argument("--output-root", required=True)
    p.add_argument("--output-partitions", type=int, default=512)
    p.add_argument("--threads", type=int, default=8)
    p.add_argument("--tmpdir", required=True)
    return p.parse_args()


def pick(df: DataFrame, candidates: Iterable[str], label: str, required: bool = True) -> Optional[str]:
    lookup = {c.lower(): c for c in df.columns}
    for candidate in candidates:
        if candidate.lower() in lookup:
            return lookup[candidate.lower()]
    if required:
        raise RuntimeError(f"Could not identify {label}. Candidates={list(candidates)}; columns={df.columns}")
    return None


def valid(c: F.Column) -> F.Column:
    x = F.trim(c.cast("string"))
    return x.isNotNull() & (x != "") & (~F.lower(x).isin("null", "none", "nan"))


def key(c: F.Column) -> F.Column:
    x = F.lower(F.trim(c.cast("string")))
    x = F.regexp_replace(x, r"&", " and ")
    x = F.regexp_replace(x, r"[^a-z0-9]+", " ")
    return F.trim(F.regexp_replace(x, r"\s+", " "))


def year(c: F.Column) -> F.Column:
    x = F.trim(c.cast("string"))
    parsed = F.coalesce(
        F.to_date(x), F.to_date(x, "yyyy-MM-dd"), F.to_date(x, "yyyy/MM/dd"),
        F.to_date(x, "MM/dd/yyyy"), F.to_date(x, "yyyy"),
    )
    regex_year = F.regexp_extract(x, r"((?:19|20)[0-9]{2})", 1).cast("int")
    y = F.coalesce(F.year(parsed), regex_year)
    return F.when(y.between(1900, 2035), y)


def add_country_flags(df: DataFrame, raw: str, prefix: str) -> DataFrame:
    raw_col = F.col(raw).cast("string")
    k = key(raw_col)
    ok = valid(raw_col)
    return (
        df.withColumn(f"_{prefix}_country_valid", ok.cast("int"))
        .withColumn(f"_{prefix}_country_us", (ok & k.isin(*sorted(US_KEYS))).cast("int"))
        .withColumn(f"_{prefix}_country_non_us", (ok & (~k.isin(*sorted(US_KEYS)))).cast("int"))
    )


def degree_rank(c: F.Column) -> F.Column:
    x = key(c)
    return (
        F.when(x.rlike(r"\b(phd|ph d|doctor of philosophy|doctorate|doctoral|dphil|scd|dsc|edd)\b"), 6)
        .when(x.rlike(r"\b(md|m d|doctor of medicine|jd|j d|juris doctor|dds|dmd|pharmd|professional degree)\b"), 5)
        .when(x.rlike(r"\b(mba|m b a|master|masters|msc|m sc|ma|m a|meng|m eng|mph|mpa|mpp)\b"), 4)
        .when(x.rlike(r"\b(bachelor|bachelors|ba|b a|bs|b s|bsc|b sc|bba|beng|b eng)\b"), 3)
        .when(x.rlike(r"\b(associate|associates|aa|a a|as|a s)\b"), 2)
        .when(x.rlike(r"\b(high school|secondary school|ged|diploma)\b"), 1)
        .otherwise(0)
    )


def degree_label(c: F.Column) -> F.Column:
    return (
        F.when(c == 6, "doctorate").when(c == 5, "professional")
        .when(c == 4, "master").when(c == 3, "bachelor")
        .when(c == 2, "associate").when(c == 1, "secondary")
        .otherwise("unknown_or_unreported")
    )


def education_controls(spark: SparkSession, path: str, sample: DataFrame) -> tuple[DataFrame, dict]:
    raw = spark.read.parquet(path)
    uid = pick(raw, USER_IDS, "education user id")
    country = pick(raw, COUNTRIES, "education country")
    start = pick(raw, START_DATES, "education start", False)
    end = pick(raw, END_DATES, "education end", False)
    degree = pick(raw, DEGREES, "degree", False)

    edu = raw.select(
        F.col(uid).cast("long").alias("user_id"),
        F.col(country).cast("string").alias("_edu_country"),
        (F.col(start).cast("string") if start else F.lit(None).cast("string")).alias("_edu_start"),
        (F.col(end).cast("string") if end else F.lit(None).cast("string")).alias("_edu_end"),
        (F.col(degree).cast("string") if degree else F.lit(None).cast("string")).alias("_degree"),
    ).where(F.col("user_id").isNotNull()).join(sample, "user_id", "inner")

    edu = add_country_flags(edu, "_edu_country", "edu")
    edu = edu.withColumn("_edu_start_year", year(F.col("_edu_start")))
    edu = edu.withColumn("_edu_end_year", year(F.col("_edu_end")))
    edu = edu.withColumn("_degree_rank", degree_rank(F.col("_degree")))

    out = edu.groupBy("user_id").agg(
        F.count(F.lit(1)).cast("long").alias("n_education_records"),
        F.sum("_edu_country_valid").cast("long").alias("n_education_country_observed"),
        F.sum("_edu_country_us").cast("long").alias("n_us_education_records"),
        F.sum("_edu_country_non_us").cast("long").alias("n_non_us_education_records"),
        F.sum(F.when(F.col("_edu_country_valid") == 0, 1).otherwise(0)).cast("long").alias("n_education_country_missing"),
        F.min("_edu_start_year").alias("first_education_start_year"),
        F.max("_edu_end_year").alias("last_education_end_year"),
        F.max("_degree_rank").cast("int").alias("highest_degree_rank"),
    )
    out = out.withColumn("highest_degree_level", degree_label(F.col("highest_degree_rank")))
    out = out.withColumn("has_bachelor_or_more", (F.col("highest_degree_rank") >= 3).cast("int"))
    out = out.withColumn("has_master_or_more", (F.col("highest_degree_rank") >= 4).cast("int"))
    out = out.withColumn("has_doctorate", (F.col("highest_degree_rank") >= 6).cast("int"))
    return out, {"user_id": uid, "country": country, "start": start, "end": end, "degree": degree}


def job_controls(spark: SparkSession, path: str, sample: DataFrame) -> tuple[DataFrame, dict]:
    raw = spark.read.parquet(path)
    uid = pick(raw, USER_IDS, "history user id")
    country = pick(raw, COUNTRIES, "employment country")
    start = pick(raw, START_DATES, "employment start", False)

    jobs = raw.select(
        F.col(uid).cast("long").alias("user_id"),
        F.col(country).cast("string").alias("_job_country"),
        (F.col(start).cast("string") if start else F.lit(None).cast("string")).alias("_job_start"),
    ).where(F.col("user_id").isNotNull()).join(sample, "user_id", "inner")

    jobs = add_country_flags(jobs, "_job_country", "job")
    jobs = jobs.withColumn("_job_start_year", year(F.col("_job_start")))
    out = jobs.groupBy("user_id").agg(
        F.count(F.lit(1)).cast("long").alias("n_job_records"),
        F.sum("_job_country_valid").cast("long").alias("n_job_country_observed"),
        F.sum("_job_country_us").cast("long").alias("n_us_job_records"),
        F.sum("_job_country_non_us").cast("long").alias("n_non_us_job_records"),
        F.sum(F.when(F.col("_job_country_valid") == 0, 1).otherwise(0)).cast("long").alias("n_job_country_missing"),
        F.min("_job_start_year").alias("first_job_start_year"),
    )
    return out, {"user_id": uid, "country": country, "start": start}


def user_demographics(spark: SparkSession, path: str, sample: DataFrame) -> tuple[DataFrame, dict]:
    raw = spark.read.parquet(path)
    uid = pick(raw, USER_IDS, "user-file id")
    demographic = sorted(c for c in raw.columns if c != uid and any(t in c.lower() for t in ("gender", "ethnic", "race")))
    if not demographic:
        raise RuntimeError(f"No gender/ethnicity/race columns found in user file. Columns={raw.columns}")

    aliases = {}
    used = set()
    exprs = [F.col(uid).cast("long").alias("user_id")]
    for original in demographic:
        stem = re.sub(r"[^a-z0-9]+", "_", original.lower()).strip("_")
        alias = f"revelio_user_{stem}"
        n = 2
        while alias in used:
            alias = f"revelio_user_{stem}_{n}"
            n += 1
        used.add(alias)
        aliases[original] = alias
        exprs.append(F.col(original).alias(alias))

    users = raw.select(*exprs).where(F.col("user_id").isNotNull()).join(sample, "user_id", "inner")
    completeness = None
    for alias in aliases.values():
        term = F.when(F.col(alias).isNotNull(), 1).otherwise(0)
        completeness = term if completeness is None else completeness + term
    hash_inputs = [F.coalesce(F.col(a).cast("string"), F.lit("")) for a in aliases.values()]
    w = Window.partitionBy("user_id").orderBy(completeness.desc(), F.sha2(F.concat_ws("||", *hash_inputs), 256).asc())
    users = users.withColumn("_r", F.row_number().over(w)).where(F.col("_r") == 1).drop("_r")
    return users, {"user_id": uid, "demographic_columns": aliases}


def origin_v4(spark: SparkSession, path: str, sample: DataFrame) -> tuple[DataFrame, dict]:
    raw = spark.read.parquet(path)
    uid = pick(raw, USER_IDS, "origin-v4 user id")
    country = pick(raw, ORIGIN_COUNTRIES, "origin-v4 country")
    country_key = pick(raw, ORIGIN_KEYS, "origin-v4 key", False)
    source = pick(raw, ORIGIN_SOURCES, "origin-v4 source", False)
    out = raw.select(
        F.col(uid).cast("long").alias("user_id"),
        F.col(country).cast("string").alias("_origin_country"),
        (F.col(country_key).cast("string") if country_key else key(F.col(country))).alias("_origin_key"),
        (F.col(source).cast("string") if source else F.lit(None).cast("string")).alias("_origin_source"),
    ).where(F.col("user_id").isNotNull()).join(sample, "user_id", "inner")
    if out.groupBy("user_id").count().where(F.col("count") > 1).limit(1).count():
        raise RuntimeError("Origin-v4 is not unique by user_id")
    return out, {"user_id": uid, "country": country, "key": country_key, "source": source}


def fill_counts(df: DataFrame) -> DataFrame:
    prefixes = ("n_education", "n_us_education", "n_non_us_education", "n_job", "n_us_job", "n_non_us_job")
    for c in df.columns:
        if c.startswith(prefixes):
            df = df.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
    return df


def add_origin_classification(df: DataFrame) -> DataFrame:
    """Create preferred and robustness origin classifications.

    The preferred U.S. category is intentionally strict: both education and
    employment histories must be present, every record must have a valid
    country, and no record may contain a non-U.S. country. Workers with partial
    country coverage remain unassigned in the preferred definition.

    The robustness definition follows the broader interpretation discussed by
    the research team and assigns every worker with no observed non-U.S. signal
    to the United States.
    """
    origin_key = F.coalesce(
        key(F.col("_origin_key")),
        key(F.col("_origin_country")),
    )

    foreign_origin_v4 = F.coalesce(
        valid(F.col("_origin_country"))
        & (~origin_key.isin(*sorted(US_KEYS))),
        F.lit(False),
    )

    has_education_history = F.col("n_education_records") > 0
    has_employment_history = F.col("n_job_records") > 0
    has_education_country = F.col("n_education_country_observed") > 0
    has_employment_country = F.col("n_job_country_observed") > 0

    education_country_complete = (
        has_education_history
        & (F.col("n_education_country_missing") == 0)
    )
    employment_country_complete = (
        has_employment_history
        & (F.col("n_job_country_missing") == 0)
    )

    any_non_us_signal = (
        foreign_origin_v4
        | (F.col("n_non_us_education_records") > 0)
        | (F.col("n_non_us_job_records") > 0)
    )
    no_non_us_signal = ~any_non_us_signal

    # This broader flag is useful for diagnosing cases with U.S. evidence in
    # both histories but incomplete country coverage on other records.
    us_observed_both = (
        has_education_country
        & has_employment_country
        & no_non_us_signal
    )

    # Brett's preferred definition: both histories exist, every record is
    # country-coded, and all countries are U.S.
    us_conservative = (
        education_country_complete
        & employment_country_complete
        & no_non_us_signal
    )

    unresolved_non_us = any_non_us_signal & (~foreign_origin_v4)

    return (
        df.withColumn(
            "non_us_origin_observed_flag",
            any_non_us_signal.cast("int"),
        )
        .withColumn(
            "non_us_origin_country_resolved_flag",
            foreign_origin_v4.cast("int"),
        )
        .withColumn(
            "us_origin_observed_education_and_employment_flag",
            us_observed_both.cast("int"),
        )
        .withColumn(
            "us_origin_conservative_flag",
            us_conservative.cast("int"),
        )
        .withColumn(
            "us_origin_complete_country_coverage_flag",
            us_conservative.cast("int"),
        )
        .withColumn(
            "origin_unassigned_conservative_flag",
            ((~foreign_origin_v4) & (~us_conservative)).cast("int"),
        )
        .withColumn(
            "origin_status_conservative",
            F.when(foreign_origin_v4, "non_us_observed")
            .when(
                unresolved_non_us,
                "non_us_signal_country_unresolved",
            )
            .when(
                us_conservative,
                "us_complete_education_and_employment_history",
            )
            .otherwise(
                "unassigned_insufficient_or_incomplete_history",
            ),
        )
        .withColumn(
            "country_of_origin_conservative",
            F.when(foreign_origin_v4, F.col("_origin_country"))
            .when(us_conservative, F.lit("United States"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "country_of_origin_key_conservative",
            F.when(foreign_origin_v4, origin_key)
            .when(us_conservative, F.lit("united states"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "country_of_origin_source_conservative",
            F.when(foreign_origin_v4, F.col("_origin_source"))
            .when(
                us_conservative,
                F.lit(
                    "all_education_and_employment_records_country_coded_us"
                ),
            )
            .when(
                unresolved_non_us,
                F.lit("non_us_signal_country_unresolved"),
            )
            .otherwise(
                F.lit("insufficient_or_incomplete_country_history"),
            ),
        )
        .withColumn(
            "us_origin_robustness_flag",
            no_non_us_signal.cast("int"),
        )
        .withColumn(
            "country_of_origin_robustness",
            F.when(foreign_origin_v4, F.col("_origin_country"))
            .when(no_non_us_signal, F.lit("United States"))
            .otherwise(F.lit(None).cast("string")),
        )
        .withColumn(
            "origin_status_robustness",
            F.when(foreign_origin_v4, "non_us_observed")
            .when(
                unresolved_non_us,
                "non_us_signal_country_unresolved",
            )
            .when(us_conservative, "us_conservative")
            .when(us_observed_both, "us_partial_country_coverage")
            .otherwise("us_assigned_no_non_us_signal"),
        )
        .withColumn(
            "origin_has_education_history_flag",
            has_education_history.cast("int"),
        )
        .withColumn(
            "origin_has_employment_history_flag",
            has_employment_history.cast("int"),
        )
        .withColumn(
            "origin_has_education_country_flag",
            has_education_country.cast("int"),
        )
        .withColumn(
            "origin_has_employment_country_flag",
            has_employment_country.cast("int"),
        )
        .withColumn(
            "origin_education_country_complete_flag",
            education_country_complete.cast("int"),
        )
        .withColumn(
            "origin_employment_country_complete_flag",
            employment_country_complete.cast("int"),
        )
        .withColumn(
            "origin_no_non_us_signal_flag",
            no_non_us_signal.cast("int"),
        )
    )


def enrich(path_in: str, path_out: str, controls: DataFrame, partitions: int) -> dict:
    source = controls.sparkSession.read.parquet(path_in)
    uid = pick(source, USER_IDS, f"user id in {path_in}")
    before = source.count()
    extra = controls.select(F.col("user_id").alias("_join_user_id"), *[F.col(c) for c in controls.columns if c != "user_id"])
    result = source.withColumn("_join_user_id", F.col(uid).cast("long")).join(extra, "_join_user_id", "left").drop("_join_user_id")
    after = result.count()
    if before != after:
        raise RuntimeError(f"Row count changed: {path_in}: {before} -> {after}")
    if result.where(F.col("origin_status_conservative").isNull()).limit(1).count():
        raise RuntimeError(f"Unmatched user controls in {path_in}")
    result.repartition(partitions).write.mode("errorifexists").parquet(path_out)
    return {"input": path_in, "output": path_out, "rows": before, "input_columns": len(source.columns), "output_columns": len(result.columns), "user_id_column": uid}


def main() -> None:
    a = args()
    out = Path(a.output_root)
    if out.exists():
        raise RuntimeError(f"Refusing to overwrite existing output: {out}")
    out.parent.mkdir(parents=True, exist_ok=True)
    Path(a.tmpdir).mkdir(parents=True, exist_ok=True)

    spark = (
        SparkSession.builder.appName("foodservice_722_controls_v2")
        .master(f"local[{a.threads}]")
        .config("spark.local.dir", a.tmpdir)
        .config("spark.sql.shuffle.partitions", "2048")
        .config("spark.default.parallelism", "2048")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    final = Path(a.final_root)
    inputs = {
        "exact_baseline_people": str(final / "exact_baseline" / "people"),
        "exact_baseline_spells": str(final / "exact_baseline" / "spells"),
        "exact_plus_fuzzy_people": str(final / "exact_plus_fuzzy" / "people"),
        "exact_plus_fuzzy_spells": str(final / "exact_plus_fuzzy" / "spells"),
    }
    for label, path in inputs.items():
        if not os.path.exists(os.path.join(path, "_SUCCESS")):
            raise RuntimeError(f"Missing successful input {label}: {path}")

    person_frames = []
    for label in ("exact_baseline_people", "exact_plus_fuzzy_people"):
        df = spark.read.parquet(inputs[label])
        uid = pick(df, USER_IDS, f"user id in {label}")
        person_frames.append(df.select(F.col(uid).cast("long").alias("user_id")))
    sample = person_frames[0].unionByName(person_frames[1]).where(F.col("user_id").isNotNull()).dropDuplicates(["user_id"]).cache()
    n_sample = sample.count()

    edu, edu_schema = education_controls(spark, a.education, sample)
    jobs, job_schema = job_controls(spark, a.full_histories, sample)
    demographics, user_schema = user_demographics(spark, a.users, sample)
    origin, origin_schema = origin_v4(spark, a.origin_v4, sample)

    controls = sample.join(origin, "user_id", "left").join(edu, "user_id", "left").join(jobs, "user_id", "left").join(demographics, "user_id", "left")
    controls = fill_counts(controls)
    controls = controls.withColumn("highest_degree_rank", F.coalesce(F.col("highest_degree_rank"), F.lit(0)).cast("int"))
    controls = controls.withColumn("highest_degree_level", F.coalesce(F.col("highest_degree_level"), F.lit("unknown_or_unreported")))
    for c in ("has_bachelor_or_more", "has_master_or_more", "has_doctorate"):
        controls = controls.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("int"))
    controls = add_origin_classification(controls)
    controls = controls.withColumn("career_entry_year_proxy", F.coalesce(F.col("first_job_start_year"), F.col("first_education_start_year")))
    controls = controls.withColumn("career_entry_year_proxy_source",
        F.when(F.col("first_job_start_year").isNotNull(), "first_observed_job")
         .when(F.col("first_education_start_year").isNotNull(), "first_observed_education")
         .otherwise("unavailable"))
    controls = controls.drop("_origin_country", "_origin_key", "_origin_source").cache()

    n_controls = controls.count()
    n_unique = controls.select("user_id").distinct().count()
    if n_controls != n_sample or n_unique != n_sample:
        raise RuntimeError(f"Controls are not one row per sample user: sample={n_sample}, rows={n_controls}, unique={n_unique}")

    controls.repartition(a.output_partitions).write.mode("errorifexists").parquet(str(out / "person_controls"))
    outputs = {
        "exact_baseline_people": enrich(inputs["exact_baseline_people"], str(out / "exact_baseline" / "people"), controls, a.output_partitions),
        "exact_baseline_spells": enrich(inputs["exact_baseline_spells"], str(out / "exact_baseline" / "spells"), controls, a.output_partitions),
        "exact_plus_fuzzy_people": enrich(inputs["exact_plus_fuzzy_people"], str(out / "exact_plus_fuzzy" / "people"), controls, a.output_partitions),
        "exact_plus_fuzzy_spells": enrich(inputs["exact_plus_fuzzy_spells"], str(out / "exact_plus_fuzzy" / "spells"), controls, a.output_partitions),
    }

    primary_counts = {r["origin_status_conservative"]: int(r["count"]) for r in controls.groupBy("origin_status_conservative").count().collect()}
    robust_counts = {r["origin_status_robustness"]: int(r["count"]) for r in controls.groupBy("origin_status_robustness").count().collect()}
    manifest = {
        "status": "complete",
        "created_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
        "final_root": a.final_root,
        "output_root": a.output_root,
        "source_inputs": {"origin_v4": a.origin_v4, "full_histories": a.full_histories, "education": a.education, "users": a.users},
        "sample_users": n_sample,
        "person_controls_rows": n_controls,
        "primary_origin_rule": "Non-U.S. from origin-v4; U.S. only when education and employment histories are both present, every record has a valid country, and every country is the United States; otherwise unassigned.",
        "robustness_origin_rule": "All workers with no observed non-U.S. signal are assigned to the United States, including workers with incomplete country coverage.",
        "origin_status_counts": primary_counts,
        "origin_status_robustness_counts": robust_counts,
        "schema_resolution": {"education": edu_schema, "employment": job_schema, "user_file": user_schema, "origin_v4": origin_schema},
        "outputs": outputs,
        "notes": [
            "Original final matching outputs were read only and not modified.",
            "Gender and ethnicity are Revelio/name-inferred variables, not self-reported demographics.",
            "Career timing variables are proxies and should not be described as exact age.",
        ],
    }
    out.mkdir(parents=True, exist_ok=True)
    with (out / "controls_manifest.json").open("w", encoding="utf-8") as f:
        json.dump(manifest, f, indent=2, sort_keys=True)
    (out / "_CONTROLS_COMPLETE").touch()
    print(json.dumps(manifest, indent=2, sort_keys=True))
    print(f"CONTROLS PIPELINE COMPLETE: {out}")
    spark.stop()


if __name__ == "__main__":
    main()
