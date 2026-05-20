#!/usr/bin/env python3
# build_inventor_patent_public_panel.py
#
# Creates an INTERMEDIATE dataset at (user_id, patent_id) level (no inventor-year aggregation),
# enriched with:
#   - patent metadata (from revelio_patents_inventor_matches)
#   - position / firm-year attributes (from processed/inventor_position_education, expanded + forward-filled)
#   - education (first/last) + first position start date
#   - firm identifiers incl. ultimate_parent_rcid and GVKEY (from academic_company_ref)
#
# Filters to PUBLIC firms only: keeps observations where gvkey is non-missing.
#
# Usage (single shard):
#   python -u build_inventor_patent_public_panel.py 0
#
# Usage (sbatch array):
#   python -u build_inventor_patent_public_panel.py ${SLURM_ARRAY_TASK_ID}

import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, first

# ===================== CONFIG =====================
SHARD_INDEX = int(sys.argv[1])

INPUT_POS = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
INPUT_PAT = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
COMPANY_REF_PATH = "/labs/khanna/linkedin_202507/academic_company_ref"

OUT_DIR = "/labs/khanna/linkedin_202507/processed/inventor_patent_public_shards"
os.makedirs(OUT_DIR, exist_ok=True)

# Spark local dirs away from home quota
SPARK_LOCAL = "/labs/khanna/linkedin_202507/spark_local"
SPARK_WAREHOUSE = "/labs/khanna/linkedin_202507/spark_warehouse"
SPARK_TMP = "/labs/khanna/linkedin_202507/spark_tmp"

CHECKPOINT_DIR = "/labs/khanna/linkedin_202507/scratch/spark_checkpoints"
os.makedirs(CHECKPOINT_DIR, exist_ok=True)
# ================================================

spark = (
    SparkSession.builder
    .appName(f"build_inventor_patent_public_panel_shard_{SHARD_INDEX}")
    .config("spark.driver.memory", "200g")
    .config("spark.executor.memory", "200g")
    .config("spark.local.dir", SPARK_LOCAL)
    .config("spark.sql.warehouse.dir", SPARK_WAREHOUSE)
    .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={SPARK_TMP}")
    .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={SPARK_TMP}")
    .config("spark.sql.shuffle.partitions", "800")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("INFO")
spark.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

# Rebase safety (like your original)
spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

print(f"[INFO] Starting shard {SHARD_INDEX}")

def check_dupes(df, keys, label, n=5):
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")
    total = df.count()
    print(f"[INFO] Total rows: {total:,}")
    if keys:
        dupes = df.groupBy(*keys).count().filter(F.col("count") > 1)
        n_dupes = dupes.count()
        print(f"[CHECK] {n_dupes:,} duplicate {tuple(keys)} pairs.")
        if n_dupes > 0:
            print("[SAMPLE] Top duplicates:")
            dupes.show(n, truncate=False)

# -----------------------------
# 1) Load POS shard
# -----------------------------
print("[INFO] Reading position/education shard...")
pos = (
    spark.read.parquet(INPUT_POS)
    .filter(col("shard") == SHARD_INDEX)
)

# Ensure rcid types consistent
if "rcid" in pos.columns:
    pos = pos.withColumn("rcid", col("rcid").cast("long"))

# -----------------------------
# 2) Build rcid -> (ultimate_parent_rcid, gvkey) mapping from academic_company_ref (DEDUPED)
# -----------------------------
print("[INFO] Loading and deduplicating academic_company_ref (rcid -> parent, gvkey)...")
company_ref_raw = (
    spark.read.parquet(COMPANY_REF_PATH)
         .select("rcid", "ultimate_parent_rcid", "gvkey")
         .withColumn("rcid", col("rcid").cast("long"))
         .withColumn("ultimate_parent_rcid", col("ultimate_parent_rcid").cast("long"))
         .withColumn("gvkey", col("gvkey").cast("long"))
)

# Unique per rcid to avoid exploding join
company_ref = (
    company_ref_raw
    .groupBy("rcid")
    .agg(
        F.first("ultimate_parent_rcid", ignorenulls=True).alias("ultimate_parent_rcid_ref"),
        F.first("gvkey", ignorenulls=True).alias("gvkey_rcid")
    )
)

# parent_rcid -> gvkey mapping (also unique)
parent_gvkey = (
    company_ref
    .select(
        F.col("rcid").alias("ultimate_parent_rcid"),
        F.col("gvkey_rcid").alias("gvkey_parent")
    )
)

# -----------------------------
# 3) Add parent rcid + gvkey onto pos, then FILTER to public firms early
# -----------------------------
print("[INFO] Adding ultimate_parent_rcid + gvkey to positions...")
pos = (
    pos.join(company_ref, on="rcid", how="left")
       .withColumn(
           "ultimate_parent_rcid",
           F.coalesce(
               col("ultimate_parent_rcid"),      # if already present in pos
               col("ultimate_parent_rcid_ref"),  # from ref
               col("rcid")                       # fallback
           )
       )
       .drop("ultimate_parent_rcid_ref")
)

pos = (
    pos.join(parent_gvkey, on="ultimate_parent_rcid", how="left")
       .withColumn("gvkey", F.coalesce(col("gvkey_parent"), col("gvkey_rcid")))
       .drop("gvkey_parent")
)

# Treat gvkey==0 as missing
pos = pos.withColumn("gvkey", F.when((col("gvkey").isNull()) | (col("gvkey") == 0), F.lit(None)).otherwise(col("gvkey")))

# Early public filter (this is what keeps the intermediate dataset manageable)
pos = pos.filter(col("gvkey").isNotNull())

print("[INFO] After public filter on positions:")
print("       distinct users:", pos.select("user_id").distinct().count())
print("       rows:", pos.count())

# -----------------------------
# 4) Deduplicate position spells and expand across years
# -----------------------------
print("[INFO] Expanding position spells across years (public only)...")

# Parse start/end years
pos = pos.withColumn("start_year", year("startdate").cast("int")) \
         .withColumn("end_year", year("enddate").cast("int"))
pos = pos.withColumn("end_year", F.when(col("end_year").isNull(), col("start_year")).otherwise(col("end_year")))

# Dedup identical spells to prevent repeated expansion
spell_keys = [k for k in ["user_id", "position_id", "startdate", "enddate"] if k in pos.columns]
if spell_keys:
    pos = pos.dropDuplicates(spell_keys)

pos_expanded = pos.withColumn("year", F.explode(F.sequence(col("start_year"), col("end_year"))))

check_dupes(pos_expanded, ["user_id", "year", "position_id"], "POS expanded (public)")

# -----------------------------
# 5) Aggregate to user-year (public)
# -----------------------------
print("[INFO] Aggregating position data to user-year (public)...")

def first_safe(c): return F.first(c, ignorenulls=True).alias(c)
def last_safe(c):  return F.last(c, ignorenulls=True).alias(c)

pos_year = (
    pos_expanded
    .groupBy("user_id", "year")
    .agg(
        F.countDistinct("position_id").alias("n_positions"),
        F.countDistinct("rcid").alias("n_unique_companies"),
        F.first("position_id", ignorenulls=True).alias("first_position_id"),

        F.mean("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
        F.mean("total_compensation").alias("avg_total_comp"),
        F.min("total_compensation").alias("min_total_comp"),
        F.max("total_compensation").alias("max_total_comp"),
        F.mean("seniority").alias("avg_seniority"),
        F.min("seniority").alias("min_seniority"),
        F.max("seniority").alias("max_seniority"),

        # location
        F.first("city", ignorenulls=True).alias("first_city"),
        F.last("city", ignorenulls=True).alias("last_city"),
        F.first("state", ignorenulls=True).alias("first_state"),
        F.last("state", ignorenulls=True).alias("last_state"),
        F.first("country", ignorenulls=True).alias("first_country"),
        F.last("country", ignorenulls=True).alias("last_country"),
        F.first("region", ignorenulls=True).alias("first_region"),
        F.last("region", ignorenulls=True).alias("last_region"),
        F.first("metro_area", ignorenulls=True).alias("first_metro_area"),
        F.last("metro_area", ignorenulls=True).alias("last_metro_area"),
        F.first("location_raw", ignorenulls=True).alias("first_location_raw"),
        F.last("location_raw", ignorenulls=True).alias("last_location_raw"),

        # firm identifiers
        F.first("rcid", ignorenulls=True).alias("first_rcid"),
        F.last("rcid", ignorenulls=True).alias("last_rcid"),
        F.first("ultimate_parent_rcid", ignorenulls=True).alias("first_parent_rcid"),
        F.last("ultimate_parent_rcid", ignorenulls=True).alias("last_parent_rcid"),
        F.first("gvkey", ignorenulls=True).alias("first_gvkey"),
        F.last("gvkey", ignorenulls=True).alias("last_gvkey"),
    )
)

check_dupes(pos_year, ["user_id", "year"], "POS yearly aggregated (public)")

# -----------------------------
# 6) Education summaries + first position date (from PUBLIC-filtered pos)
# -----------------------------
print("[INFO] Extracting education (first/last overall) from pos (public only)...")

# Only if education columns exist
edu_cols_needed = ["startdate_edu", "enddate_edu", "university_name", "degree", "field", "university_country"]
has_edu = all(c in pos.columns for c in edu_cols_needed)

if has_edu:
    edu_first = (
        pos.filter(col("startdate_edu").isNotNull())
           .orderBy("startdate_edu")
           .groupBy("user_id")
           .agg(
               first("university_name", ignorenulls=True).alias("first_university"),
               first("degree", ignorenulls=True).alias("first_degree"),
               first("field", ignorenulls=True).alias("first_field"),
               first("university_country", ignorenulls=True).alias("first_university_country"),
               first("startdate_edu", ignorenulls=True).alias("first_startdate_edu"),
               first("enddate_edu", ignorenulls=True).alias("first_enddate_edu"),
           )
    )

    edu_last = (
        pos.filter(col("enddate_edu").isNotNull())
           .orderBy(F.desc("enddate_edu"))
           .groupBy("user_id")
           .agg(
               first("university_name", ignorenulls=True).alias("last_university"),
               first("degree", ignorenulls=True).alias("last_degree"),
               first("field", ignorenulls=True).alias("last_field"),
               first("university_country", ignorenulls=True).alias("last_university_country"),
               first("startdate_edu", ignorenulls=True).alias("last_startdate_edu"),
               first("enddate_edu", ignorenulls=True).alias("last_enddate_edu"),
           )
    )

    edu = edu_first.join(edu_last, on="user_id", how="outer")
else:
    edu = spark.createDataFrame([], "user_id string")  # empty placeholder

print("[INFO] Extracting first position start date per user (public only)...")
first_pos = (
    pos.filter(col("startdate").isNotNull())
       .groupBy("user_id")
       .agg(F.min("startdate").alias("first_startdate_pos"))
)

# -----------------------------
# 7) Load PATENTS, filter to users in public positions, keep patent-level rows (NO aggregation)
# -----------------------------
print("[INFO] Reading patent matches...")
pat = (
    spark.read
         .option("datetimeRebaseMode", "LEGACY")
         .option("int96RebaseMode", "LEGACY")
         .parquet(INPUT_PAT)
)

# clean filing_date
pat = pat.withColumn(
    "filing_date",
    F.when(
        (col("filing_date") < F.to_date(F.lit("1950-01-01"))) |
        (col("filing_date") > F.to_date(F.lit("2025-12-31"))),
        None
    ).otherwise(col("filing_date"))
)

pat = pat.withColumn("year", year("filing_date")).filter(col("year").isNotNull())

# Restrict patents to users present in THIS shard’s public-position data (reduces size)
public_users = pos.select("user_id").distinct()
pat = pat.join(public_users, on="user_id", how="inner")

# Keep patent-level uniqueness
if "patent_id" in pat.columns:
    pat = pat.dropDuplicates(["user_id", "patent_id"])
else:
    pat = pat.dropDuplicates(["user_id", "application_id", "year"])

check_dupes(pat, ["user_id", "patent_id"] if "patent_id" in pat.columns else ["user_id", "year"], "PAT panel filtered to public users")

# -----------------------------
# 8) Forward-fill pos_year only over years we need (up to max patent year per user)
#     (avoids huge min->global-max expansion)
# -----------------------------
print("[INFO] Building forward-filled position-year panel only through each user's max patent year...")

pos_bounds = (
    pos_year.groupBy("user_id")
            .agg(F.min("year").alias("min_pos_year"))
            .filter(col("min_pos_year").isNotNull())
)

pat_bounds = (
    pat.groupBy("user_id")
       .agg(F.max("year").alias("max_pat_year"))
       .filter(col("max_pat_year").isNotNull())
)

bounds = (
    pos_bounds.join(pat_bounds, on="user_id", how="inner")
              .filter(col("max_pat_year") >= col("min_pos_year"))
)

year_seq = (
    bounds.withColumn("year", F.explode(F.sequence(col("min_pos_year"), col("max_pat_year"))))
          .select("user_id", "year")
)

pos_year_full = year_seq.join(pos_year, on=["user_id", "year"], how="left")

# forward-fill key firm/location fields
w = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
cols_to_fill = [
    "first_rcid", "last_rcid", "first_parent_rcid", "last_parent_rcid",
    "first_gvkey", "last_gvkey",
    "first_city", "last_city", "first_state", "last_state",
    "first_country", "last_country", "first_region", "last_region",
    "first_metro_area", "last_metro_area", "first_location_raw", "last_location_raw",
    "first_position_id",
    "n_positions", "n_unique_companies",
    "avg_salary","min_salary","max_salary",
    "avg_total_comp","min_total_comp","max_total_comp",
    "avg_seniority","min_seniority","max_seniority"
]
for c in cols_to_fill:
    if c in pos_year_full.columns:
        pos_year_full = pos_year_full.withColumn(c, F.last(c, ignorenulls=True).over(w))

# Create a single gvkey column to use in filters/merges
pos_year_full = pos_year_full.withColumn("gvkey", F.coalesce(col("first_gvkey"), col("last_gvkey")))
pos_year_full = pos_year_full.withColumn("gvkey", F.when((col("gvkey").isNull()) | (col("gvkey") == 0), F.lit(None)).otherwise(col("gvkey")))

# -----------------------------
# 9) Build INTERMEDIATE dataset: patent-level + firm-year + edu + first_pos
# -----------------------------
print("[INFO] Merging patent-level panel with position-year + education + first_pos...")

intermediate = (
    pat.join(pos_year_full, on=["user_id", "year"], how="left")
       .join(edu, on="user_id", how="left")
       .join(first_pos, on="user_id", how="left")
)

# Final public filter (some patent-years may still have null gvkey after join)
intermediate = intermediate.filter(col("gvkey").isNotNull())

# Optional: reduce width a bit by dropping helper columns
drop_cols = [c for c in ["min_pos_year", "max_pat_year"] if c in intermediate.columns]
if drop_cols:
    intermediate = intermediate.drop(*drop_cols)

print("[INFO] Materializing intermediate (checkpoint)...")
intermediate = intermediate.checkpoint(eager=True)

print("[INFO] Final counts (this shard):")
print("       rows:", intermediate.count())
print("       distinct users:", intermediate.select("user_id").distinct().count())
if "patent_id" in intermediate.columns:
    print("       distinct patents:", intermediate.select("patent_id").distinct().count())
print("       distinct gvkey:", intermediate.select("gvkey").distinct().count())

# -----------------------------
# 10) Write shard output
# -----------------------------
out_path = f"{OUT_DIR}/inventor_patent_public_shard={SHARD_INDEX}.parquet"
print(f"[INFO] Writing shard to {out_path}")

# Reduce small files a bit (tune if you want)
intermediate = intermediate.coalesce(16)

intermediate.write.mode("overwrite").option("compression", "snappy").parquet(out_path)

print(f"[INFO] Finished shard {SHARD_INDEX}")
spark.stop()
