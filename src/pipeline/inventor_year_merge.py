import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, countDistinct, first, last

# =============== CONFIG =====================
SHARD_INDEX = int(sys.argv[1])  # shard passed from sbatch array task
INPUT_POS = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
INPUT_PAT = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
OUT_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_shards"
MERGED_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
# ============================================

spark = (
    SparkSession.builder
    .appName(f"inventor_year_merge_shard_{SHARD_INDEX}")
    .config("spark.driver.memory", "16g")
    .config("spark.executor.memory", "16g")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

# Set checkpoint dir for safety
checkpoint_dir = os.path.expanduser("~/scratch/spark_checkpoints")
os.makedirs(checkpoint_dir, exist_ok=True)
spark.sparkContext.setCheckpointDir(checkpoint_dir)

print(f"[INFO] Starting shard {SHARD_INDEX}")

def check_dupes(df, label, n=5):
    print(f"\n[===== {label} =====]")
    print(f"[INFO] Columns ({len(df.columns)}): {df.columns}")

    total = df.count()
    print(f"[INFO] Total rows: {total:,}")

    has_year = "year" in df.columns
    if has_year:
        dupes = (
            df.groupBy("user_id", "year")
              .count()
              .filter(F.col("count") > 1)
        )
        n_dupes = dupes.count()
        print(f"[CHECK] {n_dupes:,} duplicate (user_id, year) pairs.")
        if n_dupes > 0:
            print("[SAMPLE] Top duplicate pairs:")
            dupes.show(n, truncate=False)
    else:
        dupes = (
            df.groupBy("user_id")
              .count()
              .filter(F.col("count") > 1)
        )
        n_dupes = dupes.count()
        print(f"[CHECK] {n_dupes:,} duplicate user_id (no year column).")
        if n_dupes > 0:
            print("[SAMPLE] Top duplicate user_ids:")
            dupes.show(n, truncate=False)


# -----------------------------
# 1. Load shard data
# -----------------------------
print("[INFO] Reading position/education shard...")
pos = (
    spark.read.parquet(INPUT_POS)
    .filter(col("shard") == SHARD_INDEX)
)

print("[INFO] Reading patent matches...")
pat = spark.read.parquet(INPUT_PAT)

# -----------------------------
# 2. Patent side aggregation
# -----------------------------
print("[INFO] Aggregating patent data...")
pat_year = (
    pat.withColumn("year", year("patent_date"))
       .filter(col("year").isNotNull())
       .groupBy("user_id", "year")
       .agg(
           countDistinct("patent_id").alias("n_patents"),
           countDistinct("application_id").alias("n_applications"),
           F.sum(F.when(col("inventor_sequence") == "1", 1).otherwise(0)).alias("n_first_inventor")
       )
)

check_dupes(pat_year, "After PATENT aggregation")

# -----------------------------
# 3. Position side aggregation
# -----------------------------
print("[INFO] Expanding position spells across years...")
pos = pos.withColumn("start_year", year("startdate").cast("int")) \
         .withColumn("end_year", year("enddate").cast("int"))
pos = pos.withColumn("end_year", F.when(col("end_year").isNull(), col("start_year")).otherwise(col("end_year")))

# --- Deduplicate raw position spells to avoid multiple expansions ---
print("[INFO] Deduplicating identical position spells...")
pos = pos.dropDuplicates(["user_id", "position_id", "startdate", "enddate"])

check_dupes(pos, "Raw POS before expansion (may not have year)")

pos_expanded = pos.withColumn("year", F.explode(F.sequence(col("start_year"), col("end_year"))))

print("[INFO] Aggregating position data with year-level first_rcid propagation...")

# Make sure start_year is numeric and non-null
pos_expanded = pos_expanded.withColumn("start_year", F.col("start_year").cast("int"))

check_dupes(pos_expanded, "After POS expansion across years")

# --- (A) Aggregate per user-year ---
pos_year = (
    pos_expanded
    .groupBy("user_id", "year")
    .agg(
        countDistinct("position_id").alias("n_positions"),
        countDistinct("rcid").alias("n_unique_companies"),
        F.mean("salary").alias("avg_salary"),
        F.min("salary").alias("min_salary"),
        F.max("salary").alias("max_salary"),
        F.mean("total_compensation").alias("avg_total_comp"),
        F.min("total_compensation").alias("min_total_comp"),
        F.max("total_compensation").alias("max_total_comp"),
        F.mean("seniority").alias("avg_seniority"),
        F.min("seniority").alias("min_seniority"),
        F.max("seniority").alias("max_seniority"),

        # --- location info ---
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

        # --- NEW: choose earliest firm that starts in that year ---
        F.first("rcid", ignorenulls=True).alias("first_rcid"),
        F.last("rcid", ignorenulls=True).alias("last_rcid")
    )
)

# --- (B) Forward-fill to make continuous inventor-year panel ---
print("[INFO] Expanding to continuous inventor-year panel and forward-filling attributes...")

# 1️⃣ Deduplicate first to ensure unique (user_id, year)
print("[DEBUG] Deduplicating per (user_id, year) before forward fill...")
pos_year = (
    pos_year
    .groupBy("user_id", "year")
    .agg(*[
        F.first(c, ignorenulls=True).alias(c)
        for c in pos_year.columns if c not in ["user_id", "year"]
    ])
)
check_dupes(pos_year, "After POS yearly aggregation")


# 2️⃣ Compute the global maximum year across all data (patents + positions)
pat_with_year = pat.withColumn("year", F.year("patent_date"))
max_pat_year = pat_with_year.select(F.max("year")).collect()[0][0]
max_pos_year = pos_year.select(F.max("year")).collect()[0][0]
global_max_year = int(max(max_pat_year, max_pos_year))
print(f"[INFO] Global maximum year detected: {global_max_year}")

# 3️⃣ Compute min and max years per user (extend all to global max)
year_bounds = pos_year.groupBy("user_id").agg(
    F.min("year").alias("min_year")
).withColumn("max_year", F.lit(global_max_year))

# 4️⃣ Generate full sequence for each inventor
year_seq = (
    year_bounds
    .withColumn("year", F.explode(F.sequence(F.col("min_year"), F.col("max_year"))))
    .drop("min_year", "max_year")
)

# 5️⃣ Left-join position-year data to get continuous series
pos_year_full = year_seq.join(pos_year, on=["user_id", "year"], how="left")

# 6️⃣ Remove any residual duplicates introduced by joins
pos_year_full = (
    pos_year_full
    .groupBy("user_id", "year")
    .agg(*[
        F.first(c, ignorenulls=True).alias(c)
        for c in pos_year_full.columns if c not in ["user_id", "year"]
    ])
)

# 7️⃣ Forward-fill key firm and location fields using window
w = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
cols_to_fill = [
    "first_rcid", "first_city", "first_state", "first_country",
    "first_region", "first_metro_area", "first_location_raw",
    "last_rcid", "last_city", "last_state", "last_country",
    "last_region", "last_metro_area", "last_location_raw"
]

print("[DEBUG] Performing forward-fill for firm/location columns...")
for c in cols_to_fill:
    if c in pos_year_full.columns:
        pos_year_full = pos_year_full.withColumn(c, F.last(c, ignorenulls=True).over(w))

# 8️⃣ Sanity check: confirm final year coverage
max_year_check = pos_year_full.select(F.max("year")).collect()[0][0]
print(f"[INFO] Forward-fill complete. Max year in final panel: {max_year_check}")

pos_year = pos_year_full

check_dupes(pos_year_full, "After POS forward-fill expansion")

# -----------------------------
# 4. Education attributes (invariant)
# -----------------------------
print("[INFO] Extracting education (first/last overall)...")

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
           first("enddate_edu", ignorenulls=True).alias("first_enddate_edu")
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
           first("enddate_edu", ignorenulls=True).alias("last_enddate_edu")
       )
)

edu = edu_first.join(edu_last, on="user_id", how="outer")

check_dupes(edu, "First and Last Education")

print("[INFO] Extracting first position start date per user...")
first_pos = (
    pos.filter(col("startdate").isNotNull())
       .groupBy("user_id")
       .agg(F.min("startdate").alias("first_startdate_pos"))
)

check_dupes(first_pos, "First Position")

# -----------------------------
# 5. Merge and prepare for write
# -----------------------------
print("[INFO] Merging patents and positions...")

# Fill patent nulls with 0 BEFORE merging
pat_year = pat_year.fillna({
    "n_patents": 0,
    "n_applications": 0,
    "n_first_inventor": 0
})

inventor_year = (
    pat_year.join(pos_year, on=["user_id", "year"], how="outer")
            .join(edu, on="user_id", how="left")
            .join(first_pos, on="user_id", how="left")
)

# Ensure patent counts remain 0 (not null) after join
inventor_year = inventor_year.fillna({
    "n_patents": 0,
    "n_applications": 0,
    "n_first_inventor": 0
})


# Force materialization (avoid recomputation during write)
inventor_year = inventor_year.checkpoint(eager=True)

# Reduce shuffle pressure before write
inventor_year = inventor_year.coalesce(8)
print(f"[DEBUG] Shard {SHARD_INDEX} partitions before write:",
      inventor_year.rdd.getNumPartitions())

check_dupes(inventor_year, "After FINAL merge (pat+pos+edu)")

# -----------------------------
# 6. Write shard output
# -----------------------------
out_path = f"{OUT_DIR}/inventor_year_shard={SHARD_INDEX}.parquet"
print(f"[INFO] Writing shard to {out_path}")
inventor_year.write.mode("overwrite").parquet(out_path)

print(f"[INFO] Finished shard {SHARD_INDEX}")
spark.stop()
