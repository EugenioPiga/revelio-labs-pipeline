import os
import sys
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import col, year, countDistinct, first, last

# =============== CONFIG =====================
SHARD_INDEX = int(sys.argv[1])  # shard passed from sbatch array task
INPUT_POS = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
INPUT_PAT = "/labs/khanna/linkedin_202507/revelio_patents_inventor_matches"
USPTO_DIR   = "/labs/khanna/USPTO_202507"
US_CIT_PATH = f"{USPTO_DIR}/g_us_patent_citation.tsv"
CPC_PATH    = f"{USPTO_DIR}/g_cpc_current.tsv"
OUT_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_shards"
MERGED_DIR = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
# ============================================

spark = (
    SparkSession.builder
    .appName(f"inventor_year_merge_shard_{SHARD_INDEX}")
    .config("spark.driver.memory", "200g")
    .config("spark.executor.memory", "200g")

    # LOCAL DIRS AWAY FROM HOME QUOTA
    .config("spark.local.dir", "/labs/khanna/linkedin_202507/spark_local")
    .config("spark.sql.warehouse.dir", "/labs/khanna/linkedin_202507/spark_warehouse")
    .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=/labs/khanna/linkedin_202507/spark_tmp")
    .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=/labs/khanna/linkedin_202507/spark_tmp")

    .getOrCreate()
)

spark.sparkContext.setLogLevel("INFO")

spark.conf.set("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
spark.conf.set("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")

print("[DEBUG] datetimeRebaseModeInRead =", spark.conf.get("spark.sql.parquet.datetimeRebaseModeInRead"))
print("[DEBUG] int96RebaseModeInRead =", spark.conf.get("spark.sql.parquet.int96RebaseModeInRead"))

# Set checkpoint dir for safety
checkpoint_dir = "/labs/khanna/linkedin_202507/scratch/spark_checkpoints"
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

company_ref = (
    spark.read.parquet("/labs/khanna/linkedin_202507/academic_company_ref")
         .select("rcid", "ultimate_parent_rcid")
)

pos = (
    pos
    .join(company_ref.withColumnRenamed(
        "ultimate_parent_rcid", "parent_from_ref"
    ), on="rcid", how="left")
    .withColumn(
        "ultimate_parent_rcid",
        F.coalesce(
            col("ultimate_parent_rcid"),  
            col("parent_from_ref"),         
            col("rcid")                     
        )
    )
    .drop("parent_from_ref")
)

print("[INFO] Reading patent matches...")

pat = (
    spark.read
         .option("datetimeRebaseMode", "LEGACY")
         .option("int96RebaseMode", "LEGACY")
         .parquet(INPUT_PAT)
)

pat = pat.withColumn(
    "filing_date",
    F.when(
        (col("filing_date") < F.to_date(F.lit("1950-01-01"))) |
        (col("filing_date") > F.to_date(F.lit("2025-12-31"))),
        None
    ).otherwise(col("filing_date"))
)

print("[INFO] Loading US->US citations (citation-level) and collapsing to cited patent...")

us_cit = (
    spark.read.option("header", True).option("sep", "\t").csv(US_CIT_PATH)
    # cited patent = patent receiving the citation
    .select(F.col("citation_patent_id").alias("patent_id"))
    .groupBy("patent_id")
    .agg(F.count("*").alias("n_us_citations"))
)

print("[INFO] Merging US citations into patent matches...")
pat = (
    pat.join(us_cit, on="patent_id", how="left")
       .fillna({"n_us_citations": 0})
)

print("[INFO] Loading CPC (g_cpc_current) and taking cpc_sequence==0...")

cpc_main = (
    spark.read.option("header", True).option("sep", "\t").csv(CPC_PATH)
    .select("patent_id", "cpc_sequence", "cpc_section", "cpc_class")
    .withColumn("cpc_sequence", F.col("cpc_sequence").cast("int"))
    .filter(F.col("cpc_sequence") == 0)
    .select(
        "patent_id",
        F.col("cpc_section").alias("cpc_section_main"),
        F.col("cpc_class").alias("cpc_class_main")
    )
)

pat = pat.join(cpc_main, on="patent_id", how="left")

# -----------------------------
# 2. Patent side aggregation
# -----------------------------

pat = pat.withColumn("year", F.year("filing_date")).filter(F.col("year").isNotNull())

# -------------------------
# Modal fields per inventor-year
# ties broken randomly
# -------------------------

section_counts = (
    pat.filter(F.col("cpc_section_main").isNotNull())
       .groupBy("user_id", "year", "cpc_section_main")
       .agg(F.countDistinct("patent_id").alias("n_patents_in_section"))
)

w_sec = Window.partitionBy("user_id", "year").orderBy(
    F.col("n_patents_in_section").desc(),
    F.rand(seed=123)
)

modal_section_y = (
    section_counts.withColumn("rn", F.row_number().over(w_sec))
                  .filter(F.col("rn") == 1)
                  .select("user_id", "year", F.col("cpc_section_main").alias("modal_section_y"))
)

class_counts = (
    pat.filter(F.col("cpc_class_main").isNotNull())
       .groupBy("user_id", "year", "cpc_class_main")
       .agg(F.countDistinct("patent_id").alias("n_patents_in_class"))
)

w_cls = Window.partitionBy("user_id", "year").orderBy(
    F.col("n_patents_in_class").desc(),
    F.rand(seed=456)
)

modal_class_y = (
    class_counts.withColumn("rn", F.row_number().over(w_cls))
                .filter(F.col("rn") == 1)
                .select("user_id", "year", F.col("cpc_class_main").alias("modal_class_y"))
)


# -------------------------
# Inventor-year aggregation
# each inventor gets full credit for citations on patents they appear on
# -------------------------
pat_year = (
    pat.groupBy("user_id", "year")
       .agg(
           F.countDistinct("patent_id").alias("n_patents"),
           F.countDistinct("application_id").alias("n_applications"),
           F.sum(F.when(F.col("inventor_sequence") == "1", 1.0).otherwise(0.0)).alias("n_first_inventor"),
           F.sum(F.col("n_us_citations")).alias("n_us_citations")
       )
       .join(modal_section_y, on=["user_id", "year"], how="left")
       .join(modal_class_y,   on=["user_id", "year"], how="left")
)

check_dupes(pat_year, "After PATENT aggregation (US citations + modal fields)")


# -------------------------
# Career modal fields per inventor (time-invariant)
# ties broken randomly
# -------------------------

career_section_counts = (
    pat.filter(F.col("cpc_section_main").isNotNull())
       .groupBy("user_id", "cpc_section_main")
       .agg(F.countDistinct("patent_id").alias("n_patents_section_total"))
)

w_csec = Window.partitionBy("user_id").orderBy(
    F.col("n_patents_section_total").desc(),
    F.rand(seed=789)
)

career_modal_section = (
    career_section_counts.withColumn("rn", F.row_number().over(w_csec))
                         .filter(F.col("rn") == 1)
                         .select("user_id", F.col("cpc_section_main").alias("career_modal_section"))
)

career_class_counts = (
    pat.filter(F.col("cpc_class_main").isNotNull())
       .groupBy("user_id", "cpc_class_main")
       .agg(F.countDistinct("patent_id").alias("n_patents_class_total"))
)

w_ccls = Window.partitionBy("user_id").orderBy(
    F.col("n_patents_class_total").desc(),
    F.rand(seed=987)
)

career_modal_class = (
    career_class_counts.withColumn("rn", F.row_number().over(w_ccls))
                       .filter(F.col("rn") == 1)
                       .select("user_id", F.col("cpc_class_main").alias("career_modal_class"))
)

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

        # --- Choose earliest firm that starts in that year ---
        F.first("rcid", ignorenulls=True).alias("first_rcid"),
        F.last("rcid", ignorenulls=True).alias("last_rcid"),
        F.first("ultimate_parent_rcid", ignorenulls=True).alias("first_parent_rcid"),
        F.last("ultimate_parent_rcid", ignorenulls=True).alias("last_parent_rcid")
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

# Add an "observed” marker to pos_year
pos_year = pos_year.withColumn("pos_obs", F.lit(1))

# 2️⃣ Compute global maximum year across patents + positions
pat_with_year = pat.withColumn("year", F.year("filing_date"))
max_pat_year = pat_with_year.select(F.max("year")).collect()[0][0]
max_pos_year = pos_year.select(F.max("year")).collect()[0][0]

# Handle None values
years = [y for y in [max_pat_year, max_pos_year] if y is not None]

if len(years) == 0:
    print("[WARN] No valid years in this shard. Skipping shard...")
    sys.exit(0)

global_max_year = int(max(years))
print(f"[INFO] Global maximum year detected: {global_max_year}")

# Compute min and max years per user (extend all to global max)
year_bounds = (
    pos_year.groupBy("user_id")
    .agg(F.min("year").alias("min_year"))
    .filter(F.col("min_year").isNotNull())
    .withColumn("max_year", F.lit(global_max_year))
)

# Generate full sequence for each inventor
year_seq = (
    year_bounds
    .withColumn("year", F.explode(F.sequence(F.col("min_year"), F.col("max_year"))))
    .drop("min_year", "max_year")
)

# Left-join position-year data to get continuous series
pos_year_full = year_seq.join(pos_year, on=["user_id", "year"], how="left")

# Add a summy equal to 1 if this (user,year) did NOT exist in raw pos_year and is only in the expanded sequence
pos_year_full = pos_year_full.withColumn(
    "pos_extrapolated",
    F.when(F.col("pos_obs").isNull(), F.lit(1)).otherwise(F.lit(0))
).drop("pos_obs")

# Remove any residual duplicates introduced by joins
pos_year_full = (
    pos_year_full
    .groupBy("user_id", "year")
    .agg(*[
        F.first(c, ignorenulls=True).alias(c)
        for c in pos_year_full.columns if c not in ["user_id", "year"]
    ])
)

# Forward-fill key firm and location fields using window
w = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)
cols_to_fill = [
    "first_rcid", "first_city", "first_parent_rcid", "last_parent_rcid",
    "first_state", "first_country", "first_region", "first_metro_area", 
    "first_location_raw", "last_rcid", "last_city", "last_state", 
    "last_country", "last_region", "last_metro_area", "last_location_raw", "first_position_id"
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
# 4. Education attributes (time invariant)
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

# Fill patent-side counts BEFORE merging
pat_year = pat_year.fillna({
    "n_patents": 0,
    "n_applications": 0,
    "n_first_inventor": 0,
    "n_us_citations": 0
})

inventor_year = (
    pat_year.join(pos_year, on=["user_id", "year"], how="outer")
            .join(edu, on="user_id", how="left")
            .join(first_pos, on="user_id", how="left")
)

inventor_year = (
    inventor_year
    .join(career_modal_section, on="user_id", how="left")
    .join(career_modal_class,   on="user_id", how="left")
)

# -------------------------------------
# Create variables for immigrant status
# -------------------------------------

US = F.lit("United States")

inventor_year = inventor_year.withColumn(
    "immig_deg_first_nonUS",
    F.when(
        F.col("first_university_country").isNotNull() &
        (F.col("first_university_country") != US),
        F.lit(1)
    ).otherwise(F.lit(0))
)

pos_country_y = F.coalesce(F.col("last_country"), F.col("first_country"))

w_all_ordered = (
    Window.partitionBy("user_id")
          .orderBy("year")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

inventor_year = inventor_year.withColumn("pos_country_y", pos_country_y)

inventor_year = inventor_year.withColumn(
    "first_pos_country",
    F.first("pos_country_y", ignorenulls=True).over(w_all_ordered)
)

inventor_year = inventor_year.withColumn(
    "immig_job_first_nonUS",
    F.when(
        F.col("first_pos_country").isNotNull() &
        (F.col("first_pos_country") != US),
        F.lit(1)
    ).otherwise(F.lit(0))
)

inventor_year = inventor_year.withColumn(
    "immig_first_deg_or_job_nonUS",
    F.when(
        (F.col("immig_deg_first_nonUS") == 1) | (F.col("immig_job_first_nonUS") == 1),
        F.lit(1)
    ).otherwise(F.lit(0))
)

inventor_year = inventor_year.drop("pos_country_y", "first_pos_country")

# Other measures

w_cum = (
    Window.partitionBy("user_id")
          .orderBy("year")
          .rowsBetween(Window.unboundedPreceding, 0)
)

w_all = (
    Window.partitionBy("user_id")
          .rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
)

# Measure 1: first_university_country != position_country. 
# Time-varying: permanent from that year onward.
# Time-invariant: immigrant for all years

uni_c = F.col("first_university_country")

# Event: mismatch between university country and position country observed that year
m1_event = F.when(
    uni_c.isNotNull() & (
        (F.col("first_country").isNotNull() & (F.col("first_country") != uni_c))
    ),
    F.lit(1)
).otherwise(F.lit(0))

inventor_year = inventor_year.withColumn("immig1_event", m1_event)

# Time-varying (permanent after first event)
inventor_year = inventor_year.withColumn(
    "immig1_post",  # 1 from first event year onward
    F.max("immig1_event").over(w_cum)
)

# Time-invariant (if ever event at any point, immigrant for all years)
inventor_year = inventor_year.withColumn(
    "immig1_ever",  # 1 for all years if event ever happens
    F.max("immig1_event").over(w_all)
)

# Measure 2: country for year-to-year comparisons (use last_country if available else first_country)
# Time-varying: permanent from that year onward.
# Time-invariant: immigrant for all years

pos_country_y = F.coalesce(F.col("last_country"), F.col("first_country"))

within_year_change = F.when(
    F.col("first_country").isNotNull() & F.col("last_country").isNotNull() &
    (F.col("first_country") != F.col("last_country")),
    F.lit(1)
).otherwise(F.lit(0))

lag_country = F.lag(pos_country_y).over(Window.partitionBy("user_id").orderBy("year"))

across_year_change = F.when(
    pos_country_y.isNotNull() & lag_country.isNotNull() & (pos_country_y != lag_country),
    F.lit(1)
).otherwise(F.lit(0))

m2_event = F.when((within_year_change == 1) | (across_year_change == 1), F.lit(1)).otherwise(F.lit(0))

inventor_year = inventor_year.withColumn("immig2_event", m2_event)

# Time-varying (permanent after first change)
inventor_year = inventor_year.withColumn(
    "immig2_post",
    F.max("immig2_event").over(w_cum)
)

# Time-invariant (if ever change happens, immigrant for all years)
inventor_year = inventor_year.withColumn(
    "immig2_ever",
    F.max("immig2_event").over(w_all)
)

inventor_year = inventor_year.drop("immig1_event", "immig2_event")

# -----------------------------
# Backfill fields (section and class) using "next patent"
# -----------------------------

w_future = Window.partitionBy("user_id").orderBy("year").rowsBetween(0, Window.unboundedFollowing)
w_past   = Window.partitionBy("user_id").orderBy("year").rowsBetween(Window.unboundedPreceding, 0)

for v in ["modal_section_y", "modal_class_y"]:
    inventor_year = inventor_year.withColumn(
        f"{v}_next_patent",
        F.first(v, ignorenulls=True).over(w_future)
    )
    inventor_year = inventor_year.withColumn(
        f"{v}_next_patent",
        F.last(f"{v}_next_patent", ignorenulls=True).over(w_past)
    )
    inventor_year = inventor_year.drop(v).withColumnRenamed(f"{v}_next_patent", v)

# ---------------------------------------
# Ensure counts remain 0 after OUTER join
# ---------------------------------------
inventor_year = inventor_year.fillna({
    "n_patents": 0,
    "n_applications": 0,
    "n_first_inventor": 0,
    "n_us_citations": 0
})

# Clean "pos_extrapolated" and make it 0/1
inventor_year = inventor_year.fillna({"pos_extrapolated": 0})


# Force materialization (avoid recomputation during write)
inventor_year = inventor_year.checkpoint(eager=True)

# Reduce shuffle pressure before write
inventor_year = inventor_year.coalesce(8)
print(f"[DEBUG] Shard {SHARD_INDEX} partitions before write:",
      inventor_year.rdd.getNumPartitions())

check_dupes(inventor_year, "After FINAL merge (pat+pos+edu+field-backfill)")

# -----------------------------
# 6. Write shard output
# -----------------------------
out_path = f"{OUT_DIR}/inventor_year_shard={SHARD_INDEX}.parquet"
print(f"[INFO] Writing shard to {out_path}")
inventor_year.write.mode("overwrite").parquet(out_path)

print(f"[INFO] Finished shard {SHARD_INDEX}")
spark.stop()
