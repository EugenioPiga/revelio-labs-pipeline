#!/usr/bin/env python3
###############################################################################
# check_forwardfill_and_duplicates_merged.py
# Validate final merged inventor-year dataset (no duplicates, proper forward fill)
# and inspect long-history inventors for sanity
# Author: Eugenio
###############################################################################

from pyspark.sql import SparkSession, functions as F, Window

spark = (
    SparkSession.builder
    .appName("check_forwardfill_and_duplicates_merged")
    .config("spark.sql.caseSensitive", "false")
    .config("spark.sql.parquet.mergeSchema", "true")
    .config("spark.driver.memory", "8g")
    .config("spark.executor.memory", "8g")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================
# CONFIG
# ======================
PATH = "/labs/khanna/linkedin_202507/processed/inventor_year_merged"

print(f"[INFO] Reading merged dataset from: {PATH}")
df = spark.read.parquet(PATH)

# --- Sanity info ---
row_count = df.count()
unique_inventors = df.select("user_id").distinct().count()
print(f"[INFO] Total rows: {row_count:,}")
print(f"[INFO] Unique inventors: {unique_inventors:,}")

summary = df.agg(
    F.min("year").alias("min_year"),
    F.max("year").alias("max_year"),
    (F.count("year") / F.countDistinct("user_id")).alias("avg_years_per_inventor")
).collect()[0]
print(f"[INFO] Year range: {summary['min_year']}–{summary['max_year']}")
print(f"[INFO] Avg years per inventor: {summary['avg_years_per_inventor']:.2f}")

# ======================
# 1️⃣ Check duplicate (user_id, year) pairs
# ======================
dup = (
    df.groupBy("user_id", "year")
      .count()
      .filter("count > 1")
)
dup_count = dup.count()
print(f"\n[CHECK] Duplicate (user_id, year) pairs: {dup_count}")
if dup_count > 0:
    dup.show(10, truncate=False)
    dup_ratio = dup_count / row_count
    print(f"[INFO] Duplicate ratio: {dup_ratio:.4%}")

# ======================
# 2️⃣ Check missing forward-fill after first non-null rcid
# ======================
rcid_first = (
    df.filter(F.col("first_rcid").isNotNull())
      .groupBy("user_id")
      .agg(F.min("year").alias("first_rcid_year"))
)

df_check = (
    df.join(rcid_first, on="user_id", how="left")
      .filter(F.col("year") >= F.col("first_rcid_year"))
)

missing_after_fill = df_check.filter(F.col("first_rcid").isNull()).count()
print(f"[CHECK] Rows with NULL rcid after inventor already had one: {missing_after_fill}")

if missing_after_fill > 0:
    offenders = (
        df_check.filter(F.col("first_rcid").isNull())
                .select("user_id").distinct().limit(5).collect()
    )
    sample_ids = [r["user_id"] for r in offenders]
    print(f"[INFO] Sample offenders: {sample_ids}")
    for uid in sample_ids:
        print(f"\n[DEBUG] user_id={uid}")
        df.filter(df.user_id == uid).orderBy("year").select("year", "first_rcid").show(20)

# ======================
# 3️⃣ Inspect long-history inventors for sanity
# ======================
print("\n[INFO] Inspecting long-history inventors...")

# Compute number of years per inventor
inventor_years = (
    df.groupBy("user_id")
      .agg(
          F.countDistinct("year").alias("n_years"),
          F.min("year").alias("min_year"),
          F.max("year").alias("max_year")
      )
)

# Select inventors with long history (e.g., 30+ years)
long_hist = inventor_years.filter(F.col("n_years") >= 30).orderBy(F.desc("n_years"))
n_long = long_hist.count()
print(f"[INFO] Found {n_long:,} inventors with 30+ years of history.")

if n_long > 0:
    top_long = [r["user_id"] for r in long_hist.limit(5).collect()]
    print(f"[INFO] Sample long-history inventors: {top_long}")

    for uid in top_long:
        print(f"\n[DEBUG] Full history for user_id={uid}")
        df.filter(F.col("user_id") == uid) \
          .orderBy("year") \
          .select(
              "year",
              "first_rcid",
              "first_city",
              "first_country",
              "n_patents",
              "n_positions",
              "first_startdate_pos", 
              "first_startdate_edu",
              "first_enddate_edu"
          ).show(60, truncate=False)


# ======================
# 4️⃣ Optional: Inspect a specific inventor's full history
# ======================
target_id = 6586880  # 🔹 change this to any user_id you want to explore

print(f"\n[INFO] Inspecting full career and education history for user_id={target_id} ...")

cols_to_show = [
    "year", "first_rcid", "last_rcid",
    "first_city", "last_city",
    "first_country", "last_country",
    "n_positions", "n_patents", "n_applications", "n_first_inventor",
    "avg_salary", "min_salary", "max_salary", "avg_total_comp",
    "first_startdate_pos", "first_startdate_edu", "first_enddate_edu",
    "last_startdate_edu", "last_enddate_edu",
    "first_university", "last_university",
    "first_field", "last_field"
]

# Filter and order by year for a chronological view
df.filter(F.col("user_id") == target_id) \
  .orderBy("year") \
  .select(*[c for c in cols_to_show if c in df.columns]) \
  .show(200, truncate=False)

# Show firm changes across time (rcid transitions)
w = Window.partitionBy("user_id").orderBy("year")
firm_switches = (
    df.filter(F.col("user_id") == target_id)
      .withColumn("prev_rcid", F.lag("first_rcid").over(w))
      .withColumn("switched_firm", F.when(F.col("first_rcid") != F.col("prev_rcid"), 1).otherwise(0))
      .filter("switched_firm = 1")
      .select("year", "first_rcid", "prev_rcid")
)

if firm_switches.count() > 0:
    print(f"[INFO] Firm switches detected for user_id={target_id}:")
    firm_switches.show(20, truncate=False)
else:
    print(f"[INFO] No firm switches detected for user_id={target_id}.")

# ======================
# 5️⃣ Summary statistics for startdate variables
# ======================
print("\n[INFO] Computing summary statistics for first_startdate_pos and first_startdate_edu ...")

# Convert to year for easier comparison
df = (
    df.withColumn("year_first_start_pos", F.year("first_startdate_pos"))
      .withColumn("year_first_start_edu", F.year("first_startdate_edu"))
)

# Basic descriptive stats
stats = (
    df.agg(
        F.min("year_first_start_pos").alias("min_pos_year"),
        F.expr("percentile(year_first_start_pos, 0.25)").alias("p25_pos"),
        F.expr("percentile(year_first_start_pos, 0.5)").alias("median_pos"),
        F.expr("percentile(year_first_start_pos, 0.75)").alias("p75_pos"),
        F.max("year_first_start_pos").alias("max_pos_year"),
        F.min("year_first_start_edu").alias("min_edu_year"),
        F.expr("percentile(year_first_start_edu, 0.25)").alias("p25_edu"),
        F.expr("percentile(year_first_start_edu, 0.5)").alias("median_edu"),
        F.expr("percentile(year_first_start_edu, 0.75)").alias("p75_edu"),
        F.max("year_first_start_edu").alias("max_edu_year")
    )
).collect()[0]

print(f"[STATS] first_startdate_pos → "
      f"min={stats['min_pos_year']}, p25={stats['p25_pos']:.0f}, median={stats['median_pos']:.0f}, "
      f"p75={stats['p75_pos']:.0f}, max={stats['max_pos_year']}")
print(f"[STATS] first_startdate_edu → "
      f"min={stats['min_edu_year']}, p25={stats['p25_edu']:.0f}, median={stats['median_edu']:.0f}, "
      f"p75={stats['p75_edu']:.0f}, max={stats['max_edu_year']}")

# Frequency of suspicious early start years (like 1950)
print("\n[INFO] Frequency of early start years (<= 1955) in positions and education:")
early_pos = df.filter(F.col("year_first_start_pos") <= 1955).count()
early_edu = df.filter(F.col("year_first_start_edu") <= 1955).count()
total_nonnull_pos = df.filter(F.col("year_first_start_pos").isNotNull()).count()
total_nonnull_edu = df.filter(F.col("year_first_start_edu").isNotNull()).count()

share_pos = early_pos / total_nonnull_pos if total_nonnull_pos > 0 else 0
share_edu = early_edu / total_nonnull_edu if total_nonnull_edu > 0 else 0

print(f"[INFO] Early (≤1955) first_startdate_pos: {early_pos:,} "
      f"({share_pos:.2%} of non-null)")
print(f"[INFO] Early (≤1955) first_startdate_edu: {early_edu:,} "
      f"({share_edu:.2%} of non-null)")


# ============================
# 6️⃣ Construct AGE from first_startdate_pos only
# ============================
print("\n[INFO] Creating age control based solely on first_startdate_pos...")

df = df.withColumn("pos_start_year", F.year("first_startdate_pos"))
df = df.withColumn(
    "age",
    F.when(
        (F.col("year") >= F.year("first_startdate_pos")),
        F.col("year") - F.year("first_startdate_pos")
    )
)


# ============================
# Summary statistics: full sample
# ============================
print("\n[INFO] Summary statistics for AGE (full sample):")

summary_full = (
    df.select("age")
      .filter(F.col("age").isNotNull())
      .agg(
          F.count("age").alias("count"),
          F.mean("age").alias("mean"),
          F.expr("percentile(age, array(0.25,0.5,0.75))").alias("quartiles"),
          F.min("age").alias("min"),
          F.max("age").alias("max")
      )
      .collect()[0]
)

q25, q50, q75 = summary_full["quartiles"]
print(f"[STATS - Full sample] count={summary_full['count']:,}, "
      f"mean={summary_full['mean']:.2f}, "
      f"min={summary_full['min']:.2f}, "
      f"p25={q25:.2f}, median={q50:.2f}, p75={q75:.2f}, "
      f"max={summary_full['max']:.2f}")

# ============================
# Summary statistics: post-1990 sample
# ============================
print("\n[INFO] Summary statistics for AGE (year ≥ 1990):")

summary_post1990 = (
    df.filter(F.col("year") >= 1990)
      .select("age")
      .filter(F.col("age").isNotNull())
      .agg(
          F.count("age").alias("count"),
          F.mean("age").alias("mean"),
          F.expr("percentile(age, array(0.25,0.5,0.75))").alias("quartiles"),
          F.min("age").alias("min"),
          F.max("age").alias("max")
      )
      .collect()[0]
)

q25_90, q50_90, q75_90 = summary_post1990["quartiles"]
print(f"[STATS - Year ≥1990] count={summary_post1990['count']:,}, "
      f"mean={summary_post1990['mean']:.2f}, "
      f"min={summary_post1990['min']:.2f}, "
      f"p25={q25_90:.2f}, median={q50_90:.2f}, p75={q75_90:.2f}, "
      f"max={summary_post1990['max']:.2f}")

print("\n[INFO] ✅ Age (based on first position start year) successfully created and summarized.")



spark.stop()
print("\n[INFO] ✅ Diagnostic completed successfully on merged dataset.")
