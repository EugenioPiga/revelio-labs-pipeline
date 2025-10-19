from pyspark.sql import SparkSession, functions as F

# ======================
# Config
# ======================
COMPANY_PATH = "/labs/khanna/linkedin_202507/academic_company_ref"
POSITION_PATH = "/labs/khanna/linkedin_202507/academic_individual_position"
INVENTOR_POSITION_PATH = "/labs/khanna/linkedin_202507/processed/inventor_position_file"

# ======================
# Spark Session
# ======================
spark = (
    SparkSession.builder
    .appName("rcid_exploration_universe_inventors")
    .config("spark.executor.memory", "20g")
    .config("spark.driver.memory", "20g")
    .config("spark.executor.cores", "4")
    .config("spark.sql.shuffle.partitions", "400")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# ======================
# Helper: run analysis
# ======================
def analyze_positions(df_position, label):
    print(f"\n==================== {label} ====================")

    # Select + rename company variables
    df_company_sel = (
        df_company
        .select(
            F.col("rcid").alias("rcid"),
            F.col("hq_city").alias("comp_hq_city"),
            F.col("hq_state").alias("comp_hq_state"),
            F.col("hq_country").alias("comp_hq_country")
        )
    )

    # Join
    df_join = df_position.join(df_company_sel, on="rcid", how="left")

    # --- HQ vs Position country mismatches ---
    mismatch_country = (
        df_join
        .filter((F.col("comp_hq_country").isNotNull()) & (F.col("country").isNotNull()))
        .withColumn("mismatch", F.when(F.col("comp_hq_country") != F.col("country"), 1).otherwise(0))
        .groupBy("mismatch")
        .count()
    )
    print("\n--- HQ country vs Position country mismatches ---")
    mismatch_country.show()

    # --- rcids with multiple HQs or multiple position cities ---
    multi_locations = (
        df_join
        .groupBy("rcid")
        .agg(
            F.countDistinct("comp_hq_city").alias("n_hq_cities"),
            F.countDistinct("city").alias("n_position_cities")
        )
        .filter((F.col("n_hq_cities") > 1) | (F.col("n_position_cities") > 1))
    )
    print("\n--- rcids with multiple HQ or multiple position cities (top 20) ---")
    multi_locations.show(20, truncate=False)

    # --- Fraction of firms with multiple HQs or multiple cities ---
    firm_stats = (
        df_join
        .groupBy("rcid")
        .agg(
            F.countDistinct("comp_hq_city").alias("n_hq_cities"),
            F.countDistinct("city").alias("n_position_cities")
        )
    )

    total_firms = firm_stats.count()
    multi_hq = firm_stats.filter(F.col("n_hq_cities") > 1).count()
    multi_city = firm_stats.filter(F.col("n_position_cities") > 1).count()

    print("\n--- Fractions ---")
    print(f"Total firms: {total_firms}")
    print(f"Fraction with multiple HQs: {multi_hq / total_firms:.4f}")
    print(f"Fraction with positions in multiple cities: {multi_city / total_firms:.4f}")


# ======================
# Load company file once
# ======================
print("[INFO] Reading company file...")
df_company = spark.read.parquet(COMPANY_PATH)

# ======================
# 1. Universe analysis
# ======================
print("[INFO] Running analysis on ALL LinkedIn positions (universe)...")
df_position_universe = spark.read.parquet(POSITION_PATH)
analyze_positions(df_position_universe, "UNIVERSE")

# ======================
# 2. Inventor analysis
# ======================
print("[INFO] Running analysis on INVENTORS only...")
df_position_inventor = spark.read.parquet(INVENTOR_POSITION_PATH)
analyze_positions(df_position_inventor, "INVENTORS")
