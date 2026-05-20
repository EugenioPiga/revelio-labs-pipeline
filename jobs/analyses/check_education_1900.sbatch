#!/bin/bash
#SBATCH --job-name=check_education_pre1930_sample
#SBATCH --output=/home/epiga/revelio_labs/logs/check_education_pre1930_%j.out
#SBATCH --error=/home/epiga/revelio_labs/logs/check_education_pre1930_%j.err
#SBATCH --time=00:30:00
#SBATCH --mem=40G
#SBATCH --cpus-per-task=8
#SBATCH --partition=general
#SBATCH --mail-type=BEGIN,END,FAIL
#SBATCH --mail-user=epiga@ucsd.edu

source miniconda/bin/activate revelio

python3 <<'EOF'
from pyspark.sql import SparkSession, functions as F

spark = (
    SparkSession.builder
    .appName("check_education_pre1930_sample")
    .config("spark.sql.shuffle.partitions", "100")
    .config("spark.sql.adaptive.enabled", "true")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

INPUT = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
OUTPUT = "/home/epiga/revelio_labs/output/education_pre1930_inventors_sample.csv"

print(f"[INFO] Reading dataset from {INPUT} ...")
df = spark.read.parquet(INPUT)
print(f"[INFO] Total rows in dataset: {df.count():,}")
print(f"[INFO] Columns: {df.columns}")

# Sample 1% for quick inspection
df_sample = df.sample(fraction=0.01, seed=42)
print(f"[INFO] Sampled 1% subset with {df_sample.count():,} rows.")

# Extract start year safely and filter where year < 1930
edu_pre1930 = (
    df_sample
    .filter(F.col("startdate_edu").isNotNull())
    .withColumn("start_year", F.year("startdate_edu"))
    .filter(F.col("start_year") < 1930)
)

count_pre1930 = edu_pre1930.count()
print(f"[INFO] Found {count_pre1930:,} records with startdate_edu before 1930 in sample.")

if count_pre1930 > 0:
    print("\n[INFO] Sample of up to 20 records:\n")
    edu_pre1930.select(
        "user_id",
        "user_fullname",
        "university_name",
        "degree",
        "field",
        "startdate_edu",
        "enddate_edu",
        "start_year"
    ).show(20, truncate=False)

    print(f"[INFO] Saving all {count_pre1930:,} records to {OUTPUT}")
    (
        edu_pre1930
        .select("user_id", "user_fullname", "university_name", "degree", "field", "startdate_edu", "enddate_edu", "start_year")
        .coalesce(1)
        .write
        .option("header", True)
        .mode("overwrite")
        .csv(OUTPUT)
    )
else:
    print("[INFO] ✅ No education records before 1930 found in sample.")

spark.stop()
print("[INFO] Done.")
EOF

echo "[INFO] Job finished at $(date)"
