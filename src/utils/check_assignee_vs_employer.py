#!/usr/bin/env python
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ======================
# Config
# ======================
ASSIGNEE_FILE = "/labs/khanna/linkedin_202507/revelio_patents_assignee_matches"
POSITION_FILE = "/labs/khanna/linkedin_202507/processed/inventor_position_file"
OUT_DIR = "/home/epiga/revelio_labs/output/assignee_vs_employer_named.csv"

spark = (
    SparkSession.builder
    .appName("Check assignee vs employer RCIDs with inventor names")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

os.makedirs(OUT_DIR, exist_ok=True)

# ======================
# Load datasets
# ======================
print("[INFO] Reading assignee data...")
assignee = spark.read.parquet(ASSIGNEE_FILE).select(
    "patent_id", "rcid", "company_name"
).withColumnRenamed("rcid", "assignee_rcid") \
 .withColumnRenamed("company_name", "assignee_name")

print("[INFO] Reading inventor positions...")
inventor = spark.read.parquet(POSITION_FILE).select(
    "user_id", "user_fullname", "patent_ids", "rcid", "company_name"
).withColumnRenamed("rcid", "employer_rcid") \
 .withColumnRenamed("company_name", "employer_name")

# Explode patent_ids so each patent gets its own row
inventor = inventor.withColumn("patent_id", F.explode("patent_ids"))

# ======================
# Join on patent_id
# ======================
df = inventor.join(assignee, on="patent_id", how="inner")

# Keep only mismatches
mismatch = df.filter(F.col("employer_rcid") != F.col("assignee_rcid"))

# ======================
# RCID -> multiple names check
# ======================

from pyspark.sql import functions as F

def save_rcid_name_map(df, rcid_col, name_col, out_name):
    """
    For a given (rcid, name) mapping, find cases where a single rcid
    maps to multiple distinct names. Save only those cases.
    """
    name_map = (
        df.groupBy(rcid_col)
          .agg(F.collect_set(name_col).alias("names"))
          .withColumn("n_names", F.size("names"))
          .filter(F.col("n_names") > 1)  # keep only duplicates
          .withColumn("names_str", F.concat_ws("; ", F.col("names")))  # array -> string
    )

    out_path = os.path.join(OUT_DIR, out_name)
    print(f"[INFO] Writing RCID-to-names map to {out_path} (only where >1 name)")
    (
        name_map
        .select(rcid_col, "n_names", "names_str")
        .orderBy(F.col("n_names").desc())  # biggest offenders first
        .limit(1000)                       # show more examples, not just 100
        .coalesce(1)
        .write.mode("overwrite").option("header", True).csv(out_path)
    )


# Employer RCID duplicates
save_rcid_name_map(inventor, "employer_rcid", "employer_name", "employer_name_map")

# Assignee RCID duplicates
save_rcid_name_map(assignee, "assignee_rcid", "assignee_name", "assignee_name_map")

# ======================
# Save mismatches with names (sample to avoid explosion)
# ======================
print("[INFO] Writing mismatches with names...")
mismatch.select(
    "patent_id",
    "user_id", "user_fullname",
    "employer_rcid", "employer_name",
    "assignee_rcid", "assignee_name"
).limit(100000).coalesce(1).write.mode("overwrite").option("header", True).csv(
    os.path.join(OUT_DIR, "mismatch_sample")
)

print(f"[INFO] Done. Results saved to {OUT_DIR}")
