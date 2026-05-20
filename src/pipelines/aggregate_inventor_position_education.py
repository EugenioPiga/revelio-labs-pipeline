#!/usr/bin/env python
"""
aggregate_inventor_position_education.py

Take inventor_position_education and aggregate education variables per inventor.
Attach those summaries back to the inventor-position rows.

For each inventor (user_id), compute:
- First education (earliest startdate_edu)
- Last education (latest enddate_edu)
- Total number of degrees

Output: inventor_position_with_education (sharded parquet).
"""

import os
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


def main():
    spark = (
        SparkSession.builder
        .appName("aggregate_inventor_position_education")
        .config("spark.sql.shuffle.partitions", "512")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # Paths
    in_dir = "/labs/khanna/linkedin_202507/processed/inventor_position_education"
    out_dir = "/labs/khanna/linkedin_202507/processed/inventor_position_with_education"

    print("[1/5] Load inventor_position_education …")
    df = spark.read.parquet(os.path.join(in_dir, "*"))

    # --- First education
    w_first = Window.partitionBy("user_id").orderBy(F.col("startdate_edu").asc_nulls_last())
    first_edu = (
        df.withColumn("rn", F.row_number().over(w_first))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "user_id",
            F.col("university_name").alias("first_university_name"),
            F.col("degree").alias("first_degree"),
            F.col("field").alias("first_field"),
            F.col("startdate_edu").alias("first_startdate_edu"),
            F.col("enddate_edu").alias("first_enddate_edu"),
            F.col("rsid").alias("first_rsid"),
            F.col("ultimate_parent_school_name").alias("first_ultimate_parent_school_name"),
            F.col("ultimate_parent_rsid").alias("first_ultimate_parent_rsid"),
            F.col("description").alias("first_description"),
            F.col("university_country").alias("first_university_country"),
            F.col("university_location").alias("first_university_location"),
            F.col("education_number").alias("first_education_number"),
        )
    )

    # --- Last education
    w_last = Window.partitionBy("user_id").orderBy(F.col("enddate_edu").desc_nulls_last())
    last_edu = (
        df.withColumn("rn", F.row_number().over(w_last))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(
            "user_id",
            F.col("university_name").alias("last_university_name"),
            F.col("degree").alias("last_degree"),
            F.col("field").alias("last_field"),
            F.col("startdate_edu").alias("last_startdate_edu"),
            F.col("enddate_edu").alias("last_enddate_edu"),
            F.col("rsid").alias("last_rsid"),
            F.col("ultimate_parent_school_name").alias("last_ultimate_parent_school_name"),
            F.col("ultimate_parent_rsid").alias("last_ultimate_parent_rsid"),
            F.col("description").alias("last_description"),
            F.col("university_country").alias("last_university_country"),
            F.col("university_location").alias("last_university_location"),
            F.col("education_number").alias("last_education_number"),
        )
    )

    # --- Number of degrees
    n_degrees = df.dropDuplicates(["user_id", "university_name", "degree", "field", "startdate_edu", "enddate_edu"]).groupBy("user_id").agg(F.count("*").alias("num_degrees"))

    print("[2/5] Merge aggregates …")
    edu_agg = (
        first_edu
        .join(last_edu, "user_id", "outer")
        .join(n_degrees, "user_id", "outer")
    )

    print("[3/5] Attach aggregates back to inventor-position rows …")
    df_out = df.join(edu_agg, "user_id", "left")

    print("[4/5] Write out …")
    (
        df_out
        .repartition(200)   # shards, lighter memory
        .write.mode("overwrite")
        .option("compression", "snappy")
        .parquet(out_dir)
    )

    print(f"==> Done.")
    spark.stop()


if __name__ == "__main__":
    main()
