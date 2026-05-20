#!/usr/bin/env python3
import os
import argparse
from pyspark.sql import SparkSession, functions as F

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--us-cit-path", required=True)
    ap.add_argument("--cpc-path", required=True)
    ap.add_argument("--out-dir", required=True)
    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "40")))
    ap.add_argument("--shuffle-partitions", type=int, default=4000)
    ap.add_argument("--coalesce", type=int, default=800)
    ap.add_argument("--tmpdir", default=None)
    return ap.parse_args()

def main():
    args = parse_args()

    builder = (
        SparkSession.builder
        .appName("precompute_uspto_aux")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "200g"))
        .config("spark.executor.memory", os.environ.get("SPARK_EXECUTOR_MEMORY", "200g"))
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    )
    if args.tmpdir:
        builder = builder.config("spark.local.dir", args.tmpdir)

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    os.makedirs(args.out_dir, exist_ok=True)
    out_cit = os.path.join(args.out_dir, "us_citations_by_patent")
    out_cpc = os.path.join(args.out_dir, "cpc_main")

    # ---- US->US citations collapsed to cited patent_id ----
    print("[1/2] Precomputing US citations by patent_id …")
    us_cit = (
        spark.read.option("header", True).option("sep", "\t").csv(args.us_cit_path)
        .select(F.col("citation_patent_id").alias("patent_id"))
        .where(F.col("patent_id").isNotNull())
        .repartition(args.shuffle_partitions, "patent_id")
        .groupBy("patent_id")
        .agg(F.count("*").alias("n_us_citations"))
    )

    (us_cit.coalesce(args.coalesce)
          .write.mode("overwrite")
          .option("compression", "snappy")
          .parquet(out_cit))
    print(f"      -> wrote {out_cit}")

    # ---- CPC main (sequence==0) ----
    print("[2/2] Precomputing CPC main (sequence==0) …")
    cpc_main = (
        spark.read.option("header", True).option("sep", "\t").csv(args.cpc_path)
        .select("patent_id", "cpc_sequence", "cpc_section", "cpc_class")
        .withColumn("cpc_sequence", F.col("cpc_sequence").cast("int"))
        .filter(F.col("cpc_sequence") == 0)
        .select(
            "patent_id",
            F.col("cpc_section").alias("cpc_section_main"),
            F.col("cpc_class").alias("cpc_class_main"),
        )
        .where(F.col("patent_id").isNotNull())
        .dropDuplicates(["patent_id"])
    )

    (cpc_main.coalesce(args.coalesce)
            .write.mode("overwrite")
            .option("compression", "snappy")
            .parquet(out_cpc))
    print(f"      -> wrote {out_cpc}")

    spark.stop()
    print("[INFO] Done.")

if __name__ == "__main__":
    main()
