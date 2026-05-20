#!/usr/bin/env python3
###############################################################################
# step6_colleague_citation_collaboration_measures.py
#
# Add colleague citation, incoming-citation, and collaboration measures to the
# final inventor-year panel.
#
# Input panel:
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2_onet_naics
#
# Output panel:
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2
#
# What this does:
#   1. Reads the final merged inventor-year panel with O*NET / NAICS variables.
#
#   2. Builds inventor-year attributes:
#        parent_for_links = coalesce(current_parent_rcid,
#                                    last_parent_rcid,
#                                    first_parent_rcid)
#        metro_for_links  = coalesce(last_metro_area, first_metro_area)
#        seniority_floor  = floor(avg_seniority)
#
#   3. Uses patent-inventor matches and USPTO citation edges to construct
#      inventor-to-inventor citation links:
#        i = citing inventor
#        j = cited inventor
#
#      Citation year is defined as the filing year of the citing patent.
#
#   4. Computes outgoing citation-to-colleague measures, grouped by the citing
#      inventor i and year. These answer:
#
#        "Whom does the focal inventor cite?"
#
#      The outgoing citation measures split cited colleagues into:
#        - same parent
#        - same parent x metro
#        - more senior cited colleague
#        - less/equal senior cited colleague
#
#   5. Computes incoming citation-received measures, grouped by the cited/focal
#      inventor j and citing year. These answer:
#
#        "Who cites the focal inventor?"
#
#      The incoming citation measures split citing colleagues into:
#        - same parent
#        - same parent x metro
#        - citing inventor more senior than the focal inventor
#        - citing inventor less senior than the focal inventor
#        - citing inventor equal seniority to the focal inventor
#        - citing inventor less/equal senior than the focal inventor
#
#      These variables are designed to study whether, over tenure, an inventor's
#      work is increasingly cited by junior or senior colleagues inside the same
#      parent firm.
#
#   6. Computes both link-level and patent-level citation measures:
#        - link-level counts count inventor-to-inventor citation links;
#        - patent-level counts count distinct cited or citing patents satisfying
#          each same-parent / relative-seniority condition.
#
#   7. Builds co-inventor collaboration links from patent-inventor matches.
#      Collaboration year is the filing year of the focal patent.
#
#   8. Computes collaboration-with-colleague measures, grouped by focal inventor
#      and year. These split collaborators into:
#        - same parent
#        - same parent x metro
#        - more senior collaborator
#        - less/equal senior collaborator
#
#   9. Joins all outgoing citation, incoming citation, and collaboration measures
#      back to the inventor-year panel.
#
#  10. Creates share variables:
#        - outgoing citation shares;
#        - incoming citation-received shares;
#        - incoming citation-received shares conditional on same-parent citations;
#        - patent-level incoming citation shares;
#        - collaboration link shares;
#        - collaboration patent shares.
#
#  11. Publishes final output as inventor_year_merged_v2.
#
# Key interpretation:
#   Existing outgoing variables answer:
#     "Over tenure, does inventor i cite senior or junior colleagues?"
#
#   New incoming variables answer:
#     "Over tenure, is inventor j cited by senior or junior colleagues?"
#
# Notes:
#   - Citation year is the filing year of the citing patent.
#   - Collaboration year is the filing year of the focal patent.
#   - Seniority comparison uses floor(avg_seniority).
#   - Self-links are excluded: i != j.
#   - Parent comparisons use parent_for_links.
#   - Metro comparisons use metro_for_links.
###############################################################################

import argparse
import os
import shutil
import time
from pathlib import Path

from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F


VALID_YEAR_MIN = 1950
VALID_YEAR_MAX = 2025


# =============================================================================
# Arguments
# =============================================================================

def parse_args():
    ap = argparse.ArgumentParser()

    ap.add_argument(
        "--input-panel",
        default="/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2_onet_naics",
        help="Merged inventor-year panel with O*NET / NAICS variables."
    )

    ap.add_argument(
        "--patent-matches",
        default="/labs/khanna/linkedin_202507/revelio_patents_inventor_matches",
        help="Revelio inventor-patent match parquet dataset."
    )

    ap.add_argument(
        "--citations-tsv",
        default="/labs/khanna/USPTO_202507/g_us_patent_citation.tsv",
        help="USPTO citation edge TSV. Expected columns: patent_id, citation_patent_id."
    )

    ap.add_argument(
        "--out-dir",
        default="/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2",
        help="Final output directory."
    )

    ap.add_argument(
        "--tmp-out-dir",
        default=None,
        help="Temporary output directory. Default: out-dir + _tmp_step6_<timestamp>."
    )

    ap.add_argument(
        "--checkpoint-dir",
        default="/labs/khanna/linkedin_202507/scratch/spark_checkpoints_step6_colleague_links",
        help="Spark checkpoint directory."
    )

    ap.add_argument(
        "--tmpdir",
        default="/labs/khanna/linkedin_202507/scratch/spark_step6_colleague_links",
        help="Spark local scratch directory."
    )

    ap.add_argument("--threads", type=int, default=int(os.environ.get("SLURM_CPUS_PER_TASK", "64")))
    ap.add_argument("--shuffle-partitions", type=int, default=4000)
    ap.add_argument("--out-partitions", type=int, default=300)

    ap.add_argument(
        "--no-publish",
        action="store_true",
        help="If set, write only tmp-out-dir and do not replace/publish out-dir."
    )

    return ap.parse_args()


# =============================================================================
# Helpers
# =============================================================================

def path_exists(path: str) -> bool:
    return os.path.exists(path) or os.path.islink(path)


def remove_path(path: str):
    if os.path.islink(path) or os.path.isfile(path):
        os.unlink(path)
    elif os.path.isdir(path):
        shutil.rmtree(path)


def publish_output(tmp_out: str, out_dir: str):
    """
    Safely publish tmp_out as out_dir.

    If out_dir exists:
      - if symlink: unlink symlink only
      - if directory/file: rename to backup
    Then rename tmp_out to out_dir.
    """
    timestamp = time.strftime("%Y%m%d_%H%M%S")
    backup = f"{out_dir}_BACKUP_PRE_STEP6_{timestamp}"

    print("\n[PUBLISH]")
    print(f"tmp_out = {tmp_out}")
    print(f"out_dir = {out_dir}")

    if not os.path.isdir(tmp_out):
        raise RuntimeError(f"Temporary output does not exist: {tmp_out}")

    if path_exists(out_dir):
        if os.path.islink(out_dir):
            target = os.readlink(out_dir)
            print(f"[PUBLISH] Existing out_dir is a symlink to: {target}")
            print(f"[PUBLISH] Unlinking symlink only: {out_dir}")
            os.unlink(out_dir)
        else:
            print(f"[PUBLISH] Existing out_dir found. Moving to backup:")
            print(f"          {backup}")
            os.rename(out_dir, backup)

    print(f"[PUBLISH] Moving tmp output to final path:")
    print(f"          {out_dir}")
    os.rename(tmp_out, out_dir)

    print("[PUBLISH] Done.")


def add_missing(df, colname, dtype):
    if colname not in df.columns:
        df = df.withColumn(colname, F.lit(None).cast(dtype))
    return df


def safe_divide(num_col, den_col):
    return F.when(F.col(den_col) > 0, F.col(num_col) / F.col(den_col)).otherwise(F.lit(0.0))


def sum_flag(condition, name):
    return F.sum(F.when(condition, F.lit(1)).otherwise(F.lit(0))).alias(name)


def first_existing_col(df, candidates, dtype=None):
    """
    Return coalesce of first available candidate columns.
    If dtype is provided, cast each candidate to dtype before coalescing.
    """
    exprs = []
    for c in candidates:
        if c in df.columns:
            e = F.col(c)
            if dtype:
                e = e.cast(dtype)
            exprs.append(e)
    if not exprs:
        return F.lit(None).cast(dtype if dtype else "string")
    return F.coalesce(*exprs)


def check_keys(df, label, do_count=False):
    print(f"\n[CHECK] {label}")
    print(f"[CHECK] columns = {len(df.columns)}")
    if do_count:
        print(f"[CHECK] rows = {df.count():,}")
    try:
        df.limit(5).show(5, truncate=False)
    except Exception as e:
        print(f"[WARN] sample failed: {e}")


# =============================================================================
# Main
# =============================================================================

def main():
    args = parse_args()

    if args.tmp_out_dir is None:
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        tmp_out = f"{args.out_dir}_tmp_step6_{timestamp}"
    else:
        tmp_out = args.tmp_out_dir

    # -------------------------------------------------------------------------
    # Spark setup
    # -------------------------------------------------------------------------

    os.makedirs(args.tmpdir, exist_ok=True)
    os.makedirs(args.checkpoint_dir, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("step6_colleague_citation_collaboration_measures")
        .master(f"local[{args.threads}]")
        .config("spark.sql.shuffle.partitions", str(args.shuffle_partitions))
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.parquet.enableVectorizedReader", "false")
        .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
        .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
        .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .config("spark.local.dir", args.tmpdir)
        .config("spark.sql.warehouse.dir", os.path.join(args.tmpdir, "warehouse"))
        .config("spark.driver.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
        .config("spark.executor.extraJavaOptions", f"-Djava.io.tmpdir={os.path.join(args.tmpdir, 'tmp')}")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.setCheckpointDir(args.checkpoint_dir)

    print("=" * 100)
    print("[STEP6] Colleague citation and collaboration measures")
    print("=" * 100)
    print(f"[INPUT_PANEL]    {args.input_panel}")
    print(f"[PATENT_MATCHES] {args.patent_matches}")
    print(f"[CITATIONS_TSV]  {args.citations_tsv}")
    print(f"[TMP_OUT]        {tmp_out}")
    print(f"[FINAL_OUT]      {args.out_dir}")
    print("=" * 100)

    if path_exists(tmp_out):
        print(f"[INFO] Removing existing tmp output: {tmp_out}")
        remove_path(tmp_out)

    # =========================================================================
    # 1. Read inventor-year panel
    # =========================================================================

    print("\n[1/8] Loading inventor-year panel ...")

    panel = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.input_panel)
    )

    panel = (
        panel.withColumn("user_id_key", F.col("user_id").cast("string"))
             .withColumn("year", F.col("year").cast("int"))
    )

    panel = panel.filter(
        F.col("user_id_key").isNotNull()
        & F.col("year").isNotNull()
        & (F.col("year") >= VALID_YEAR_MIN)
        & (F.col("year") <= VALID_YEAR_MAX)
    )

    print(f"[INFO] Panel columns: {len(panel.columns)}")

    # Ensure needed columns exist.
    for c in ["current_parent_rcid", "last_parent_rcid", "first_parent_rcid"]:
        panel = add_missing(panel, c, "long")

    for c in ["last_metro_area", "first_metro_area"]:
        panel = add_missing(panel, c, "string")

    panel = add_missing(panel, "avg_seniority", "double")
    panel = add_missing(panel, "n_patents", "double")

    # Inventor-year attributes used for link classification.
    attrs = (
        panel
        .select(
            "user_id_key",
            "year",
            first_existing_col(
                panel,
                ["current_parent_rcid", "last_parent_rcid", "first_parent_rcid"],
                dtype="long"
            ).alias("parent_for_links_long"),
            first_existing_col(
                panel,
                ["last_metro_area", "first_metro_area"],
                dtype="string"
            ).alias("metro_for_links"),
            F.floor(F.col("avg_seniority").cast("double")).cast("int").alias("seniority_floor")
        )
        .withColumn("parent_for_links", F.col("parent_for_links_long").cast("string"))
        .drop("parent_for_links_long")
        .dropDuplicates(["user_id_key", "year"])
        .cache()
    )

    base_users = attrs.select("user_id_key").distinct().cache()

    check_keys(attrs, "attrs", do_count=False)

    # =========================================================================
    # 2. Patent-inventor table
    # =========================================================================

    print("\n[2/8] Loading patent-inventor matches ...")

    pat_inv_raw = (
        spark.read
             .option("datetimeRebaseMode", "LEGACY")
             .option("int96RebaseMode", "LEGACY")
             .parquet(args.patent_matches)
    )

    required_pat_cols = ["patent_id", "user_id", "filing_date"]
    for c in required_pat_cols:
        if c not in pat_inv_raw.columns:
            raise ValueError(f"Required column {c} not found in patent matches.")

    pat_inv = (
        pat_inv_raw
        .select(
            F.col("patent_id").cast("string").alias("patent_id"),
            F.col("user_id").cast("string").alias("user_id_key"),
            F.col("filing_date").alias("filing_date")
        )
        .filter(F.col("patent_id").isNotNull() & F.col("user_id_key").isNotNull())
        .withColumn(
            "filing_date",
            F.when(
                (F.col("filing_date") < F.to_date(F.lit(f"{VALID_YEAR_MIN}-01-01")))
                | (F.col("filing_date") > F.to_date(F.lit(f"{VALID_YEAR_MAX}-12-31"))),
                F.lit(None)
            ).otherwise(F.col("filing_date"))
        )
        .withColumn("year", F.year("filing_date").cast("int"))
        .filter(F.col("year").isNotNull())
        .join(base_users, on="user_id_key", how="inner")
        .dropDuplicates(["patent_id", "user_id_key", "year"])
        .cache()
    )

    check_keys(pat_inv, "pat_inv", do_count=False)

    # Patent-year table, one row per patent-year.
    patent_year = (
        pat_inv
        .groupBy("patent_id")
        .agg(F.min("year").alias("patent_year"))
        .cache()
    )

    # =========================================================================
    # 3. Citation edges
    # =========================================================================

    print("\n[3/8] Loading citation edges ...")

    citations_raw = (
        spark.read
             .option("header", True)
             .option("sep", "\t")
             .csv(args.citations_tsv)
    )

    if "patent_id" not in citations_raw.columns or "citation_patent_id" not in citations_raw.columns:
        raise ValueError(
            "Citation TSV must contain columns patent_id and citation_patent_id. "
            f"Found columns: {citations_raw.columns}"
        )

    citations = (
        citations_raw
        .select(
            F.col("patent_id").cast("string").alias("citing_patent_id"),
            F.col("citation_patent_id").cast("string").alias("cited_patent_id")
        )
        .filter(F.col("citing_patent_id").isNotNull() & F.col("cited_patent_id").isNotNull())
        .dropDuplicates(["citing_patent_id", "cited_patent_id"])
    )

    # Keep only citation edges where citing patent is in our inventor-patent table.
    citing_inv = (
        pat_inv
        .select(
            F.col("patent_id").alias("citing_patent_id"),
            F.col("user_id_key").alias("i_user_id_key"),
            F.col("year").alias("year")
        )
        .dropDuplicates(["citing_patent_id", "i_user_id_key", "year"])
    )

    cited_inv = (
        pat_inv
        .select(
            F.col("patent_id").alias("cited_patent_id"),
            F.col("user_id_key").alias("j_user_id_key")
        )
        .dropDuplicates(["cited_patent_id", "j_user_id_key"])
    )

    citation_links = (
        citations
        .join(citing_inv, on="citing_patent_id", how="inner")
        .join(cited_inv, on="cited_patent_id", how="inner")
        .filter(F.col("i_user_id_key") != F.col("j_user_id_key"))
    )

    # Join i and j attributes in citing year.
    attrs_i = (
        attrs
        .select(
            F.col("user_id_key").alias("i_user_id_key"),
            F.col("year").alias("year"),
            F.col("parent_for_links").alias("i_parent"),
            F.col("metro_for_links").alias("i_metro"),
            F.col("seniority_floor").alias("i_seniority_floor")
        )
    )

    attrs_j = (
        attrs
        .select(
            F.col("user_id_key").alias("j_user_id_key"),
            F.col("year").alias("year"),
            F.col("parent_for_links").alias("j_parent"),
            F.col("metro_for_links").alias("j_metro"),
            F.col("seniority_floor").alias("j_seniority_floor")
        )
    )

    citation_links = (
        citation_links
        .join(attrs_i, on=["i_user_id_key", "year"], how="left")
        .join(attrs_j, on=["j_user_id_key", "year"], how="left")
    )

    citation_links = (
        citation_links
        .withColumn(
            "same_parent",
            F.col("i_parent").isNotNull()
            & F.col("j_parent").isNotNull()
            & (F.col("i_parent") == F.col("j_parent"))
        )
        .withColumn(
            "same_parent_metro",
            F.col("same_parent")
            & F.col("i_metro").isNotNull()
            & F.col("j_metro").isNotNull()
            & (F.col("i_metro") == F.col("j_metro"))
        )
        .withColumn(
            "j_more_senior",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("j_seniority_floor") > F.col("i_seniority_floor"))
        )
        .withColumn(
            "j_less_equal_senior",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("j_seniority_floor") <= F.col("i_seniority_floor"))
        )
        .withColumn("same_parent_more_senior", F.col("same_parent") & F.col("j_more_senior"))
        .withColumn("same_parent_less_equal_senior", F.col("same_parent") & F.col("j_less_equal_senior"))
        .withColumn("same_parent_metro_more_senior", F.col("same_parent_metro") & F.col("j_more_senior"))
        .withColumn("same_parent_metro_less_equal_senior", F.col("same_parent_metro") & F.col("j_less_equal_senior"))
    )


    # -------------------------------------------------------------------------
    # Incoming-citation perspective:
    #   i = citing inventor
    #   j = cited / focal inventor
    #
    # These flags classify whether citations RECEIVED by j come from inventors
    # who are senior, junior, equal, or less/equal senior relative to j.
    # -------------------------------------------------------------------------

    citation_links = (
        citation_links
        .withColumn(
            "i_more_senior_than_j",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("i_seniority_floor") > F.col("j_seniority_floor"))
        )
        .withColumn(
            "i_less_senior_than_j",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("i_seniority_floor") < F.col("j_seniority_floor"))
        )
        .withColumn(
            "i_equal_senior_to_j",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("i_seniority_floor") == F.col("j_seniority_floor"))
        )
        .withColumn(
            "i_less_equal_senior_than_j",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("i_seniority_floor") <= F.col("j_seniority_floor"))
        )
        .withColumn("same_parent_from_more_senior", F.col("same_parent") & F.col("i_more_senior_than_j"))
        .withColumn("same_parent_from_less_senior", F.col("same_parent") & F.col("i_less_senior_than_j"))
        .withColumn("same_parent_from_equal_senior", F.col("same_parent") & F.col("i_equal_senior_to_j"))
        .withColumn("same_parent_from_less_equal_senior", F.col("same_parent") & F.col("i_less_equal_senior_than_j"))
        .withColumn("same_parent_metro_from_more_senior", F.col("same_parent_metro") & F.col("i_more_senior_than_j"))
        .withColumn("same_parent_metro_from_less_senior", F.col("same_parent_metro") & F.col("i_less_senior_than_j"))
        .withColumn("same_parent_metro_from_equal_senior", F.col("same_parent_metro") & F.col("i_equal_senior_to_j"))
        .withColumn("same_parent_metro_from_less_equal_senior", F.col("same_parent_metro") & F.col("i_less_equal_senior_than_j"))
    )

    check_keys(citation_links, "citation_links", do_count=False)

    # =========================================================================
    # 4. Citation measures
    # =========================================================================

    print("\n[4/8] Aggregating citation-to-colleague measures ...")

    citation_link_counts = (
        citation_links
        .groupBy(F.col("i_user_id_key").alias("user_id_key"), "year")
        .agg(
            F.count("*").alias("n_cite_links_total"),

            sum_flag(F.col("same_parent"), "n_cite_links_same_parent"),
            sum_flag(F.col("same_parent_more_senior"), "n_cite_links_same_parent_more_senior"),
            sum_flag(F.col("same_parent_less_equal_senior"), "n_cite_links_same_parent_less_equal_senior"),

            sum_flag(F.col("same_parent_metro"), "n_cite_links_same_parent_metro"),
            sum_flag(F.col("same_parent_metro_more_senior"), "n_cite_links_same_parent_metro_more_senior"),
            sum_flag(F.col("same_parent_metro_less_equal_senior"), "n_cite_links_same_parent_metro_less_equal_senior"),
        )
    )

    # Patent-level citation measures: whether each cited patent contains at least
    # one colleague / senior colleague / less-equal senior colleague.
    citation_patent_flags = (
        citation_links
        .groupBy(
            F.col("i_user_id_key").alias("user_id_key"),
            "year",
            "cited_patent_id"
        )
        .agg(
            F.max(F.when(F.col("same_parent"), 1).otherwise(0)).alias("p_cited_same_parent"),
            F.max(F.when(F.col("same_parent_more_senior"), 1).otherwise(0)).alias("p_cited_same_parent_more_senior"),
            F.max(F.when(F.col("same_parent_less_equal_senior"), 1).otherwise(0)).alias("p_cited_same_parent_less_equal_senior"),

            F.max(F.when(F.col("same_parent_metro"), 1).otherwise(0)).alias("p_cited_same_parent_metro"),
            F.max(F.when(F.col("same_parent_metro_more_senior"), 1).otherwise(0)).alias("p_cited_same_parent_metro_more_senior"),
            F.max(F.when(F.col("same_parent_metro_less_equal_senior"), 1).otherwise(0)).alias("p_cited_same_parent_metro_less_equal_senior"),
        )
    )

    citation_patent_counts = (
        citation_patent_flags
        .groupBy("user_id_key", "year")
        .agg(
            F.countDistinct("cited_patent_id").alias("n_cited_patents_total"),

            F.sum("p_cited_same_parent").alias("n_cited_patents_same_parent"),
            F.sum("p_cited_same_parent_more_senior").alias("n_cited_patents_same_parent_more_senior"),
            F.sum("p_cited_same_parent_less_equal_senior").alias("n_cited_patents_same_parent_less_equal_senior"),

            F.sum("p_cited_same_parent_metro").alias("n_cited_patents_same_parent_metro"),
            F.sum("p_cited_same_parent_metro_more_senior").alias("n_cited_patents_same_parent_metro_more_senior"),
            F.sum("p_cited_same_parent_metro_less_equal_senior").alias("n_cited_patents_same_parent_metro_less_equal_senior"),
        )
    )


    # -------------------------------------------------------------------------
    # Incoming citation measures:
    #   grouped by j_user_id_key, the cited / focal inventor.
    #
    # Interpretation:
    #   n_cite_links_received_same_parent_from_less_senior
    #   = number of citation links received by inventor j from junior inventors
    #     in the same parent firm, in the citing patent's filing year.
    # -------------------------------------------------------------------------

    incoming_citation_link_counts = (
        citation_links
        .groupBy(F.col("j_user_id_key").alias("user_id_key"), "year")
        .agg(
            F.count("*").alias("n_cite_links_received_total"),

            sum_flag(F.col("same_parent"), "n_cite_links_received_same_parent"),
            sum_flag(F.col("same_parent_from_more_senior"), "n_cite_links_received_same_parent_from_more_senior"),
            sum_flag(F.col("same_parent_from_less_senior"), "n_cite_links_received_same_parent_from_less_senior"),
            sum_flag(F.col("same_parent_from_equal_senior"), "n_cite_links_received_same_parent_from_equal_senior"),
            sum_flag(F.col("same_parent_from_less_equal_senior"), "n_cite_links_received_same_parent_from_less_equal_senior"),

            sum_flag(F.col("same_parent_metro"), "n_cite_links_received_same_parent_metro"),
            sum_flag(F.col("same_parent_metro_from_more_senior"), "n_cite_links_received_same_parent_metro_from_more_senior"),
            sum_flag(F.col("same_parent_metro_from_less_senior"), "n_cite_links_received_same_parent_metro_from_less_senior"),
            sum_flag(F.col("same_parent_metro_from_equal_senior"), "n_cite_links_received_same_parent_metro_from_equal_senior"),
            sum_flag(F.col("same_parent_metro_from_less_equal_senior"), "n_cite_links_received_same_parent_metro_from_less_equal_senior"),
        )
    )

    # Patent-level incoming measures:
    # whether a citing patent includes at least one same-parent senior/junior
    # inventor citing the focal inventor.
    incoming_citation_patent_flags = (
        citation_links
        .groupBy(
            F.col("j_user_id_key").alias("user_id_key"),
            "year",
            "citing_patent_id"
        )
        .agg(
            F.max(F.when(F.col("same_parent"), 1).otherwise(0)).alias("p_received_same_parent"),
            F.max(F.when(F.col("same_parent_from_more_senior"), 1).otherwise(0)).alias("p_received_same_parent_from_more_senior"),
            F.max(F.when(F.col("same_parent_from_less_senior"), 1).otherwise(0)).alias("p_received_same_parent_from_less_senior"),
            F.max(F.when(F.col("same_parent_from_equal_senior"), 1).otherwise(0)).alias("p_received_same_parent_from_equal_senior"),
            F.max(F.when(F.col("same_parent_from_less_equal_senior"), 1).otherwise(0)).alias("p_received_same_parent_from_less_equal_senior"),

            F.max(F.when(F.col("same_parent_metro"), 1).otherwise(0)).alias("p_received_same_parent_metro"),
            F.max(F.when(F.col("same_parent_metro_from_more_senior"), 1).otherwise(0)).alias("p_received_same_parent_metro_from_more_senior"),
            F.max(F.when(F.col("same_parent_metro_from_less_senior"), 1).otherwise(0)).alias("p_received_same_parent_metro_from_less_senior"),
            F.max(F.when(F.col("same_parent_metro_from_equal_senior"), 1).otherwise(0)).alias("p_received_same_parent_metro_from_equal_senior"),
            F.max(F.when(F.col("same_parent_metro_from_less_equal_senior"), 1).otherwise(0)).alias("p_received_same_parent_metro_from_less_equal_senior"),
        )
    )

    incoming_citation_patent_counts = (
        incoming_citation_patent_flags
        .groupBy("user_id_key", "year")
        .agg(
            F.countDistinct("citing_patent_id").alias("n_citing_patents_received_total"),

            F.sum("p_received_same_parent").alias("n_citing_patents_received_same_parent"),
            F.sum("p_received_same_parent_from_more_senior").alias("n_citing_patents_received_same_parent_from_more_senior"),
            F.sum("p_received_same_parent_from_less_senior").alias("n_citing_patents_received_same_parent_from_less_senior"),
            F.sum("p_received_same_parent_from_equal_senior").alias("n_citing_patents_received_same_parent_from_equal_senior"),
            F.sum("p_received_same_parent_from_less_equal_senior").alias("n_citing_patents_received_same_parent_from_less_equal_senior"),

            F.sum("p_received_same_parent_metro").alias("n_citing_patents_received_same_parent_metro"),
            F.sum("p_received_same_parent_metro_from_more_senior").alias("n_citing_patents_received_same_parent_metro_from_more_senior"),
            F.sum("p_received_same_parent_metro_from_less_senior").alias("n_citing_patents_received_same_parent_metro_from_less_senior"),
            F.sum("p_received_same_parent_metro_from_equal_senior").alias("n_citing_patents_received_same_parent_metro_from_equal_senior"),
            F.sum("p_received_same_parent_metro_from_less_equal_senior").alias("n_citing_patents_received_same_parent_metro_from_less_equal_senior"),
        )
    )


    citation_measures = (
        citation_link_counts
        .join(citation_patent_counts, on=["user_id_key", "year"], how="outer")
        .join(incoming_citation_link_counts, on=["user_id_key", "year"], how="outer")
        .join(incoming_citation_patent_counts, on=["user_id_key", "year"], how="outer")
    )

    # =========================================================================
    # 5. Collaboration measures
    # =========================================================================

    print("\n[5/8] Building and aggregating co-inventor collaboration measures ...")

    patent_i = (
        pat_inv
        .select(
            "patent_id",
            "year",
            F.col("user_id_key").alias("i_user_id_key")
        )
        .dropDuplicates(["patent_id", "year", "i_user_id_key"])
    )

    patent_j = (
        pat_inv
        .select(
            "patent_id",
            "year",
            F.col("user_id_key").alias("j_user_id_key")
        )
        .dropDuplicates(["patent_id", "year", "j_user_id_key"])
    )

    collab_links = (
        patent_i
        .join(patent_j, on=["patent_id", "year"], how="inner")
        .filter(F.col("i_user_id_key") != F.col("j_user_id_key"))
        .join(attrs_i, on=["i_user_id_key", "year"], how="left")
        .join(attrs_j, on=["j_user_id_key", "year"], how="left")
    )

    collab_links = (
        collab_links
        .withColumn(
            "same_parent",
            F.col("i_parent").isNotNull()
            & F.col("j_parent").isNotNull()
            & (F.col("i_parent") == F.col("j_parent"))
        )
        .withColumn(
            "same_parent_metro",
            F.col("same_parent")
            & F.col("i_metro").isNotNull()
            & F.col("j_metro").isNotNull()
            & (F.col("i_metro") == F.col("j_metro"))
        )
        .withColumn(
            "j_more_senior",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("j_seniority_floor") > F.col("i_seniority_floor"))
        )
        .withColumn(
            "j_less_equal_senior",
            F.col("i_seniority_floor").isNotNull()
            & F.col("j_seniority_floor").isNotNull()
            & (F.col("j_seniority_floor") <= F.col("i_seniority_floor"))
        )
        .withColumn("same_parent_more_senior", F.col("same_parent") & F.col("j_more_senior"))
        .withColumn("same_parent_less_equal_senior", F.col("same_parent") & F.col("j_less_equal_senior"))
        .withColumn("same_parent_metro_more_senior", F.col("same_parent_metro") & F.col("j_more_senior"))
        .withColumn("same_parent_metro_less_equal_senior", F.col("same_parent_metro") & F.col("j_less_equal_senior"))
    )

    check_keys(collab_links, "collab_links", do_count=False)

    collab_link_counts = (
        collab_links
        .groupBy(F.col("i_user_id_key").alias("user_id_key"), "year")
        .agg(
            F.count("*").alias("n_collab_links_total"),

            sum_flag(F.col("same_parent"), "n_collab_links_same_parent"),
            sum_flag(F.col("same_parent_more_senior"), "n_collab_links_same_parent_more_senior"),
            sum_flag(F.col("same_parent_less_equal_senior"), "n_collab_links_same_parent_less_equal_senior"),

            sum_flag(F.col("same_parent_metro"), "n_collab_links_same_parent_metro"),
            sum_flag(F.col("same_parent_metro_more_senior"), "n_collab_links_same_parent_metro_more_senior"),
            sum_flag(F.col("same_parent_metro_less_equal_senior"), "n_collab_links_same_parent_metro_less_equal_senior"),
        )
    )

    # Patent-level collaboration measures: whether each focal patent has at least
    # one colleague / senior colleague / less-equal senior colleague.
    collab_patent_flags = (
        collab_links
        .groupBy(
            F.col("i_user_id_key").alias("user_id_key"),
            "year",
            "patent_id"
        )
        .agg(
            F.max(F.lit(1)).alias("p_any_coinventor"),

            F.max(F.when(F.col("same_parent"), 1).otherwise(0)).alias("p_same_parent_collab"),
            F.max(F.when(F.col("same_parent_more_senior"), 1).otherwise(0)).alias("p_same_parent_more_senior_collab"),
            F.max(F.when(F.col("same_parent_less_equal_senior"), 1).otherwise(0)).alias("p_same_parent_less_equal_senior_collab"),

            F.max(F.when(F.col("same_parent_metro"), 1).otherwise(0)).alias("p_same_parent_metro_collab"),
            F.max(F.when(F.col("same_parent_metro_more_senior"), 1).otherwise(0)).alias("p_same_parent_metro_more_senior_collab"),
            F.max(F.when(F.col("same_parent_metro_less_equal_senior"), 1).otherwise(0)).alias("p_same_parent_metro_less_equal_senior_collab"),
        )
    )

    collab_patent_counts = (
        collab_patent_flags
        .groupBy("user_id_key", "year")
        .agg(
            F.countDistinct("patent_id").alias("n_patents_with_any_coinventor"),

            F.sum("p_same_parent_collab").alias("n_patents_with_same_parent_collab"),
            F.sum("p_same_parent_more_senior_collab").alias("n_patents_with_same_parent_more_senior_collab"),
            F.sum("p_same_parent_less_equal_senior_collab").alias("n_patents_with_same_parent_less_equal_senior_collab"),

            F.sum("p_same_parent_metro_collab").alias("n_patents_with_same_parent_metro_collab"),
            F.sum("p_same_parent_metro_more_senior_collab").alias("n_patents_with_same_parent_metro_more_senior_collab"),
            F.sum("p_same_parent_metro_less_equal_senior_collab").alias("n_patents_with_same_parent_metro_less_equal_senior_collab"),
        )
    )

    collab_measures = (
        collab_link_counts
        .join(collab_patent_counts, on=["user_id_key", "year"], how="outer")
    )

    # =========================================================================
    # 6. Join measures to panel
    # =========================================================================

    print("\n[6/8] Joining Step 6 measures back to panel ...")

    measures = (
        citation_measures
        .join(collab_measures, on=["user_id_key", "year"], how="outer")
    )

    final = (
        panel
        .join(measures, on=["user_id_key", "year"], how="left")
    )

    # Fill counts with zero.
    measure_cols = [c for c in measures.columns if c not in ["user_id_key", "year"]]

    count_fill = {c: 0 for c in measure_cols}
    if count_fill:
        final = final.fillna(count_fill)

    # -------------------------------------------------------------------------
    # Shares
    # -------------------------------------------------------------------------

    print("\n[7/8] Creating shares ...")

    # Citation link shares.
    cite_link_den = "n_cite_links_total"
    for c in [
        "same_parent",
        "same_parent_more_senior",
        "same_parent_less_equal_senior",
        "same_parent_metro",
        "same_parent_metro_more_senior",
        "same_parent_metro_less_equal_senior",
    ]:
        num = f"n_cite_links_{c}"
        out = f"share_cite_links_{c}"
        if num in final.columns:
            final = final.withColumn(out, safe_divide(num, cite_link_den))

    # Citation patent shares.
    cited_pat_den = "n_cited_patents_total"
    for c in [
        "same_parent",
        "same_parent_more_senior",
        "same_parent_less_equal_senior",
        "same_parent_metro",
        "same_parent_metro_more_senior",
        "same_parent_metro_less_equal_senior",
    ]:
        num = f"n_cited_patents_{c}"
        out = f"share_cited_patents_{c}"
        if num in final.columns:
            final = final.withColumn(out, safe_divide(num, cited_pat_den))

    # Incoming citation link shares over all incoming citation links.
    received_link_den = "n_cite_links_received_total"
    for c in [
        "same_parent",
        "same_parent_from_more_senior",
        "same_parent_from_less_senior",
        "same_parent_from_equal_senior",
        "same_parent_from_less_equal_senior",
        "same_parent_metro",
        "same_parent_metro_from_more_senior",
        "same_parent_metro_from_less_senior",
        "same_parent_metro_from_equal_senior",
        "same_parent_metro_from_less_equal_senior",
    ]:
        num = f"n_cite_links_received_{c}"
        out = f"share_cite_links_received_{c}"
        if num in final.columns and received_link_den in final.columns:
            final = final.withColumn(out, safe_divide(num, received_link_den))

    # Incoming citation link shares conditional on being from the same parent.
    same_parent_received_den = "n_cite_links_received_same_parent"
    for c in [
        "same_parent_from_more_senior",
        "same_parent_from_less_senior",
        "same_parent_from_equal_senior",
        "same_parent_from_less_equal_senior",
    ]:
        num = f"n_cite_links_received_{c}"
        out = f"share_cite_links_received_{c}_cond_same_parent"
        if num in final.columns and same_parent_received_den in final.columns:
            final = final.withColumn(out, safe_divide(num, same_parent_received_den))

    # Incoming citing-patent shares over all citing patents received.
    received_citing_pat_den = "n_citing_patents_received_total"
    for c in [
        "same_parent",
        "same_parent_from_more_senior",
        "same_parent_from_less_senior",
        "same_parent_from_equal_senior",
        "same_parent_from_less_equal_senior",
        "same_parent_metro",
        "same_parent_metro_from_more_senior",
        "same_parent_metro_from_less_senior",
        "same_parent_metro_from_equal_senior",
        "same_parent_metro_from_less_equal_senior",
    ]:
        num = f"n_citing_patents_received_{c}"
        out = f"share_citing_patents_received_{c}"
        if num in final.columns and received_citing_pat_den in final.columns:
            final = final.withColumn(out, safe_divide(num, received_citing_pat_den))

    # Collaboration link shares.
    collab_link_den = "n_collab_links_total"
    for c in [
        "same_parent",
        "same_parent_more_senior",
        "same_parent_less_equal_senior",
        "same_parent_metro",
        "same_parent_metro_more_senior",
        "same_parent_metro_less_equal_senior",
    ]:
        num = f"n_collab_links_{c}"
        out = f"share_collab_links_{c}"
        if num in final.columns:
            final = final.withColumn(out, safe_divide(num, collab_link_den))

    # Collaboration patent shares over total patents.
    # This captures: what share of this inventor's own patents involve colleagues?
    total_pat_den = "n_patents"
    for c in [
        "same_parent_collab",
        "same_parent_more_senior_collab",
        "same_parent_less_equal_senior_collab",
        "same_parent_metro_collab",
        "same_parent_metro_more_senior_collab",
        "same_parent_metro_less_equal_senior_collab",
    ]:
        num = f"n_patents_with_{c}"
        out = f"share_patents_with_{c}"
        if num in final.columns:
            final = final.withColumn(out, safe_divide(num, total_pat_den))

    # Collaboration patent shares over co-invented patents.
    # This captures: among collaborative patents, what share involve colleagues?
    coinv_den = "n_patents_with_any_coinventor"
    for c in [
        "same_parent_collab",
        "same_parent_more_senior_collab",
        "same_parent_less_equal_senior_collab",
        "same_parent_metro_collab",
        "same_parent_metro_more_senior_collab",
        "same_parent_metro_less_equal_senior_collab",
    ]:
        num = f"n_patents_with_{c}"
        out = f"share_coinvented_patents_with_{c}"
        if num in final.columns:
            final = final.withColumn(out, safe_divide(num, coinv_den))

    # Drop internal join key before writing.
    final = final.drop("user_id_key")

    # =========================================================================
    # 8. Write final
    # =========================================================================

    print("\n[8/8] Writing final Step 6 output ...")

    final = final.checkpoint(eager=True)

    print(f"[WRITE] tmp_out = {tmp_out}")

    (
        final.repartition(args.out_partitions)
             .write
             .mode("overwrite")
             .option("compression", "snappy")
             .parquet(tmp_out)
    )

    spark.stop()

    if args.no_publish:
        print("[INFO] --no-publish set. Final output left at tmp path:")
        print(f"       {tmp_out}")
    else:
        publish_output(tmp_out, args.out_dir)

    print("[DONE] Step 6 completed successfully.")


if __name__ == "__main__":
    main()
