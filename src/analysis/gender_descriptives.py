#!/usr/bin/env python3
import os
import duckdb
import pandas as pd

IN_DIR  = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR = "/home/epiga/revelio_labs/descriptives_gender"
os.makedirs(OUT_DIR, exist_ok=True)

PARQUET_GLOB = os.path.join(IN_DIR, "**", "*.parquet")  # recursive

con = duckdb.connect()
con.execute("PRAGMA threads=32;")          # adjust if you want
con.execute("PRAGMA enable_progress_bar;")

# --- Define a consistent sex label ---
# Prefer au_sex_predicted if it’s M/F-like; fallback to probs if needed.
SEX_CASE = """
CASE
  WHEN au_sex_predicted IS NULL THEN
    CASE
      WHEN au_m_prob IS NOT NULL AND au_f_prob IS NOT NULL THEN
        CASE WHEN au_m_prob >= au_f_prob THEN 'male' ELSE 'female' END
      ELSE 'unknown'
    END
  WHEN lower(cast(au_sex_predicted as varchar)) IN ('m','male') THEN 'male'
  WHEN lower(cast(au_sex_predicted as varchar)) IN ('f','female') THEN 'female'
  ELSE
    CASE
      WHEN au_m_prob IS NOT NULL AND au_f_prob IS NOT NULL THEN
        CASE WHEN au_m_prob >= au_f_prob THEN 'male' ELSE 'female' END
      ELSE 'unknown'
    END
END
"""

# -----------------------------
# A) Basic gender composition
# -----------------------------
q_counts = f"""
WITH inv AS (
  SELECT
    user_id,
    year,
    n_patents,
    n_us_citations,
    n_first_inventor,
    {SEX_CASE} AS sex
  FROM read_parquet('{PARQUET_GLOB}')
)
SELECT
  sex,
  COUNT(*) AS n_user_years,
  COUNT(DISTINCT user_id) AS n_users,
  AVG(CASE WHEN n_patents IS NULL THEN 0 ELSE n_patents END) AS avg_patents_per_user_year
FROM inv
GROUP BY sex
ORDER BY n_users DESC;
"""
df_counts = con.execute(q_counts).df()
df_counts.to_csv(os.path.join(OUT_DIR, "gender_counts.csv"), index=False)

# -----------------------------------------
# B) Productivity by gender (user-year)
# -----------------------------------------
q_prod_uy = f"""
WITH inv AS (
  SELECT
    user_id,
    year,
    COALESCE(n_patents,0) AS n_patents,
    COALESCE(n_us_citations,0) AS n_us_citations,
    COALESCE(n_first_inventor,0) AS n_first_inventor,
    {SEX_CASE} AS sex
  FROM read_parquet('{PARQUET_GLOB}')
)
SELECT
  sex,
  COUNT(*) AS n_user_years,
  COUNT(DISTINCT user_id) AS n_users,
  AVG(n_patents) AS mean_patents_uy,
  AVG(n_us_citations) AS mean_citations_uy,
  AVG(CASE WHEN n_patents > 0 THEN (n_first_inventor * 1.0) / n_patents ELSE NULL END) AS mean_first_inventor_share,
  quantile_cont(n_patents, 0.5) AS median_patents_uy,
  quantile_cont(n_us_citations, 0.5) AS median_citations_uy
FROM inv
GROUP BY sex
ORDER BY n_users DESC;
"""
df_prod_uy = con.execute(q_prod_uy).df()
df_prod_uy.to_csv(os.path.join(OUT_DIR, "gender_productivity_user_year.csv"), index=False)

# -----------------------------------------
# C) Productivity by gender (lifetime user)
# -----------------------------------------
q_prod_user = f"""
WITH inv AS (
  SELECT
    user_id,
    COALESCE(n_patents,0) AS n_patents,
    COALESCE(n_us_citations,0) AS n_us_citations,
    COALESCE(n_first_inventor,0) AS n_first_inventor,
    {SEX_CASE} AS sex
  FROM read_parquet('{PARQUET_GLOB}')
),
per_user AS (
  SELECT
    user_id,
    sex,
    SUM(n_patents) AS patents_total,
    SUM(n_us_citations) AS citations_total,
    SUM(n_first_inventor) AS first_inventor_total
  FROM inv
  GROUP BY user_id, sex
)
SELECT
  sex,
  COUNT(*) AS n_users,
  AVG(patents_total) AS mean_patents_per_user,
  AVG(citations_total) AS mean_citations_per_user,
  AVG(CASE WHEN patents_total > 0 THEN (first_inventor_total * 1.0)/patents_total ELSE NULL END) AS mean_first_inventor_share_user,
  quantile_cont(patents_total, 0.5) AS median_patents_per_user,
  quantile_cont(citations_total, 0.5) AS median_citations_per_user
FROM per_user
GROUP BY sex
ORDER BY n_users DESC;
"""
df_prod_user = con.execute(q_prod_user).df()
df_prod_user.to_csv(os.path.join(OUT_DIR, "gender_productivity_user_lifetime.csv"), index=False)

# ---------------------------------------------------------
# D) Distribution of male share across clusters (parent-year)
# ---------------------------------------------------------
# Change cluster_id to last_rcid if you want firm-year instead of parent-year.
q_male_share = f"""
WITH inv AS (
  SELECT
    user_id,
    year,
    last_parent_rcid AS cluster_id,
    {SEX_CASE} AS sex,
    COALESCE(n_patents,0) AS n_patents
  FROM read_parquet('{PARQUET_GLOB}')
  WHERE year IS NOT NULL AND last_parent_rcid IS NOT NULL
),
cluster_year AS (
  SELECT
    cluster_id,
    year,
    COUNT(DISTINCT user_id) AS n_inventors,
    AVG(CASE WHEN sex='male' THEN 1.0 WHEN sex='female' THEN 0.0 ELSE NULL END) AS male_share,
    SUM(n_patents) AS patents_total
  FROM inv
  GROUP BY cluster_id, year
)
SELECT *
FROM cluster_year
WHERE n_inventors >= 10;  -- min cluster size; tweak
"""
df_cluster = con.execute(q_male_share).df()
df_cluster.to_csv(os.path.join(OUT_DIR, "male_share_parent_year.csv"), index=False)

# Summary quantiles
q_male_share_summ = """
SELECT
  COUNT(*) AS n_cluster_years,
  AVG(male_share) AS mean_male_share,
  quantile_cont(male_share, 0.01) AS p01,
  quantile_cont(male_share, 0.05) AS p05,
  quantile_cont(male_share, 0.10) AS p10,
  quantile_cont(male_share, 0.25) AS p25,
  quantile_cont(male_share, 0.50) AS p50,
  quantile_cont(male_share, 0.75) AS p75,
  quantile_cont(male_share, 0.90) AS p90,
  quantile_cont(male_share, 0.95) AS p95,
  quantile_cont(male_share, 0.99) AS p99
FROM df_cluster
"""
# DuckDB can query pandas via replacement scan:
con.register("df_cluster", df_cluster)
df_cluster_summ = con.execute(q_male_share_summ).df()
df_cluster_summ.to_csv(os.path.join(OUT_DIR, "male_share_parent_year_summary.csv"), index=False)

print("[DONE] Wrote outputs to:", OUT_DIR)
