#!/usr/bin/env Rscript
###############################################################################
# Diagnose Missing first_rcid in inventor_year_merged
# Author: Eugenio — 2025-10-17
###############################################################################

library(arrow)
library(dplyr)
library(data.table)
library(readr)

INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/diagnostics"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

OUT_LOG <- file.path(OUT_DIR, "missing_first_rcid_summary.txt")

cat("[INFO] Reading parquet file (minimal columns)...\n")
df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, year, first_rcid, first_city, n_patents) %>%
  collect()

cat("[INFO] Data loaded:", nrow(df), "rows\n")

setDT(df)
df[, year := as.integer(year)]
df[, user_id := as.character(user_id)]
df[, first_rcid := as.character(first_rcid)]

sink(OUT_LOG)
cat("=== Missing first_rcid Diagnostics ===\n\n")

# 1️⃣ Overall missingness
cat("[1] Overall missing share of first_rcid:\n")
miss_share <- mean(is.na(df$first_rcid))
cat("Missing share:", round(100 * miss_share, 2), "%\n\n")

# 2️⃣ Missingness by year
cat("[2] Missing share by year:\n")
miss_by_year <- df[, .(missing_share = mean(is.na(first_rcid))), by = year][order(year)]
print(miss_by_year)

# 3️⃣ Missingness by user_id group (do they ever have a value?)
cat("\n[3] Inventors with all NA first_rcid vs partially NA:\n")
by_user <- df[, .(
  n_obs = .N,
  all_na = all(is.na(first_rcid)),
  any_na = any(is.na(first_rcid)),
  any_non_na = any(!is.na(first_rcid))
), by = user_id]

summary_stats <- by_user[, .(
  total_inventors = .N,
  all_na = sum(all_na),
  any_na = sum(any_na),
  any_non_na = sum(any_non_na)
)]

print(summary_stats)

# 4️⃣ Propagation test — does first_rcid stay constant for an inventor?
cat("\n[4] Propagation consistency (should be 1 unique firm per user):\n")
unique_counts <- df[!is.na(first_rcid), .N, by = .(user_id, first_rcid)]
num_firms_per_user <- unique_counts[, .N, by = user_id]
inconsistent_users <- num_firms_per_user[N > 1, .N]
cat("Inventors with >1 unique first_rcid:", inconsistent_users, "\n")
cat("Share of inconsistent:", round(100 * inconsistent_users / nrow(num_firms_per_user), 2), "%\n")

# 5️⃣ Top cities with missing first_rcid
cat("\n[5] Top cities with missing first_rcid:\n")
miss_city <- df[is.na(first_rcid), .N, by = first_city][order(-N)][1:15]
print(miss_city)

# 6️⃣ Patenting activity among missing vs non-missing
cat("\n[6] Mean n_patents by first_rcid missingness:\n")
activity <- df[, .(mean_patents = mean(n_patents, na.rm = TRUE)), by = .(is_missing = is.na(first_rcid))]
print(activity)

sink()
cat("[INFO] Diagnostics written to:", OUT_LOG, "\n")
