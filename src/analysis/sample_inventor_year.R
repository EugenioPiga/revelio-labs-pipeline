#!/usr/bin/env Rscript
###############################################################################
# SAMPLE 1% OF INVENTOR_YEAR_MERGED AND SAVE LOCALLY
# Author: Eugenio
# Purpose: Extract 1% random sample for local analysis
###############################################################################

options(bitmapType = "cairo")
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","data.table","dplyr","readr")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
suppressPackageStartupMessages(lapply(pkgs, library, character.only = TRUE))

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
#OUT_DIR   <- "/home/epiga/revelio_labs/output"
OUT_DIR   <- "/home/gps-yuhei/revelio_labs/output"
OUT_FILE  <- file.path(OUT_DIR, "inventor_year_sample_1pct.csv.gz")
OUT_RDS   <- file.path(OUT_DIR, "inventor_year_sample_1pct.rds")
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

SAMPLE_FRAC <- 0.01  # 1% sample
set.seed(42)

# =========================
# Load and sample
# =========================
cat("[INFO] Opening large dataset (Arrow streaming)...\n")
ds <- arrow::open_dataset(INPUT_DIR, format = "parquet")

# Arrow sampling directly on the dataset
cat("[INFO] Identifying unique user IDs...\n")
user_ids <- ds %>%
  dplyr::filter(!is.na(user_id)) %>%
  dplyr::select(user_id) %>%
  dplyr::distinct() %>%
  dplyr::collect()
total_users <- nrow(user_ids)
cat("[INFO] Total unique user IDs:", total_users, "\n")

cat("[INFO] Sampling 1% of user IDs...\n")
sample_size <- max(1, ceiling(total_users * SAMPLE_FRAC))
sampled_user_ids <- user_ids %>%
  dplyr::sample_n(sample_size)
cat("[INFO] Sampled user IDs:", sample_size, "\n")

cat("[INFO] Collecting observations for sampled user IDs...\n")
sample_df <- ds %>%
  dplyr::filter(user_id %in% sampled_user_ids$user_id) %>%
  dplyr::collect()

cat("[INFO] Sample collected. Observations:", nrow(sample_df), "\n")

# =========================
# Save sampled data
# =========================
cat("[INFO] Writing compressed CSV...\n")
readr::write_csv(sample_df, OUT_FILE)
cat("[INFO] ✅ Saved 1% sample to:", OUT_FILE, "\n")

cat("[INFO] Writing RDS...\n")
saveRDS(sample_df, OUT_RDS)
cat("[INFO] ✅ Saved RDS to:", OUT_RDS, "\n")

cat("[INFO] You can now download them to your local computer using:\n")
cat("scp epiga@login.sscsd.ucsd.edu:", OUT_FILE, " ./\n")
cat("scp epiga@login.sscsd.ucsd.edu:", OUT_RDS, " ./\n")
