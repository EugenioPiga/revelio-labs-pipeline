#!/usr/bin/env Rscript
###############################################################################
# check_date_format.R
# Diagnostic script: inspect education variable formats and date anomalies
# Author: Eugenio — 2025-10-21
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
.libPaths(c(user_lib, .libPaths()))
packages <- c("arrow", "readr", "dplyr", "ggplot2", "scales", "lubridate")
invisible(lapply(packages, function(p)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
))

library(readr)
library(dplyr)
library(ggplot2)
library(scales)
library(arrow)
library(lubridate)

INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"

# ============================
# Load and inspect dataset
# ============================
cat("[INFO] Opening inventor_year_merged dataset...\n")
ds <- open_dataset(INPUT, format = "parquet")

cat("\n[INFO] Columns in inventor_year_merged:\n")
print(names(ds))

# Include all relevant variables of interest
cols_to_check <- c(
  "user_id", "year",
  "first_university", "first_degree", "first_field",
  "first_startdate_edu", "first_enddate_edu",
  "first_startdate_pos"
)

available_cols <- intersect(cols_to_check, names(ds))
cat("[INFO] Columns found in dataset:\n")
print(available_cols)

# Preview a small sample
cat("\n[INFO] Sample of education-related variables (first 20 rows):\n")
sample_data <- ds %>%
  select(all_of(available_cols)) %>%
  head(20) %>%
  collect()
print(sample_data)

cat("\n[INFO] Arrow schema for these columns:\n")
print(schema(ds)[available_cols])

# ============================
# Compute tenure: education first, then position if missing
# ============================
cat("\n[INFO] Computing tenure (education priority)...\n")

tenure_df <- ds %>%
  select(user_id, year, first_startdate_edu, first_startdate_pos) %>%
  collect() %>%
  mutate(
    edu_start_year = year(first_startdate_edu),
    pos_start_year = year(first_startdate_pos),

    tenure = case_when(
      !is.na(edu_start_year) ~ year - edu_start_year + 3,   # education-based (add ~3 years for degree duration)
      is.na(edu_start_year) & !is.na(pos_start_year) ~ year - pos_start_year,  # fallback: first position
      TRUE ~ NA_real_
    )
  )

# ----------------------------
# Unfiltered summary
# ----------------------------
tenure_unfiltered <- tenure_df %>%
  filter(!is.na(tenure)) %>%
  summarise(
    min_tenure  = min(tenure, na.rm = TRUE),
    max_tenure  = max(tenure, na.rm = TRUE),
    mean_tenure = mean(tenure, na.rm = TRUE),
    p95         = quantile(tenure, 0.95, na.rm = TRUE)
  )

cat("\n[INFO] Tenure summary (unfiltered):\n")
print(tenure_unfiltered)

# ============================
# Detailed inspection of 1900 education entries
# ============================
cat("\n[INFO] Extracting observations with education start year = 1900...\n")

# Load relevant education + identity variables
edu_cols <- c(
  "user_id", "year",
  "first_university", "first_degree", "first_field",
  "first_startdate_edu", "first_enddate_edu",
  "first_startdate_pos"
)

# Check which of these exist in your dataset
available_cols <- intersect(edu_cols, names(ds))

cat("[INFO] Columns found for education diagnostics:\n")
print(available_cols)

# Collect only the relevant columns
edu_df <- ds %>%
  select(all_of(available_cols)) %>%
  collect() %>%
  mutate(
    edu_start_year = year(first_startdate_edu)
  )

# Filter cases where the education start year equals 1900
edu_1900 <- edu_df %>%
  filter(!is.na(edu_start_year) & edu_start_year == 1900)

cat("[INFO] Found", nrow(edu_1900), "records with 1900-01-01 education start dates.\n")

# Show a few examples with full name
if (nrow(edu_1900) > 0) {
  cat("\n[INFO] Sample of 20 observations with 1900 education dates (including full name):\n\n")
  print(
    edu_1900 %>%
      arrange(desc(year)) %>%
      select(
        user_id,
        first_university, first_degree, first_field,
        first_startdate_edu, first_enddate_edu,
        first_startdate_pos
      ) %>%
      head(20)
  )
} else {
  cat("[INFO] ✅ No education entries with 1900 dates detected.\n")
}


# ----------------------------
# Filtered summary (0–45 yrs)
# ----------------------------
tenure_filtered <- tenure_df %>%
  filter(!is.na(tenure), tenure >= 0, tenure <= 45) %>%
  summarise(
    min_tenure  = min(tenure, na.rm = TRUE),
    max_tenure  = max(tenure, na.rm = TRUE),
    mean_tenure = mean(tenure, na.rm = TRUE),
    p95         = quantile(tenure, 0.95, na.rm = TRUE)
  )

cat("\n[INFO] Tenure summary (filtered 0–45 yrs):\n")
print(tenure_filtered)

cat("[INFO] Done. ✅\n")

