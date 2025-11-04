#!/usr/bin/env Rscript

###############################################################################
# ppml_patents_tenure_feglm.R
# PPML with tenure controls using alpaca::feglm (multi-FE)
# Author: Eugenio — 2025-10-21
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

base_pkgs <- c("arrow", "dplyr", "readr", "broom", "lubridate")
installed <- rownames(installed.packages())
for (pkg in base_pkgs) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

# Install remotes if needed
if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
}

# Install alpaca if missing
if (!requireNamespace("alpaca", quietly = TRUE)) {
  cat("[INFO] Installing alpaca from GitHub...\n")
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib, dependencies = TRUE)
}

# Load libraries
library(arrow)
library(dplyr)
library(readr)
library(broom)
library(lubridate)
library(alpaca)
cat("[DEBUG] alpaca loaded:", "alpaca" %in% loadedNamespaces(), "\n")

# ============================
# Config
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
OUT_FILE <- file.path(OUT_DIR, "ppml_covariates_feglm.rds")
SUMMARY_FILE <- file.path(OUT_DIR, "ppml_covariates_feglm_summary.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
global_start <- Sys.time()

df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_city, year,
         first_startdate_edu, first_startdate_pos) %>%
  collect()

cat("[INFO] Data loaded:", nrow(df), "rows\n")

# ============================
# Create tenure covariates
# ============================
cat("[INFO] Creating tenure controls...\n")

df <- df %>%
  mutate(
    edu_start_year = as.numeric(substr(first_startdate_edu, 1, 4)),
    pos_start_year = as.numeric(substr(first_startdate_pos, 1, 4)),

    # Step 1: initial tenure based on education (add 3 years to account for degree duration)
    tenure = year - edu_start_year + 3,

    # Step 2: replace with position-based tenure if education missing or unrealistic (>50 yrs)
    tenure = ifelse(is.na(tenure) | tenure > 50, year - pos_start_year, tenure),

    # Step 3: drop extreme or negative values
    tenure = ifelse(tenure > 50 | tenure < 0, NA, tenure),

    # Step 4: squared term
    tenure_sq = tenure^2
  ) %>%
  filter(!is.na(tenure))

cat("[INFO] Tenure controls created. Rows after filtering:", nrow(df), "\n")

# ============================
# Filter valid sample
# ============================
df <- df %>%
  filter(!is.na(n_patents), !is.na(user_id),
         !is.na(first_rcid), !is.na(first_city),
         !is.na(year), n_patents >= 0)

cat("[INFO] Final estimation sample:", nrow(df), "rows\n")

# ============================
# Run PPML (alpaca::feglm)
# ============================
cat("[INFO] Running PPML with tenure controls using alpaca::feglm...\n")
start_cov <- Sys.time()
gc()

ppml_cov <- alpaca::feglm(
  formula = n_patents ~ tenure + tenure_sq | user_id + first_rcid + first_city + year,
  data = df,
  family = poisson(link = "log"),
  control = alpaca::feglmControl(
    dev.tol = 1e-8,
    center.tol = 1e-8,
    iter.max = 100,
    trace = TRUE
  )
)

end_cov <- Sys.time()
runtime_cov <- as.numeric(difftime(end_cov, start_cov, units = "secs"))
cat("[INFO] Covariate PPML runtime:", runtime_cov, "seconds\n")

# ============================
# Print summary before saving
# ============================
cat("\n=== Model Summary (alpaca::feglm with tenure controls) ===\n")
print(summary(ppml_cov))
cat("==========================================================\n\n")

# ============================
# Save model and summary
# ============================
saveRDS(ppml_cov, OUT_FILE)

sink(SUMMARY_FILE)
cat("=== PPML (alpaca::feglm) with Tenure Controls ===\n")
print(summary(ppml_cov))
cat("\nRuntime (seconds):", runtime_cov, "\n")
sink()
cat("[INFO] Summary saved to:", SUMMARY_FILE, "\n")

# ============================
# Extract and save Fixed Effects
# ============================
cat("[INFO] Extracting fixed effects...\n")

FE_DIR <- file.path(OUT_DIR, "ppml_covariates_feglm_fe")
dir.create(FE_DIR, showWarnings = FALSE, recursive = TRUE)

fe_list <- alpaca::getFEs(ppml_cov)
for (fe_name in names(fe_list)) {
  fe_df <- tibble(level = names(fe_list[[fe_name]]), fe = as.numeric(fe_list[[fe_name]]))
  out_path <- file.path(FE_DIR, paste0("fe_", fe_name, ".csv"))
  write_csv(fe_df, out_path)
  cat("[INFO] Saved FE for", fe_name, "to", out_path, "\n")
}

# ============================
# Merge fixed effects into decomposition dataset
# ============================
cat("[INFO] Merging fixed effects into a single decomposition dataset...\n")

df_base <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, first_rcid, first_city, year, n_patents) %>%
  collect()

read_fe <- function(name, key) {
  path <- file.path(FE_DIR, paste0("fe_", name, ".csv"))
  if (file.exists(path)) {
    read_csv(path, show_col_types = FALSE) %>%
      rename(!!key := level, !!paste0("fe_", key) := fe)
  } else NULL
}

fe_user  <- read_fe("user_id", "user_id")
fe_rcid  <- read_fe("first_rcid", "first_rcid")
fe_city  <- read_fe("first_city", "first_city")
fe_year  <- read_fe("year", "year")

# Harmonize ID types
df_base <- df_base %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_city = as.character(first_city),
    year = as.character(year)
  )

if (!is.null(fe_user)) fe_user <- fe_user %>% mutate(user_id = as.character(user_id))
if (!is.null(fe_rcid)) fe_rcid <- fe_rcid %>% mutate(first_rcid = as.character(first_rcid))
if (!is.null(fe_city)) fe_city <- fe_city %>% mutate(first_city = as.character(first_city))
if (!is.null(fe_year)) fe_year <- fe_year %>% mutate(year = as.character(year))

decomp <- df_base
if (!is.null(fe_user))  decomp <- decomp %>% left_join(fe_user,  by = "user_id")
if (!is.null(fe_rcid))  decomp <- decomp %>% left_join(fe_rcid,  by = "first_rcid")
if (!is.null(fe_city))  decomp <- decomp %>% left_join(fe_city,  by = "first_city")
if (!is.null(fe_year))  decomp <- decomp %>% left_join(fe_year,  by = "year")

out_decomp <- file.path(OUT_DIR, "decomposition_joined_covariates_feglm.csv")
write_csv(decomp, out_decomp)
cat("[INFO] Decomposition file saved to:", out_decomp, "\n")

# ============================
# Runtime summary
# ============================
global_end <- Sys.time()
cat("[INFO] Total runtime (seconds):", round(as.numeric(global_end - global_start), 2), "\n")
cat("[INFO] PPML (alpaca::feglm) with tenure controls completed successfully.\n")
