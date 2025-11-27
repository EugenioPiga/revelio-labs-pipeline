#!/usr/bin/env Rscript

###############################################################################
# ppml_patents_tenure_feglm_top10.R
# PPML with tenure controls (90th percentile inventors only)
# Author: Eugenio — 2025-10-22
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

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
}
if (!requireNamespace("alpaca", quietly = TRUE)) {
  cat("[INFO] Installing alpaca from GitHub...\n")
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib, dependencies = TRUE)
}

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
OUT_FILE <- file.path(OUT_DIR, "ppml_covariates_feglm_top10.rds")
SUMMARY_FILE <- file.path(OUT_DIR, "ppml_covariates_feglm_top10_summary.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
global_start <- Sys.time()

df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_state, year,
         first_startdate_edu, first_startdate_pos, first_state, first_country) %>%
  collect()

# only keep US cities
df <- df %>%
  filter(first_country == "United States")

cat("[INFO] Data loaded:", nrow(df), "rows\n")

# ============================
# Restrict to top 10% inventors by lifetime patenting
# ============================
cat("[INFO] Computing total lifetime patents per inventor...\n")

inventor_totals <- df %>%
  group_by(user_id) %>%
  summarise(total_patents = sum(n_patents, na.rm = TRUE), .groups = "drop")

p90_cutoff <- quantile(inventor_totals$total_patents, 0.90, na.rm = TRUE)
cat("[INFO] 90th percentile patent threshold:", round(p90_cutoff, 2), "\n")

top_inventors <- inventor_totals %>%
  filter(total_patents >= p90_cutoff) %>%
  select(user_id)

df <- df %>% semi_join(top_inventors, by = "user_id")
cat("[INFO] Filtered to top 10% inventors. Remaining rows:", nrow(df), "\n")

# ============================
# Create tenure covariates
# ============================
cat("[INFO] Creating tenure controls...\n")

df <- df %>%
  mutate(
    edu_start_year = as.numeric(substr(first_startdate_edu, 1, 4)),
    pos_start_year = as.numeric(substr(first_startdate_pos, 1, 4)),

    # Step 1: start from education (add ~3 years for degree duration)
    tenure = year - edu_start_year + 3,

    # Step 2: replace with position-based if education missing or unrealistic
    tenure = ifelse(is.na(tenure) | tenure > 50, year - pos_start_year, tenure),

    # Step 3: drop extreme or negative values
    tenure = ifelse(tenure > 50 | tenure < 0, NA, tenure),

    tenure_sq = tenure^2
  ) %>%
  filter(!is.na(tenure))

cat("[INFO] Tenure controls created. Rows after filtering:", nrow(df), "\n")

# ============================
# Run PPML (alpaca::feglm)
# ============================
cat("[INFO] Running PPML for top 10% inventors using alpaca::feglm...\n")
start_cov <- Sys.time()
gc()

ppml_top10 <- alpaca::feglm(
  formula = n_patents ~ tenure + tenure_sq | user_id + first_rcid + first_state + year,
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
cat("[INFO] PPML runtime:", runtime_cov, "seconds\n")

cat("\n=== Model Summary (Top 10% Inventors, with Tenure Controls) ===\n")
print(summary(ppml_top10))
cat("===============================================================\n\n")

saveRDS(ppml_top10, OUT_FILE)
sink(SUMMARY_FILE)
cat("=== PPML (Top 10%) with Tenure Controls ===\n")
print(summary(ppml_top10))
cat("\nRuntime (seconds):", runtime_cov, "\n")
sink()
cat("[INFO] Summary saved to:", SUMMARY_FILE, "\n")

# ============================
# Save Fixed Effects
# ============================
cat("[INFO] Extracting fixed effects...\n")

FE_DIR <- file.path(OUT_DIR, "ppml_covariates_feglm_top10_fe")
dir.create(FE_DIR, showWarnings = FALSE, recursive = TRUE)

fe_list <- alpaca::getFEs(ppml_top10)
for (fe_name in names(fe_list)) {
  fe_df <- tibble(level = names(fe_list[[fe_name]]), fe = as.numeric(fe_list[[fe_name]]))
  out_path <- file.path(FE_DIR, paste0("fe_", fe_name, ".csv"))
  write_csv(fe_df, out_path)
  cat("[INFO] Saved FE for", fe_name, "to", out_path, "\n")
}

# ============================
# Merge fixed effects and predicted values into decomposition dataset
# ============================
cat("[INFO] Merging fixed effects and predicted values into decomposition dataset...\n")

y_hat <- predict(ppml_top10, type = "response")
n_pred <- length(y_hat)
n_obs  <- nrow(df)
cat("[DEBUG] Predictions generated:", n_pred, "for", n_obs, "observations in df\n")

# Retrieve the actual estimation sample from alpaca object
if (!is.null(ppml_top10$model)) {
  df_est <- ppml_top10$model
} else if (!is.null(ppml_top10$data)) {
  df_est <- ppml_top10$data
} else {
  stop("[ERROR] Cannot locate estimation sample inside Alpaca object.")
}

n_est <- nrow(df_est)
cat("[DEBUG] Estimation sample rows:", n_est, "\n")

if (n_pred != n_est) {
  stop("[ERROR] Prediction vector length does not match estimation sample — something is inconsistent.")
}

# Attach predictions to the estimation sample
df_est$y_hat <- y_hat

# ✅ Harmonize join key types before merging (fix for factor/integer mismatch)
cat("[INFO] Harmonizing key variable types before merging predictions...\n")

df <- df %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_state = as.character(first_state),
    year = as.character(year)
  )

df_est <- df_est %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_state = as.character(first_state),
    year = as.character(year)
  )

# Merge predictions back to full dataset
df <- df %>%
  left_join(
    df_est %>% select(user_id, first_rcid, first_state, year, y_hat),
    by = c("user_id", "first_rcid", "first_state", "year")
  )

cat("[INFO] Merged predictions back into full dataset.\n")
cat("[DEBUG] Non-missing y_hat:", sum(!is.na(df$y_hat)), "of", nrow(df), "\n")

# Step 2: Keep the relevant subset
df_base <- df %>%
  select(user_id, first_rcid, first_state, year, n_patents, y_hat)

# Step 3: Read and merge fixed effects
read_fe <- function(name, key) {
  path <- file.path(FE_DIR, paste0("fe_", name, ".csv"))
  if (file.exists(path)) {
    read_csv(path, show_col_types = FALSE) %>%
      rename(!!key := level, !!paste0("fe_", key) := fe)
  } else NULL
}

fe_user  <- read_fe("user_id", "user_id")
fe_rcid  <- read_fe("first_rcid", "first_rcid")
fe_state  <- read_fe("first_state", "first_state")
fe_year  <- read_fe("year", "year")

# Step 4: Harmonize variable types for joining
df_base <- df_base %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_state = as.character(first_state),
    year = as.character(year)
  )

if (!is.null(fe_user)) fe_user <- fe_user %>% mutate(user_id = as.character(user_id))
if (!is.null(fe_rcid)) fe_rcid <- fe_rcid %>% mutate(first_rcid = as.character(first_rcid))
if (!is.null(fe_state)) fe_state <- fe_state %>% mutate(first_state = as.character(first_state))
if (!is.null(fe_year)) fe_year <- fe_year %>% mutate(year = as.character(year))

# Step 5: Merge all fixed effects and predictions
decomp <- df_base
if (!is.null(fe_user))  decomp <- decomp %>% left_join(fe_user,  by = "user_id")
if (!is.null(fe_rcid))  decomp <- decomp %>% left_join(fe_rcid,  by = "first_rcid")
if (!is.null(fe_state))  decomp <- decomp %>% left_join(fe_state,  by = "first_state")
if (!is.null(fe_year))  decomp <- decomp %>% left_join(fe_year,  by = "year")

# Step 6: Save the unified decomposition file (includes y_hat)
out_decomp <- file.path(OUT_DIR, "decomposition_joined_covariates_feglm_top10.csv")
write_csv(decomp, out_decomp)
cat("[INFO] Decomposition dataset (with y_hat) saved to:", out_decomp, "\n")

# ============================
# Runtime summary
# ============================
global_end <- Sys.time()
cat("[INFO] Total runtime (seconds):", round(as.numeric(global_end - global_start), 2), "\n")
cat("[INFO] PPML (Top 10% inventors) with tenure controls completed successfully.\n")
