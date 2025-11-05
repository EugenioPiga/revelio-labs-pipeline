#!/usr/bin/env Rscript

###############################################################################
# ppml_patents_baseline_feglm.R
# Baseline PPML using alpaca::feglm (multi-FE)
# Author: Eugenio
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

# Try to install alpaca if missing
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
#OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
OUT_DIR <- "/home/gps-yuhei/revelio_labs/output/regressions"
#BASE_FILE <- file.path(OUT_DIR, "ppml_baseline_feglm.rds")
BASE_FILE <- file.path(OUT_DIR, "ppml_baseline_feglm_test.rds")
#SUMMARY_FILE <- file.path(OUT_DIR, "ppml_baseline_feglm_summary.txt")
SUMMARY_FILE <- file.path(OUT_DIR, "ppml_baseline_feglm_summary_test.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
global_start <- Sys.time()

df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_city, first_country, year) %>%
  collect()

cat("[INFO] Data loaded:", nrow(df), "rows\n")

# ============================
# Filter sample
# ============================
df <- df %>%
  filter(!is.na(n_patents), !is.na(user_id), year >= 1990)

# only keep US cities
df <- df %>%
  filter(first_country == "United States")

# keep random 1% of user_id
set.seed(12345)
unique_users <- unique(df$user_id)
sampled_users <- sample(unique_users, size = ceiling(0.01 * length(unique_users)))
df <- df %>%
  filter(user_id %in% sampled_users)

table(df$year)

# print out number of unique cities
cat("[INFO] Number of unique first_city after filtering:", length(unique(df$first_city)), "\n")

cat("[INFO] Filtered sample (year >= 1990). Rows remaining:", nrow(df), "\n")

# ============================
# Baseline PPML (alpaca::feglm)
# ============================
cat("[INFO] Running baseline PPML with alpaca::feglm...\n")
start_base <- Sys.time()

# ---- Memory & threading notes ----
# alpaca::feglm does not use explicit thread control;
# it handles memory internally and is often more stable for large FE sets.
gc()
flush.console()
packageVersion("alpaca")

ppml_base <- alpaca::feglm(
  formula = n_patents ~ 1 | user_id + first_rcid + first_city + year,
  data = df,
  family = poisson(link = "log"),
  control = alpaca::feglmControl(
    dev.tol = 1e-8,
    center.tol = 1e-8,
    iter.max = 100,
    trace = TRUE   # prints iteration progress
  )
)

end_base <- Sys.time()
runtime_base <- as.numeric(difftime(end_base, start_base, units = "secs"))
cat("[INFO] Baseline PPML runtime:", runtime_base, "seconds\n")

# ============================
# Save model and summary
# ============================
saveRDS(ppml_base, BASE_FILE)

sink(SUMMARY_FILE)
cat("=== Baseline PPML (alpaca::feglm) ===\n")
print(summary(ppml_base))
cat("\nRuntime (seconds):", runtime_base, "\n")
sink()
cat("[INFO] Summary saved to:", SUMMARY_FILE, "\n")

# ============================
# Extract and save Fixed Effects
# ============================
cat("[INFO] Extracting fixed effects...\n")

FE_DIR <- file.path(OUT_DIR, "ppml_baseline_feglm_fe")
dir.create(FE_DIR, showWarnings = FALSE, recursive = TRUE)

fe_list <- alpaca::getFEs(ppml_base)

for (fe_name in names(fe_list)) {
  fe_df <- tibble(level = names(fe_list[[fe_name]]), fe = as.numeric(fe_list[[fe_name]]))
  out_path <- file.path(FE_DIR, paste0("fe_", fe_name, ".csv"))
  write_csv(fe_df, out_path)
  cat("[INFO] Saved FE for", fe_name, "to", out_path, "\n")
}

# ============================
# Merge fixed effects into a decomposition dataset
# ============================
cat("[INFO] Merging fixed effects into a single decomposition dataset...\n")

#df_base <- open_dataset(INPUT, format = "parquet") %>%
#  select(user_id, first_rcid, first_city, year, n_patents) %>%
#  collect()
df_base <- df

read_fe <- function(name, key) {
  path <- file.path(FE_DIR, paste0("fe_", name, ".csv"))
  if (file.exists(path)) {
    read_csv(path, show_col_types = FALSE) %>%
      rename(!!key := level, !!paste0("fe_", key) := fe)
  } else {
    NULL
  }
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

# Merge all FEs back
decomp <- df_base
if (!is.null(fe_user))  decomp <- decomp %>% left_join(fe_user,  by = "user_id")
if (!is.null(fe_rcid))  decomp <- decomp %>% left_join(fe_rcid,  by = "first_rcid")
if (!is.null(fe_city))  decomp <- decomp %>% left_join(fe_city,  by = "first_city")
if (!is.null(fe_year))  decomp <- decomp %>% left_join(fe_year,  by = "year")

# Save decomposition
out_decomp <- file.path(OUT_DIR, "decomposition_joined_baseline_feglm.csv")
write_csv(decomp, out_decomp)
cat("[INFO] Decomposition file saved to:", out_decomp, "\n")

# ============================
# Runtime summary
# ============================
global_end <- Sys.time()
cat("[INFO] Total runtime (seconds):", round(as.numeric(global_end - global_start), 2), "\n")
cat("[INFO] Baseline PPML (alpaca::feglm) and decomposition completed successfully.\n")
