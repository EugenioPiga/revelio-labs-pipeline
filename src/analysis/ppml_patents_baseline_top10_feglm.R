#!/usr/bin/env Rscript

###############################################################################
# ppml_baseline_top10_feglm.R
# Baseline PPML restricted to Top 10% inventors by lifetime patents
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
BASE_FILE <- file.path(OUT_DIR, "ppml_baseline_top10_feglm.rds")
SUMMARY_FILE <- file.path(OUT_DIR, "ppml_baseline_top10_feglm_summary.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
global_start <- Sys.time()

df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_city, year) %>%
  collect()

cat("[INFO] Data loaded:", nrow(df), "rows\n")

# ============================
# Compute lifetime patents and filter Top 10%
# ============================
cat("[INFO] Computing lifetime patents per inventor...\n")

lifetime_patents <- df %>%
  group_by(user_id) %>%
  summarise(total_patents = sum(n_patents, na.rm = TRUE), .groups = "drop")

p90_cutoff <- quantile(lifetime_patents$total_patents, 0.9, na.rm = TRUE)
cat("[INFO] 90th percentile cutoff for total patents:", p90_cutoff, "\n")

top10_ids <- lifetime_patents %>%
  filter(total_patents >= p90_cutoff) %>%
  pull(user_id)

df_top10 <- df %>%
  filter(user_id %in% top10_ids, !is.na(n_patents), !is.na(user_id), year >= 1990)

cat("[INFO] Filtered Top 10% inventors. Rows remaining:", nrow(df_top10), "\n")

# ============================
# Baseline PPML (alpaca::feglm)
# ============================
cat("[INFO] Running baseline PPML (Top 10% inventors, no tenure)...\n")
start_base <- Sys.time()
gc()
flush.console()

ppml_top10 <- alpaca::feglm(
  formula = n_patents ~ 1 | user_id + first_rcid + first_city + year,
  data = df_top10,
  family = poisson(link = "log"),
  control = alpaca::feglmControl(
    dev.tol = 1e-8,
    center.tol = 1e-8,
    iter.max = 100,
    trace = TRUE
  )
)

end_base <- Sys.time()
runtime_base <- as.numeric(difftime(end_base, start_base, units = "secs"))
cat("[INFO] Baseline Top10 PPML runtime:", runtime_base, "seconds\n")

# ============================
# Save model and summary
# ============================
saveRDS(ppml_top10, BASE_FILE)

sink(SUMMARY_FILE)
cat("=== Baseline PPML (Top 10% inventors, alpaca::feglm) ===\n")
print(summary(ppml_top10))
cat("\nRuntime (seconds):", runtime_base, "\n")
sink()
cat("[INFO] Summary saved to:", SUMMARY_FILE, "\n")

# ============================
# Extract and save Fixed Effects
# ============================
cat("[INFO] Extracting fixed effects...\n")

FE_DIR <- file.path(OUT_DIR, "ppml_baseline_top10_feglm_fe")
dir.create(FE_DIR, showWarnings = FALSE, recursive = TRUE)

fe_list <- alpaca::getFEs(ppml_top10)
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

df_base <- df_top10 %>%
  select(user_id, first_rcid, first_city, year, n_patents)

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

out_decomp <- file.path(OUT_DIR, "decomposition_joined_baseline_top10_feglm.csv")
write_csv(decomp, out_decomp)
cat("[INFO] Decomposition file saved to:", out_decomp, "\n")

# ============================
# Runtime summary
# ============================
global_end <- Sys.time()
cat("[INFO] Total runtime (seconds):", round(as.numeric(global_end - global_start), 2), "\n")
cat("[INFO] Baseline PPML (Top 10% inventors, alpaca::feglm) completed successfully.\n")
