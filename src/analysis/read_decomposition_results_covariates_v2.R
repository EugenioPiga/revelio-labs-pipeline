#!/usr/bin/env Rscript

# ===================================================
# Build decomposition_joined_covariates.csv
# and produce scatter plots safely on SSRDE
# ===================================================

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("readr", "dplyr", "arrow", "ggplot2", "lubridate")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(readr)
library(dplyr)
library(arrow)
library(ggplot2)
library(lubridate)

# ----------------------------
# Paths
# ----------------------------
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
FE_DIR  <- file.path(OUT_DIR, "ppml_cov_3fe_fe")
DATA_PATH <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_FILE  <- file.path(OUT_DIR, "decomposition_joined_covariates.csv")

# ----------------------------
# Load original inventor data
# ----------------------------
cat("[INFO] Reading inventor_year_merged (subset of needed vars)...\n")

df <- open_dataset(DATA_PATH, format = "parquet") %>%
  select(user_id, first_rcid, first_city, year, n_patents) %>%
  collect()

cat("[INFO] Loaded", nrow(df), "rows\n")

# ----------------------------
# Read all fixed effects
# ----------------------------
cat("[INFO] Reading fixed effects...\n")

fe_user  <- read_csv(file.path(FE_DIR, "fe_user_id.csv"), show_col_types = FALSE) %>%
  rename(user_id = level, fe_user = fe)
fe_firm  <- read_csv(file.path(FE_DIR, "fe_first_rcid.csv"), show_col_types = FALSE) %>%
  rename(first_rcid = level, fe_firm = fe)
fe_city  <- read_csv(file.path(FE_DIR, "fe_first_city.csv"), show_col_types = FALSE) %>%
  rename(first_city = level, fe_city = fe)
fe_year  <- read_csv(file.path(FE_DIR, "fe_year.csv"), show_col_types = FALSE) %>%
  rename(year = level, fe_year = fe)

fe_univ_path <- file.path(FE_DIR, "fe_first_university.csv")
if (file.exists(fe_univ_path)) {
  fe_univ <- read_csv(fe_univ_path, show_col_types = FALSE) %>%
    rename(first_university = level, fe_university = fe)
  has_univ <- TRUE
  cat("[INFO] Found university fixed effects\n")
} else {
  has_univ <- FALSE
  cat("[INFO] No university FE found; skipping\n")
}

# ----------------------------
# Harmonize ID types
# ----------------------------
cat("[INFO] Harmonizing ID variable types before merge...\n")

df <- df %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_city = as.character(first_city),
    year = as.character(year)
  )

fe_user <- fe_user %>% mutate(user_id = as.character(user_id))
fe_firm <- fe_firm %>% mutate(first_rcid = as.character(first_rcid))
fe_city <- fe_city %>% mutate(first_city = as.character(first_city))
fe_year <- fe_year %>% mutate(year = as.character(year))
if (exists("fe_univ")) fe_univ <- fe_univ %>% mutate(first_university = as.character(first_university))

# ----------------------------
# Merge all FEs back to base data
# ----------------------------
cat("[INFO] Merging FEs with main data...\n")

decomp <- df %>%
  left_join(fe_user,  by = "user_id") %>%
  left_join(fe_firm,  by = "first_rcid") %>%
  left_join(fe_city,  by = "first_city") %>%
  left_join(fe_year,  by = "year")

if (has_univ) decomp <- decomp %>% left_join(fe_univ, by = "first_university")

cat("[INFO] Final merged dataset:", nrow(decomp), "rows\n")

# ----------------------------
# Save decomposition dataset
# ----------------------------
write_csv(decomp, OUT_FILE)
cat("[INFO] Decomposition file saved to:\n", OUT_FILE, "\n")

# ----------------------------
# Quick diagnostic
# ----------------------------
cat("[INFO] Summary of fixed effects (non-missing counts):\n")
print(
  summarise(decomp,
    nonmiss_user = sum(!is.na(fe_user)),
    nonmiss_firm = sum(!is.na(fe_firm)),
    nonmiss_city = sum(!is.na(fe_city)),
    nonmiss_year = sum(!is.na(fe_year))
  )
)

# ============================
# Scatter plots (safe on SSRDE)
# ============================
cat("[INFO] Generating scatter plots (sampled)...\n")

PLOT_DIR <- file.path(OUT_DIR, "plots_covariates")
dir.create(PLOT_DIR, showWarnings = FALSE, recursive = TRUE)

# Load a small sample for plotting
df_cov <- open_dataset(DATA_PATH, format = "parquet") %>%
  select(user_id, n_patents, year, first_city, first_startdate_edu) %>%
  sample_frac(0.01) %>%   # <- 1% sample (~5 million rows)
  collect() %>%
  mutate(
    edu_start_year = as.numeric(format(first_startdate_edu, "%Y")),
    years_since_edu = year - edu_start_year
  ) %>%
  filter(!is.na(years_since_edu), years_since_edu >= 0, years_since_edu <= 60)

# --- A) Binned scatter ---
nbins <- 25
brks <- quantile(df_cov$years_since_edu, probs = seq(0, 1, length.out = nbins + 1), na.rm = TRUE)
df_cov$bin <- cut(df_cov$years_since_edu, breaks = unique(brks), include.lowest = TRUE)

bin_stats <- df_cov %>%
  group_by(bin) %>%
  summarise(
    age_mid = mean(years_since_edu, na.rm = TRUE),
    y_mean  = mean(n_patents, na.rm = TRUE),
    n = n(),
    .groups = "drop"
  )

gA <- ggplot(bin_stats, aes(x = age_mid, y = y_mean)) +
  geom_point() + geom_line() +
  labs(
    title = "Binned Scatter: Patents vs Years Since Education (1% sample)",
    x = "Years since education start",
    y = "Mean patents per inventor-year"
  ) +
  theme_minimal()

ggsave(file.path(PLOT_DIR, "scatter_binned_years_since_edu.png"), gA, width = 7, height = 5, dpi = 200)

# --- B) City-year weighted scatter ---
cityyr <- df_cov %>%
  group_by(first_city, year) %>%
  summarise(
    n_inventors = n_distinct(user_id),
    y_mean = mean(n_patents, na.rm = TRUE),
    x_mean = mean(years_since_edu, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(n_inventors >= 50)

gB <- ggplot(cityyr, aes(x = x_mean, y = y_mean, size = n_inventors)) +
  geom_point(alpha = 0.6) +
  geom_smooth(method = "lm", se = FALSE, linewidth = 0.6) +
  labs(
    title = "City-Year Weighted Scatter (≥50 inventors, 1% sample)",
    x = "Average years since education",
    y = "Average patents per inventor-year",
    size = "# inventors"
  ) +
  theme_minimal()

ggsave(file.path(PLOT_DIR, "scatter_cityyear_weighted.png"), gB, width = 7, height = 5, dpi = 200)

cat("[INFO] Scatter plots saved in:", PLOT_DIR, "\n")
