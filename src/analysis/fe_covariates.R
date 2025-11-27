#!/usr/bin/env Rscript

###############################################################################
# fe_covariates_2018_metro.R
# Correlation and FE-style plots: Inventor FE vs covariates (2018, metro)
# - Uses unified FE decomposition (metro-level)
# - Merges with inventor_year_merged
# - Filters: US, Top 10% inventors (lifetime patents), year = 2018
# - Constructs tenure, education rank + dummies
# - Computes correlations (inventor-level) and FE regressions (metro-level)
# - Produces FE-style bubble plots (metro-level)
# Author: Eugenio — 2025-11-xx
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

base_pkgs <- c("arrow", "dplyr", "readr", "ggplot2", "ggrepel", "broom")
installed <- rownames(installed.packages())
for (pkg in base_pkgs) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(arrow)
library(dplyr)
library(readr)
library(ggplot2)
library(ggrepel)
library(broom)

# ============================
# Config
# ============================
INPUT_INV <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
FE_PATH   <- "/home/epiga/revelio_labs/output/regressions/decomposition_joined_covariates_feglm_top10_nofirmFE_metro_area.csv"
OUT_DIR   <- "/home/epiga/revelio_labs/output/cluster_stats"
YEAR      <- 2018

if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

cat("[INFO] Starting FE–covariate analysis for year", YEAR, "\n")
cat("[INFO] Input inventor dataset:", INPUT_INV, "\n")
cat("[INFO] FE decomposition file:", FE_PATH, "\n")

# ============================
# Load inventor-year data
# ============================
ds <- open_dataset(INPUT_INV, format = "parquet")

df <- ds %>%
  filter(first_country == "United States") %>%
  collect()

cat("[INFO] Data loaded (US only):", nrow(df), "rows\n")

# ============================
# Restrict to top 10% inventors by lifetime patenting
# ============================
cat("[INFO] Computing total lifetime patents per inventor...\n")

inventor_totals <- df %>%
  group_by(user_id) %>%
  summarise(
    total_patents = sum(n_patents, na.rm = TRUE),
    .groups = "drop"
  )

p90_cutoff <- quantile(inventor_totals$total_patents, 0.90, na.rm = TRUE)
cat("[INFO] 90th percentile patent threshold:", round(p90_cutoff, 2), "\n")

top_inventors <- inventor_totals %>%
  filter(total_patents >= p90_cutoff) %>%
  select(user_id)

df <- df %>%
  semi_join(top_inventors, by = "user_id")

cat("[INFO] Filtered to top 10% inventors. Remaining rows:", nrow(df), "\n")

# ============================
# Create tenure covariates (tenure as age proxy)
# ============================
cat("[INFO] Creating tenure controls...\n")

df <- df %>%
  mutate(
    edu_start_year = as.numeric(substr(first_startdate_edu, 1, 4)),
    pos_start_year = as.numeric(substr(first_startdate_pos, 1, 4)),

    # Step 1: from education (add ~3 years for degree duration)
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
# Restrict to target year (2018)
# ============================
df <- df %>%
  filter(year == YEAR)

cat("[INFO] Rows for year", YEAR, "after tenure filter:", nrow(df), "\n")

# ============================
# Education Rank Construction
# ============================
cat("[INFO] Computing education rank...\n")

df <- df %>%
  mutate(
    education_rank = case_when(
      last_degree == "Doctor" ~ 4,
      last_degree %in% c("Master", "MBA") ~ 3,
      last_degree == "Bachelor" ~ 2,
      last_degree %in% c("Associate", "High School") ~ 1,
      last_degree %in% c("empty", NA) ~ NA_real_,
      TRUE ~ NA_real_
    ),
    edu_phd      = as.integer(education_rank == 4),
    edu_master   = as.integer(education_rank == 3),
    edu_bachelor = as.integer(education_rank == 2),
    edu_low      = as.integer(education_rank == 1)
  )

cat("[INFO] Education rank and dummies created.\n")

# ============================
# Load FE decomposition and merge
# ============================
cat("[INFO] Loading Inventor FE from decomposition and merging...\n")

fe_df <- read_csv(FE_PATH, show_col_types = FALSE) %>%
  select(user_id, year, fe_user_id) %>%
  distinct() %>%
  mutate(
    user_id = as.character(user_id),
    year    = as.character(year)
  )

df <- df %>%
  mutate(
    user_id = as.character(user_id),
    year    = as.character(year)
  ) %>%
  left_join(fe_df, by = c("user_id", "year"))

cat("[INFO] FE merged. Rows with FE:", sum(!is.na(df$fe_user_id)), "\n")

# ============================
# Individual-level correlations (micro)
# ============================
cat("[INFO] Computing inventor-level correlations FE ~ covariates...\n")

df_corr <- df %>%
  select(
    fe_user_id,
    tenure,
    avg_seniority,
    education_rank,
    edu_phd, edu_master, edu_bachelor, edu_low
  ) %>%
  distinct()

corr_vars <- names(df_corr)[-1]  # all except FE

corr_results <- lapply(corr_vars, function(v) {
  tibble(
    variable    = v,
    correlation = cor(df_corr$fe_user_id, df_corr[[v]], use = "complete.obs")
  )
}) %>% bind_rows()

cat("\n========== Inventor-level Correlations: FE vs Covariates ==========\n")
print(corr_results, n = Inf, width = Inf)

corr_csv <- file.path(OUT_DIR, "correlations_FE_covariates_2018_metro.csv")
write_csv(corr_results, corr_csv)
cat("[INFO] Saved correlations CSV:", corr_csv, "\n")

corr_txt <- file.path(OUT_DIR, "correlations_FE_covariates_2018_metro.txt")
sink(corr_txt)
cat("=========================================\n")
cat(" Inventor-level Correlations: FE vs Covariates\n")
cat(" Year:", YEAR, "\n")
cat("=========================================\n\n")
print(corr_results, n = Inf, width = Inf)
sink()
cat("[INFO] Saved correlations TXT:", corr_txt, "\n")


# ============================
# Individual-level regressions (micro)
# ============================
cat("[INFO] Running individual-level FE ~ covariate regressions...\n")

micro_covars <- c(
  "tenure",
  "avg_seniority",
  "education_rank",
  "edu_phd",
  "edu_master",
  "edu_bachelor",
  "edu_low"
)

run_micro_regressions <- function(df) {
  lapply(micro_covars, function(v) {
    form <- as.formula(paste0("fe_user_id ~ ", v))
    mod  <- lm(form, data = df)

    broom::tidy(mod) %>%
      filter(term == v) %>%
      mutate(covariate = v) %>%
      select(covariate, estimate, std.error, statistic, p.value)
  }) %>% bind_rows()
}

reg_FE_micro <- run_micro_regressions(df)

# Save tables
micro_txt <- file.path(OUT_DIR, "regression_FE_covariates_micro_2018.txt")
micro_csv <- file.path(OUT_DIR, "regression_FE_covariates_micro_2018.csv")

sink(micro_txt)
cat("=========================================\n")
cat(" Individual-level FE Regression Table (micro)\n")
cat(" Year:", YEAR, "\n")
cat("=========================================\n\n")
print(reg_FE_micro, n = Inf, width = Inf)
sink()

write_csv(reg_FE_micro, micro_csv)

cat("[INFO] Saved micro regression tables:\n")
cat("  TXT:", micro_txt, "\n")
cat("  CSV:", micro_csv, "\n")


# ============================
# Micro-level plots (continuous + discrete)
# ============================
cat("[INFO] Generating individual-level micro plots...\n")

# ---------
# DEFINE which vars are continuous vs discrete
# ---------
continuous_vars <- c("tenure", "avg_seniority")
discrete_vars   <- c("education_rank", "edu_phd", "edu_master", "edu_bachelor", "edu_low")

# ---------
# 1. BINNED SCATTER for continuous variables
# ---------
make_binned_scatter <- function(df, xvar, yvar = "fe_user_id",
                                bins = 50, tag = "metro_micro") {

  df2 <- df %>%
    mutate(bin = ntile(.data[[xvar]], bins)) %>%
    group_by(bin) %>%
    summarise(
      x = mean(.data[[xvar]], na.rm = TRUE),
      y = mean(.data[[yvar]], na.rm = TRUE),
      .groups = "drop"
    )

  p <- ggplot(df2, aes(x = x, y = y)) +
    geom_point(size = 2, alpha = 0.7) +
    geom_smooth(method = "lm", se = FALSE, color = "red", linewidth = 0.9) +
    theme_minimal(base_size = 14) +
    labs(
      title = paste0("Binned Scatter: FE vs ", xvar, " (", YEAR, ")"),
      x = xvar,
      y = "FE (user-level)"
    )

  out_file <- file.path(OUT_DIR, paste0("binned_FE_", xvar, "_", tag, "_", YEAR, ".png"))
  ggsave(out_file, p, width = 7, height = 5)
  cat("[INFO] Saved binned scatter:", out_file, "\n")
}

# ---------
# 2. GROUP-MEANS PLOT for discrete variables
# ---------
make_groupmeans_plot <- function(df, xvar, yvar = "fe_user_id",
                                 tag = "metro_micro") {

  df2 <- df %>%
    group_by(.data[[xvar]]) %>%
    summarise(
      y = mean(.data[[yvar]], na.rm = TRUE),
      se = sd(.data[[yvar]], na.rm = TRUE) / sqrt(n()),
      .groups = "drop"
    )

  p <- ggplot(df2, aes(x = .data[[xvar]], y = y)) +
    geom_point(size = 3) +
    geom_errorbar(aes(ymin = y - 1.96 * se, ymax = y + 1.96 * se),
                  width = 0.15) +
    geom_smooth(method = "lm", se = FALSE, color = "red") +
    theme_minimal(base_size = 14) +
    labs(
      title = paste0("Group-Means: FE vs ", xvar, " (", YEAR, ")"),
      x = xvar,
      y = "Mean FE"
    )

  out_file <- file.path(OUT_DIR, paste0("groupmeans_FE_", xvar, "_", tag, "_", YEAR, ".png"))
  ggsave(out_file, p, width = 7, height = 5)
  cat("[INFO] Saved group-means plot:", out_file, "\n")
}

# ---------
# RUN BOTH LOOPS
# ---------

# continuous → binned scatter
for (v in continuous_vars) {
  make_binned_scatter(df, v)
}

# discrete → group-means
for (v in discrete_vars) {
  make_groupmeans_plot(df, v)
}

cat("[INFO] Individual-level plots completed.\n\n")


# ============================
# Metro-level summary for FE vs covariates
# ============================
cat("[INFO] Building metro-level summary (2018)...\n")

df_metro <- df %>%
  group_by(first_metro_area) %>%
  summarise(
    n_inventors    = n(),
    mean_FE        = mean(fe_user_id, na.rm = TRUE),
    mean_tenure    = mean(tenure, na.rm = TRUE),
    mean_seniority = mean(avg_seniority, na.rm = TRUE),
    mean_edu_rank  = mean(education_rank, na.rm = TRUE),
    share_phd      = mean(edu_phd, na.rm = TRUE),
    share_master   = mean(edu_master, na.rm = TRUE),
    share_bachelor = mean(edu_bachelor, na.rm = TRUE),
    share_low      = mean(edu_low, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(!is.na(first_metro_area), n_inventors >= 10) %>%  # drop tiny clusters
  mutate(
    log_n_inv = log(n_inventors),
    location  = first_metro_area
  )

cat("[INFO] Metro-level summary rows:", nrow(df_metro), "\n")

# ============================
# FE ~ covariate regressions (metro, weighted)
# ============================
cat("[INFO] Running metro-level FE ~ covariate regressions...\n")

run_FE_regressions <- function(df) {
  covars <- c(
    "mean_tenure",
    "mean_seniority",
    "mean_edu_rank",
    "share_phd",
    "share_master",
    "share_bachelor",
    "share_low"
  )

  results <- lapply(covars, function(yvar) {
    form <- as.formula(paste0("mean_FE ~ ", yvar))
    mod  <- lm(form, data = df, weights = n_inventors)
    row  <- broom::tidy(mod) %>% filter(term == yvar)

    tibble(
      covariate = yvar,
      estimate  = row$estimate,
      std_error = row$std.error,
      statistic = row$statistic,
      p_value   = row$p.value
    )
  })

  bind_rows(results)
}

reg_FE_metro <- run_FE_regressions(df_metro)

reg_txt <- file.path(OUT_DIR, "regression_FE_covariates_metro_2018.txt")
reg_csv <- file.path(OUT_DIR, "regression_FE_covariates_metro_2018.csv")

sink(reg_txt)
cat("=========================================\n")
cat(" FE Regression Table — METRO (mean_FE ~ covariate)\n")
cat(" Year:", YEAR, "\n")
cat("=========================================\n\n")
print(reg_FE_metro, n = Inf, width = Inf)
sink()
write_csv(reg_FE_metro, reg_csv)

cat("[INFO] Saved FE regression tables (metro):\n")
cat("  TXT:", reg_txt, "\n")
cat("  CSV:", reg_csv, "\n")

# ============================
# FE-style bubble plots: FE vs covariates (metro)
# ============================


cat("[INFO] Generating FE-style bubble plots (metro)...\n")

make_FEcov_plot <- function(df_summary, xvar, xlab, top_k = 8) {

  label_data <- df_summary %>% slice_max(n_inventors, n = top_k)

  ggplot(df_summary, aes(
    x    = .data[[xvar]],
    y    = mean_FE,
    size = n_inventors
  )) +
    geom_point(
      color = "black",
      fill  = "grey70",
      alpha = 0.85,
      shape = 21,
      stroke = 0.4
    ) +
    geom_smooth(
      method  = "lm",
      se      = FALSE,
      aes(weight = n_inventors),
      color   = "black",
      linewidth = 0.9
    ) +
    geom_text_repel(
      data = label_data,
      aes(label = location),
      size = 3.0,
      max.overlaps = 15
    ) +
    scale_size(range = c(2.5, 8)) +
    theme_bw(base_size = 13) +
    labs(
      title = paste0("Inventor FE vs ", xlab, " (Metro, ", YEAR, ")"),
      x     = xlab,
      y     = "Mean Inventor FE"
    )
}

pretty_names <- list(
  "mean_tenure"    = "Mean Tenure (Age Proxy)",
  "mean_seniority" = "Mean Job Seniority",
  "mean_edu_rank"  = "Mean Education Rank",
  "share_phd"      = "Share PhD",
  "share_master"   = "Share Master",
  "share_bachelor" = "Share Bachelor",
  "share_low"      = "Share Low Education"
)

for (v in names(pretty_names)) {
  p <- make_FEcov_plot(df_metro, v, pretty_names[[v]])
  out_plot <- file.path(OUT_DIR, paste0("FEcov_", v, "_metro_2018.png"))
  ggsave(out_plot, p, width = 7.5, height = 5, dpi = 400)
  cat("[INFO] Saved plot:", out_plot, "\n")
}

cat("[INFO] All FE–covariate analysis (2018, metro) completed.\n")
