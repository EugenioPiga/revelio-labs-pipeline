#!/usr/bin/env Rscript

###############################################################################
# cluster_stats_2018.R
# Compute location-year inventor statistics for 2018
# Author: Eugenio — 2025-11-xx
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

base_pkgs <- c("arrow", "dplyr", "readr", "ggplot2", "ggrepel", "knitr", "broom")
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
library("ggrepel")
library("knitr")
library("broom")

# ============================
# Config
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/cluster_stats"
YEAR <- 2018

if (!dir.exists(OUT_DIR)) dir.create(OUT_DIR, recursive = TRUE)

cat("[INFO] Computing cluster statistics for year:", YEAR, "\n")
cat("[INFO] Input directory:", INPUT, "\n")

df <- open_dataset(INPUT, format = "parquet")

# only keep US inventors, then COLLECT
df <- df %>%
  filter(first_country == "United States") %>%
  collect()

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


# ================================================
# Load only year 2018
# ================================================
df <- df %>%
  filter(year == YEAR) %>%
  collect()

cat("[INFO] Rows loaded for 2018:", nrow(df), "\n")

# ================================================
# Education Rank Construction
# ================================================
cat("[INFO] Computing education rank...\n")

df <- df %>% mutate(
  education_rank = case_when(
    last_degree == "Doctor" ~ 4,
    last_degree %in% c("Master", "MBA") ~ 3,
    last_degree == "Bachelor" ~ 2,
    last_degree %in% c("Associate", "High School") ~ 1,
    last_degree %in% c("empty", NA) ~ NA_real_,
    TRUE ~ NA_real_
  ),
  edu_phd = as.integer(education_rank == 4),
  edu_master = as.integer(education_rank == 3),
  edu_bachelor = as.integer(education_rank == 2),
  edu_low = as.integer(education_rank == 1)
)

# ================================================
# Helper: Aggregate function
# ================================================
agg_fun <- function(df, loc_var) {

  df %>%
    group_by(.data[[loc_var]]) %>%
    summarise(
      cluster_size = n(),
      mean_tenure = mean(tenure, na.rm = TRUE),
      mean_seniority = mean(avg_seniority, na.rm = TRUE),
      total_inventions = sum(n_patents, na.rm = TRUE),
      mean_inventions = mean(n_patents, na.rm = TRUE),

      num_phd = sum(edu_phd, na.rm = TRUE),
      num_master = sum(edu_master, na.rm = TRUE),
      num_bachelor = sum(edu_bachelor, na.rm = TRUE),
      num_low = sum(edu_low, na.rm = TRUE),

      share_phd = num_phd / cluster_size,
      share_master = num_master / cluster_size,
      share_bachelor = num_bachelor / cluster_size,
      share_low = num_low / cluster_size
    ) %>%
    rename(location = .data[[loc_var]]) %>%
    arrange(desc(cluster_size))
}


# ================================================
# Aggregate by METRO and by STATE
# ================================================

cat("[INFO] Aggregating by first_metro_area...\n")
stats_metro <- agg_fun(df, "first_metro_area")
stats_metro <- stats_metro %>% filter(cluster_size >= 5)

cat("[INFO] Aggregating by first_state...\n")
stats_state <- agg_fun(df, "first_state")
stats_state <- stats_state %>% filter(cluster_size >= 5)

df_summary_metro <- stats_metro %>%
  mutate(
    log_n_inv = log(cluster_size),
    n_inventors = cluster_size,
    location = location   # keep name consistent
  )

df_summary_state <- stats_state %>%
  mutate(
    log_n_inv = log(cluster_size),
    n_inventors = cluster_size,
    location = location
  )

# ================================================
# Save outputs
# ================================================

csv_metro <- file.path(OUT_DIR, "cluster_stats_2018_metro.csv")
csv_state <- file.path(OUT_DIR, "cluster_stats_2018_state.csv")

write_csv(stats_metro, csv_metro)
write_csv(stats_state, csv_state)

cat("[INFO] Saved:", csv_metro, "\n")
cat("[INFO] Saved:", csv_state, "\n")


# ================================================
# Plotting function
# ================================================
plot_var <- function(df, var, label, tag) {

  df <- df %>%
    mutate(log_cluster = log(cluster_size))

  p <- ggplot(df, aes(x = log_cluster, y = .data[[var]])) +
    geom_point(alpha = 0.35) +
    geom_smooth(method = "loess", se = FALSE, color = "red", span = 0.6) +
    theme_minimal(base_size = 14) +
    labs(
      title = paste0(label, " vs log(Cluster Size) (2018, ", tag, ")"),
      x = "log(Number of Inventors)",
      y = label
    )

  out_file <- file.path(OUT_DIR, paste0("plot_", var, "_2018_", tag, "_log.png"))
  ggsave(out_file, p, width = 7, height = 5)
  cat("[INFO] Saved plot:", out_file, "\n")
}

# ======================================================
# FE-style scatter: bubble plot + weighted OLS + labels
# ======================================================
make_cluster_style_plot <- function(df_summary,
                                    yvar,         # string
                                    ylab,         # pretty label
                                    loc_var,      # "location" column
                                    title_prefix, # e.g. "Cluster-level 2018"
                                    top_k = 8) {

  # Select top-k locations to label
  label_data <- df_summary %>%
    slice_max(n_inventors, n = top_k)

  ggplot(df_summary, aes(
    x = log_n_inv,
    y = .data[[yvar]],
    size = n_inventors
  )) +
    geom_point(
      color = "black",
      fill = "grey70",
      alpha = 0.85,
      shape = 21,
      stroke = 0.4
    ) +
    geom_smooth(
      method = "lm",
      se = FALSE,
      color = "black",
      linewidth = 0.9,
      aes(weight = n_inventors)
    ) +
    geom_text_repel(
      data = label_data,
      aes_string(label = loc_var),
      size = 3.0,
      max.overlaps = 15
    ) +
    scale_size(range = c(2.5, 8)) +
    theme_bw(base_size = 13) +
    labs(
      title = paste0(title_prefix, " — ", ylab, " vs Inventors"),
      x = "log(Number of Inventors)",
      y = ylab
    )
}

# ================================================
# Generate All Plots
# ================================================

vars_to_plot <- list(
  "mean_tenure" = "Mean Tenure",
  "mean_seniority" = "Mean Job Seniority",
  "total_inventions" = "Total Inventions",
  "mean_inventions" = "Mean Inventions",
  "share_phd" = "Share PhD",
  "share_master" = "Share Master",
  "share_bachelor" = "Share Bachelor",
  "share_low" = "Share Low Education"
)

cat("[INFO] Generating plots...\n")

for (v in names(vars_to_plot)) {
   plot_var(stats_metro, v, vars_to_plot[[v]], "metro")
   plot_var(stats_state, v, vars_to_plot[[v]], "state")
}

for (v in names(vars_to_plot)) {

  # Metro
  p_metro <- make_cluster_style_plot(
    df_summary_metro,
    yvar = v,
    ylab = vars_to_plot[[v]],
    loc_var = "location",
    title_prefix = "Metro-level 2018"
  )

  ggsave(
    file.path(OUT_DIR, paste0("FEstyle_", v, "_metro_2018.png")),
    p_metro, width = 7.5, height = 5, dpi = 400
  )

  # State
  p_state <- make_cluster_style_plot(
    df_summary_state,
    yvar = v,
    ylab = vars_to_plot[[v]],
    loc_var = "location",
    title_prefix = "State-level 2018"
  )

  ggsave(
    file.path(OUT_DIR, paste0("FEstyle_", v, "_state_2018.png")),
    p_state, width = 7.5, height = 5, dpi = 400
  )
}

cat("[INFO] All plots are generated.\n")


# ================================================
# Regression tables for all covariates (final version)
# ================================================
cat("[INFO] Running regression tables...\n")

library(broom)

run_all_regressions <- function(df_summary, tag) {

  covars <- names(vars_to_plot)

  results <- lapply(covars, function(yvar) {
    form <- as.formula(paste0(yvar, " ~ log_n_inv"))
    mod <- lm(form, data = df_summary, weights = n_inventors)

    row <- tidy(mod) %>% filter(term == "log_n_inv")

    tibble(
      y = yvar,
      estimate = row$estimate,
      std_error = row$std.error,
      statistic = row$statistic,
      p_value = row$p.value,
      region = tag
    )
  })

  bind_rows(results)
}

# Separate tables
reg_metro <- run_all_regressions(df_summary_metro, "metro")
reg_state <- run_all_regressions(df_summary_state, "state")

# ==========================
# Save METRO table
# ==========================
file_metro <- file.path(OUT_DIR, "regression_table_metro.txt")

sink(file_metro)
cat("=========================================\n")
cat(" Regression Table — METRO (y ~ log_n_inv)\n")
cat(" Year:", YEAR, "\n")
cat("=========================================\n\n")
print(reg_metro, n = Inf, width = Inf)
sink()

cat("[INFO] Saved metro regression table:", file_metro, "\n")

# ==========================
# Save STATE table
# ==========================
file_state <- file.path(OUT_DIR, "regression_table_state.txt")

sink(file_state)
cat("=========================================\n")
cat(" Regression Table — STATE (y ~ log_n_inv)\n")
cat(" Year:", YEAR, "\n")
cat("=========================================\n\n")
print(reg_state, n = Inf, width = Inf)
sink()

cat("[INFO] Saved state regression table:", file_state, "\n")

# Also print to console
cat("\n========== METRO RESULTS ==========\n")
print(reg_metro, n = Inf, width = Inf)

cat("\n========== STATE RESULTS ==========\n")
print(reg_state, n = Inf, width = Inf)


