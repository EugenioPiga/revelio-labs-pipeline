#!/usr/bin/env Rscript

# =========================================================
# V3 configuration
# =========================================================
V3_INPUT_DIR <- Sys.getenv("REVELIO_V3_INPUT", unset = "")
if (!nzchar(V3_INPUT_DIR) || !dir.exists(V3_INPUT_DIR)) {
  stop(
    "REVELIO_V3_INPUT is unset or invalid. Edit ",
    "~/revelio_labs/analyses_original_v2_logic_on_v3/00_config/v3_env.sh"
  )
}

V3_OUTPUT_DIR <- Sys.getenv(
  "REVELIO_V3_OUTPUT_DIR",
  unset = "/home/epiga/revelio_labs/analyses_original_v2_logic_on_v3/output/metro_parent_productivity"
)
dir.create(V3_OUTPUT_DIR, recursive = TRUE, showWarnings = FALSE)
###############################################################################
# METRO PRODUCTIVITY PREMIUM DECOMPOSITION
#
# Goal:
#   Replicate, on inventor-year data, the reduced-form decomposition logic from
#   “There’s Nothing in the Air” using inventor productivity (n_patents) instead
#   of wages.
#
# Main design choices:
#   - Outcome: n_patents
#   - Location: metro
#   - Firm definitions:
#       (i)  parent rcid
#       (ii) metro-parent = parent rcid x metro
#   - Metro size is constructed on the FULL SAMPLE (before the US filter)
#   - US filter is applied RIGHT BEFORE estimation
#   - First-stage estimation uses consecutive inventor-years only
#   - Full-sample first stage keeps inventor-years where metro_t == metro_{t-1}
#   - "Stayer" sample keeps inventor-years where metro_parent_t == metro_parent_{t-1}
#     (same local firm in the same metro)
#
# First-stage PPML specifications:
#   (1) BASELINE:
#       n_patents_t ~ lag_n_patents + tenure + tenure_sq | user_id + metro + year
#
#   (2) + LAGGED PARENT FE:
#       n_patents_t ~ lag_n_patents + tenure + tenure_sq |
#                     user_id + metro + year + parent_{t-1}
#
#   (3) + LAGGED METRO_PARENT FE:
#       n_patents_t ~ lag_n_patents + tenure + tenure_sq |
#                     user_id + metro + year + metro_parent_{t-1}
#
# Second stage:
#   Extract metro fixed effects from each first-stage specification and project
#   them onto log average metro size (constructed on the full sample):
#
#       metro_FE_m = alpha0 + alpha * log(avg_metro_size_m) + e_m
#
# Outputs:
#   - first-stage summary CSV
#   - second-stage summary CSV (alpha, se, p-value, attenuation)
#   - metro fixed effects CSVs
#   - metro size table CSV
#   - figures analogous to the paper:
#       * metro FE vs metro size (full sample)
#       * metro FE vs metro size (stayers)
#       * alpha coefficient comparison plot
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c(
  "arrow", "data.table", "dplyr", "fixest", "ggplot2",
  "stringr", "readr", "tibble", "scales"
)

for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(stringr)
  library(readr)
  library(tibble)
  library(scales)
})

data.table::setDTthreads(max(1L, as.integer(Sys.getenv("REVELIO_R_THREADS", unset = "1"))))
set.seed(123)

# =========================
# Config
# =========================
INPUT_DIR <- V3_INPUT_DIR
OUT_DIR   <- V3_OUTPUT_DIR
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

DIRS <- list(
  tables = file.path(OUT_DIR, "tables"),
  plots  = file.path(OUT_DIR, "plots"),
  fe     = file.path(OUT_DIR, "fixed_effects"),
  logs   = file.path(OUT_DIR, "intermediate")
)
invisible(lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE))

PARENT_VAR <- "first_parent_rcid"
US_COUNTRY <- "United States"
TENURE_MAX <- 50
YEAR_MIN   <- 2010
YEAR_MAX   <- 2024
N_BINS     <- 50L

METRO_CANDIDATES <- c(
  "first_metro", "first_metro_area", "metro", "metro_name",
  "first_msa", "msa", "msa_name", "first_cbsa", "cbsa", "cbsa_name"
)

# =========================
# Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

pick_first_existing <- function(candidates, available, what = "variable") {
  hit <- intersect(candidates, available)
  if (length(hit) == 0) {
    stop(paste0("[ERROR] Could not find any ", what, ". Tried: ",
                paste(candidates, collapse = ", ")))
  }
  hit[1]
}

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) {
    stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
  }
}

safe_chr <- function(x) {
  x <- str_trim(as.character(x))
  x[x == "" | x == "NA"] <- NA_character_
  x
}

safe_num <- function(x) {
  suppressWarnings(as.numeric(x))
}

sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}

theme_eug <- function() {
  theme_minimal(base_size = 13) +
    theme(
      panel.grid.minor = element_blank(),
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )
}

fmt_p <- function(x) {
  ifelse(!is.finite(x), NA_character_, formatC(x, format = "f", digits = 4))
}

# =========================
# Derived variables
# =========================
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = safe_num(substr(as.character(first_startdate_edu), 1, 4)),
      pos_year = safe_num(substr(as.character(first_startdate_pos), 1, 4)),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA_real_, tenure),
      tenure_sq = tenure^2
    )
}

make_ids <- function(df, metro_var) {
  df %>%
    mutate(
      metro_id = safe_chr(.data[[metro_var]]),
      parent_id = safe_chr(.data[[PARENT_VAR]]),
      metro_parent_id = case_when(
        !is.na(metro_id) & !is.na(parent_id) ~ paste0(parent_id, "||", metro_id),
        TRUE ~ NA_character_
      )
    )
}

# =========================
# Full-sample metro size
# =========================
build_metro_size_table <- function(df) {
  dt <- as.data.table(df[, c("user_id", "year", "metro_id"), drop = FALSE])
  dt <- unique(dt[!is.na(user_id) & !is.na(year) & !is.na(metro_id), .(user_id, year, metro_id)])

  if (nrow(dt) == 0) {
    return(data.table(
      metro_id = character(),
      year = integer(),
      metro_size_current = integer(),
      avg_metro_size_full = numeric(),
      median_metro_size_full = numeric(),
      log_avg_metro_size_full = numeric(),
      n_years_in_size = integer()
    ))
  }

  metro_year <- dt[, .(
    metro_size_current = .N
  ), by = .(metro_id, year)]

  metro_sum <- metro_year[, .(
    avg_metro_size_full = as.numeric(mean(metro_size_current, na.rm = TRUE)),
    median_metro_size_full = as.numeric(median(metro_size_current, na.rm = TRUE)),
    n_years_in_size = as.integer(.N)
  ), by = metro_id]
  metro_sum[, log_avg_metro_size_full := log(pmax(avg_metro_size_full, 1))]

  merge(metro_year, metro_sum, by = "metro_id", all.x = TRUE)
}

# =========================
# Lag structure and samples
# =========================
add_lags <- function(df) {
  x <- as.data.table(copy(df))
  setorder(x, user_id, year)

  x[, lag_year := shift(year), by = user_id]
  x[, lag_n_patents := shift(n_patents), by = user_id]
  x[, lag_metro_id := shift(metro_id), by = user_id]
  x[, lag_parent_id := shift(parent_id), by = user_id]
  x[, lag_metro_parent_id := shift(metro_parent_id), by = user_id]

  x[, consecutive := !is.na(lag_year) & year == lag_year + 1L]
  x[, same_metro := consecutive & !is.na(metro_id) & !is.na(lag_metro_id) & metro_id == lag_metro_id]
  x[, same_parent := consecutive & !is.na(parent_id) & !is.na(lag_parent_id) & parent_id == lag_parent_id]
  x[, same_metro_parent := consecutive & !is.na(metro_parent_id) & !is.na(lag_metro_parent_id) & metro_parent_id == lag_metro_parent_id]

  x[]
}

# =========================
# Estimation helpers
# =========================
run_fepois_safe <- function(formula, data) {
  x <- copy(as.data.table(data))

  fixest::fepois(
    formula,
    data = x,
    vcov = ~user_id,
    fixef.rm = "perfect_fit",
    notes = FALSE,
    warn = FALSE
  )
}

extract_first_stage_stats <- function(model, meta) {
  ct <- fixest::coeftable(model)
  lag_row <- if ("lag_n_patents" %in% rownames(ct)) ct["lag_n_patents", , drop = FALSE] else NULL

  tibble(
    sample = meta$sample,
    spec = meta$spec,
    n_obs = meta$n_obs,
    n_users = meta$n_users,
    n_metros = meta$n_metros,
    lag_coef = if (!is.null(lag_row)) as.numeric(lag_row[1, 1]) else NA_real_,
    lag_se   = if (!is.null(lag_row)) as.numeric(lag_row[1, 2]) else NA_real_,
    lag_p    = if (!is.null(lag_row)) as.numeric(lag_row[1, 4]) else NA_real_
  )
}

extract_metro_fe <- function(model, meta) {
  fe_list <- fixest::fixef(model)
  if (!"metro_id" %in% names(fe_list)) {
    stop("[ERROR] metro_id fixed effects not found in model.")
  }

  fe <- fe_list[["metro_id"]]
  tibble(
    metro_id = names(fe),
    metro_fe = as.numeric(fe),
    sample = meta$sample,
    spec = meta$spec
  )
}

run_second_stage <- function(fe_df, baseline_alpha = NA_real_) {
  x <- as.data.table(copy(fe_df))
  x <- unique(x[is.finite(metro_fe) & is.finite(log_avg_metro_size_full), .(
    metro_id, metro_fe, avg_metro_size_full, log_avg_metro_size_full, sample, spec
  )])

  est <- fixest::feols(
    metro_fe ~ log_avg_metro_size_full,
    data = x,
    vcov = "hetero",
    notes = FALSE,
    warn = FALSE
  )

  ct <- fixest::coeftable(est)
  alpha <- as.numeric(ct["log_avg_metro_size_full", 1])
  se    <- as.numeric(ct["log_avg_metro_size_full", 2])
  pval  <- as.numeric(ct["log_avg_metro_size_full", 4])

  tibble(
    sample = unique(x$sample),
    spec = unique(x$spec),
    n_metros_second_stage = nrow(x),
    alpha = alpha,
    alpha_se = se,
    alpha_p = pval,
    alpha_ci_low = alpha - 1.96 * se,
    alpha_ci_high = alpha + 1.96 * se,
    attenuation_vs_sample_baseline_pct = ifelse(
      is.finite(baseline_alpha) & baseline_alpha != 0,
      100 * (1 - alpha / baseline_alpha),
      NA_real_
    )
  )
}

bin_fe_for_plot <- function(fe_df, n_bins = N_BINS) {
  x <- as.data.table(copy(fe_df))
  x <- x[is.finite(log_avg_metro_size_full) & is.finite(metro_fe)]
  if (nrow(x) == 0) return(tibble())

  x[, size_rank := frank(log_avg_metro_size_full, ties.method = "average")]
  x[, bin := pmin(n_bins, pmax(1L, ceiling(n_bins * size_rank / .N)))]

  out <- x[, .(
    mean_log_size = mean(log_avg_metro_size_full, na.rm = TRUE),
    mean_size = mean(avg_metro_size_full, na.rm = TRUE),
    mean_fe = mean(metro_fe, na.rm = TRUE),
    n_metros = .N
  ), by = .(sample, spec, bin)]

  as_tibble(out)
}

make_fe_plot <- function(plot_df, second_stage_df, sample_keep, title_text) {
  z <- plot_df %>% filter(sample == sample_keep)
  if (nrow(z) == 0) return(NULL)

  lab <- second_stage_df %>%
    filter(sample == sample_keep) %>%
    mutate(label = paste0(
      spec, ": alpha=", round(alpha, 4),
      " (se=", round(alpha_se, 4), ", p=", fmt_p(alpha_p), ")"
    )) %>%
    pull(label)

  ggplot(z, aes(x = mean_size, y = mean_fe, color = spec)) +
    geom_point(size = 2, alpha = 0.9) +
    geom_smooth(method = "lm", se = FALSE, linewidth = 0.9) +
    scale_x_log10(labels = label_number(big.mark = ",")) +
    labs(
      title = title_text,
      subtitle = paste(lab, collapse = " | "),
      x = "Average metro size (full sample, log scale)",
      y = "Average metro fixed effect",
      color = "Specification"
    ) +
    theme_eug()
}

make_alpha_plot <- function(second_stage_df) {
  z <- second_stage_df %>%
    mutate(
      spec = factor(spec, levels = c("Baseline", "+ Lagged Parent FE", "+ Lagged Metro-Parent FE")),
      sample = factor(sample, levels = c("FULL_SAME_METRO", "STAYERS_SAME_METRO_PARENT"))
    )

  ggplot(z, aes(x = spec, y = alpha, color = sample)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_point(position = position_dodge(width = 0.4), size = 2.8) +
    geom_errorbar(
      aes(ymin = alpha_ci_low, ymax = alpha_ci_high),
      position = position_dodge(width = 0.4),
      width = 0.15,
      linewidth = 0.7
    ) +
    labs(
      title = "Metro productivity premium: second-stage estimates",
      subtitle = "Outcome = n_patents; PPML first stage with inventor FE + metro FE + year FE",
      x = NULL,
      y = "Alpha on log average metro size",
      color = "Sample"
    ) +
    theme_eug()
}

# =========================
# Load data
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT_DIR, format = "parquet")
ds_names <- names(ds)

METRO_VAR <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro variable")
ts_msg("Using metro variable:", METRO_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents", "first_country",
  "first_startdate_edu", "first_startdate_pos",
  PARENT_VAR, METRO_VAR
))

ts_msg("Collecting full panel...")
df_full <- ds %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(all_of(c(
    "user_id", "year", "n_patents", "first_country",
    "first_startdate_edu", "first_startdate_pos",
    PARENT_VAR, METRO_VAR
  ))) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = safe_num(n_patents),
    n_patents = ifelse(is.na(n_patents), 0, n_patents),
    first_country = safe_chr(first_country),
    !!PARENT_VAR := safe_chr(.data[[PARENT_VAR]]),
    !!METRO_VAR  := safe_chr(.data[[METRO_VAR]])
  ) %>%
  compute_tenure() %>%
  make_ids(METRO_VAR) %>%
  mutate(year_fe = factor(year))

# =========================
# Build full-sample metro size table
# =========================
ts_msg("Building full-sample metro size tables...")
metro_size_full <- build_metro_size_table(df_full)
write_csv(as_tibble(metro_size_full), file.path(DIRS$tables, "metro_size_full_sample.csv"))

# =========================
# Build lag structure BEFORE US filter
# =========================
ts_msg("Building lag structure...")
df_lag <- add_lags(df_full)

# =========================
# Apply US filter RIGHT BEFORE regression
# =========================
ts_msg("Applying US filter right before estimation...")
df_us <- df_lag %>%
  filter(!is.na(first_country), first_country == US_COUNTRY) %>%
  left_join(
    metro_size_full %>%
      distinct(metro_id, avg_metro_size_full, median_metro_size_full,
               log_avg_metro_size_full, n_years_in_size),
    by = "metro_id"
  ) %>%
  mutate(year_fe = factor(year))

# =========================
# Define estimation samples
# =========================
sample_list <- list(
  FULL_SAME_METRO = df_us %>%
    filter(
      consecutive,
      same_metro,
      !is.na(lag_n_patents),
      is.finite(tenure),
      is.finite(tenure_sq),
      !is.na(metro_id),
      !is.na(lag_parent_id),
      !is.na(lag_metro_parent_id),
      is.finite(log_avg_metro_size_full)
    ),
  STAYERS_SAME_METRO_PARENT = df_us %>%
    filter(
      consecutive,
      same_metro_parent,
      !is.na(lag_n_patents),
      is.finite(tenure),
      is.finite(tenure_sq),
      !is.na(metro_id),
      !is.na(lag_parent_id),
      !is.na(lag_metro_parent_id),
      is.finite(log_avg_metro_size_full)
    )
)

# =========================
# Specifications
# =========================
specs <- list(
  Baseline = list(
    fe_rhs = "user_id + metro_id + year_fe"
  ),
  `+ Lagged Parent FE` = list(
    fe_rhs = "user_id + metro_id + year_fe + lag_parent_id"
  ),
  `+ Lagged Metro-Parent FE` = list(
    fe_rhs = "user_id + metro_id + year_fe + lag_metro_parent_id"
  )
)

# =========================
# Run models
# =========================
all_first_stage <- list()
all_second_stage <- list()
all_fe <- list()
idx_fs <- 0L
idx_ss <- 0L
idx_fe <- 0L

for (samp_name in names(sample_list)) {
  ts_msg("Running sample:", samp_name)
  df_s <- as_tibble(sample_list[[samp_name]])

  if (nrow(df_s) == 0) {
    ts_msg("Skipping", samp_name, "because sample is empty.")
    next
  }

  if (dplyr::n_distinct(df_s$user_id) < 50 || dplyr::n_distinct(df_s$metro_id) < 10) {
    ts_msg("Skipping", samp_name, "because sample is too small.")
    next
  }

  second_stage_buffer <- list()
  baseline_alpha <- NA_real_

  for (spec_name in names(specs)) {
    ts_msg("  Estimating:", spec_name)
    fe_rhs <- specs[[spec_name]]$fe_rhs

    form <- as.formula(
      paste0("n_patents ~ lag_n_patents + tenure + tenure_sq | ", fe_rhs)
    )

    est <- tryCatch(
      run_fepois_safe(form, df_s),
      error = function(e) {
        ts_msg("  FAILED:", samp_name, "|", spec_name, "|", e$message)
        NULL
      }
    )

    if (is.null(est)) next

    meta <- list(
      sample = samp_name,
      spec = spec_name,
      n_obs = nrow(df_s),
      n_users = dplyr::n_distinct(df_s$user_id),
      n_metros = dplyr::n_distinct(df_s$metro_id)
    )

    idx_fs <- idx_fs + 1L
    all_first_stage[[idx_fs]] <- extract_first_stage_stats(est, meta)

    fe_df <- extract_metro_fe(est, meta) %>%
      left_join(
        metro_size_full %>%
          distinct(metro_id, avg_metro_size_full, median_metro_size_full,
                   log_avg_metro_size_full, n_years_in_size),
        by = "metro_id"
      )

    idx_fe <- idx_fe + 1L
    all_fe[[idx_fe]] <- fe_df

    write_csv(
      fe_df,
      file.path(
        DIRS$fe,
        paste0(
          "metro_fe__",
          samp_name, "__",
          str_replace_all(spec_name, "[^A-Za-z0-9]+", "_"),
          ".csv"
        )
      )
    )

    # second stage
    if (spec_name == "Baseline") {
      ss <- run_second_stage(fe_df, baseline_alpha = NA_real_)
      baseline_alpha <- ss$alpha[1]
    } else {
      ss <- run_second_stage(fe_df, baseline_alpha = baseline_alpha)
    }

    if (spec_name == "Baseline") {
      ss$attenuation_vs_sample_baseline_pct <- 0
    }

    idx_ss <- idx_ss + 1L
    all_second_stage[[idx_ss]] <- ss
    second_stage_buffer[[spec_name]] <- ss
  }
}

if (length(all_first_stage) == 0 || length(all_second_stage) == 0 || length(all_fe) == 0) {
  stop("[ERROR] No successful estimation produced output.")
}

first_stage_tab <- bind_rows(all_first_stage)
second_stage_tab <- bind_rows(all_second_stage) %>%
  mutate(stars = sig_stars(alpha_p))
fe_all <- bind_rows(all_fe)

# =========================
# Save tables
# =========================
write_csv(first_stage_tab, file.path(DIRS$tables, "first_stage_summary.csv"))
write_csv(second_stage_tab, file.path(DIRS$tables, "second_stage_summary.csv"))
write_csv(fe_all, file.path(DIRS$fe, "metro_fixed_effects_all_specs.csv"))

# =========================
# Build plotting datasets
# =========================
plot_bins <- bin_fe_for_plot(fe_all)
write_csv(plot_bins, file.path(DIRS$tables, "metro_fe_binned_for_plots.csv"))

# =========================
# Figures
# =========================
g_full <- make_fe_plot(
  plot_df = plot_bins,
  second_stage_df = second_stage_tab,
  sample_keep = "FULL_SAME_METRO",
  title_text = "Metro fixed effects vs metro size: full sample (same metro across t-1 and t)"
)
if (!is.null(g_full)) {
  ggsave(
    filename = file.path(DIRS$plots, "figure_metro_fe_vs_size_full_sample.png"),
    plot = g_full, width = 11, height = 8, dpi = 300
  )
}

g_stayers <- make_fe_plot(
  plot_df = plot_bins,
  second_stage_df = second_stage_tab,
  sample_keep = "STAYERS_SAME_METRO_PARENT",
  title_text = "Metro fixed effects vs metro size: stayers (same metro-parent across t-1 and t)"
)
if (!is.null(g_stayers)) {
  ggsave(
    filename = file.path(DIRS$plots, "figure_metro_fe_vs_size_stayers.png"),
    plot = g_stayers, width = 11, height = 8, dpi = 300
  )
}

g_alpha <- make_alpha_plot(second_stage_tab)
ggsave(
  filename = file.path(DIRS$plots, "figure_alpha_comparison.png"),
  plot = g_alpha, width = 11, height = 7, dpi = 300
)

# =========================
# Small text summary for quick inspection
# =========================
summary_txt <- second_stage_tab %>%
  mutate(
    line = paste0(
      sample, " | ", spec,
      " | alpha=", round(alpha, 4),
      " | se=", round(alpha_se, 4),
      " | p=", fmt_p(alpha_p),
      " | attenuation=", ifelse(is.na(attenuation_vs_sample_baseline_pct), "NA",
                                 paste0(round(attenuation_vs_sample_baseline_pct, 1), "%"))
    )
  ) %>%
  pull(line)

writeLines(
  c(
    "Metro productivity premium decomposition complete.",
    "",
    "Second-stage summary:",
    summary_txt
  ),
  con = file.path(DIRS$logs, "quick_summary.txt")
)

ts_msg("DONE. Outputs in:", OUT_DIR)
