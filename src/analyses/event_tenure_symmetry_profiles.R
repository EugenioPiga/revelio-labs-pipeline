#!/usr/bin/env Rscript
###############################################################################
# EVENT-STUDY SYMMETRY PROFILES (FIRM / CITY / FIRM-WITHIN-CITY)
# Mean n_patents around job transitions by origin/destination quartiles
# Includes separate analysis for top inventors (above 90th percentile)
# Author: Eugenio
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","data.table","dplyr","ggplot2","stringr","readr","lubridate")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos="https://cloud.r-project.org", lib=user_lib)
  }
}

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes", repos="https://cloud.r-project.org", lib=user_lib)
}

if (!requireNamespace("alpaca", quietly = TRUE)) {
  cat("[INFO] Installing alpaca from GitHub...\n")
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib, dependencies = TRUE)
}

suppressPackageStartupMessages({
  library(arrow); library(data.table); library(dplyr)
  library(ggplot2); library(stringr); library(readr); library(lubridate)
  library(alpaca)
})
cat("[DEBUG] alpaca loaded:", "alpaca" %in% loadedNamespaces(), "\n")

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_tenure_symmetry"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

year_min <- 1990
T_max <- 8
min_tenure_each_side <- 2

# =========================
# Helper Functions
# =========================
make_market_key <- function(dt, mover_type) {
  switch(
    mover_type,
    "firm" = dt[, mkt := as.character(first_rcid)],
    "city" = dt[, mkt := as.character(first_city)],
    "firm_within_city" = dt[, mkt := paste0(first_rcid, "||", first_city)],
    stop("Unknown mover_type")
  )
  dt[, mkt := as.character(mkt)]
  dt[]
}

tag_first_move <- function(dt) {
  setorder(dt, user_id, year)
  dt[, mkt_lag := shift(mkt), by = user_id]
  dt[, moved := (!is.na(mkt_lag) & mkt != mkt_lag)]
  move_tbl <- dt[moved == TRUE, .(move_year = min(year, na.rm = TRUE)), by = user_id]
  dt[, c("mkt_lag","moved") := NULL]
  dt <- merge(dt, move_tbl, by = "user_id", all.x = TRUE)
  dt[]
}

compute_loo_means <- function(dt) {
  data.table::setorder(dt, mkt, year)
  mkt_tot <- dt[, .(sum_pat = sum(n_patents, na.rm = TRUE),
                    n_obs  = .N), by = .(mkt, year)]
  ui_tot <- dt[, .(sum_pat_ui = sum(n_patents, na.rm = TRUE),
                   n_obs_ui  = .N), by = .(user_id, mkt, year)]
  ui_tot <- ui_tot[mkt_tot, on = .(mkt, year)]
  ui_tot[, mean_loo := (sum_pat - sum_pat_ui) / pmax(n_obs - n_obs_ui, 1L)]
  dt <- merge(dt, ui_tot[, .(user_id, mkt, year, mean_loo)],
              by = c("user_id", "mkt", "year"), all.x = TRUE)
  dt[]
}

prep_event_time <- function(dt) {
  setorder(dt, user_id, year)
  dt[, event_time := fifelse(is.na(move_year), NA_integer_, year - move_year)]
  dt <- dt[event_time >= -T_max & event_time <= T_max]
  dt[]
}

# =========================
# Load Data Once
# =========================
cat("[INFO] Loading dataset...\n")
ds <- arrow::open_dataset(INPUT_DIR, format="parquet")
df_full <- ds %>% dplyr::filter(year >= !!year_min) %>% collect()
setDT(df_full)
df_full[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
df_full[, first_city := na_if(first_city, "empty")]
df_full <- df_full[!is.na(first_rcid) & !is.na(first_city)]
cat("[INFO] Rows after filter:", nrow(df_full), "\n")

# =========================
# Identify Top Inventors
# =========================
cat("[INFO] Identifying top inventors (>= 90th percentile lifetime patents)...\n")
inventor_totals <- df_full[, .(total_pat = sum(n_patents, na.rm = TRUE)), by = user_id]
p90_threshold <- quantile(inventor_totals$total_pat, 0.9, na.rm = TRUE)
cat("[INFO] Threshold for top inventors:", round(p90_threshold, 2), "patents\n")
top_ids <- inventor_totals[total_pat >= p90_threshold, user_id]
df_full_top <- df_full[user_id %in% top_ids]
cat("[INFO] Number of top inventors:", uniqueN(df_full_top$user_id), "\n")

# =========================
# Function to Run Quartile Analysis
# =========================
run_quartile_analysis <- function(df_input, mover_types, label_suffix = "") {
  for (mover_type in mover_types) {
    cat("\n==========================================\n")
    cat("[INFO] Processing mover type:", mover_type, label_suffix, "\n")
    cat("==========================================\n")

    df <- copy(df_input)
    df <- make_market_key(df, mover_type)
    df <- tag_first_move(df)
    df <- compute_loo_means(df)
    df <- prep_event_time(df)

    # Tenure filter
    tenure_info <- df[!is.na(move_year),
      .(
        min_pre = as.numeric(if (any(year < move_year))
          move_year - min(year[year < move_year], na.rm = TRUE)
          else NA_real_),
        min_post = as.numeric(if (any(year > move_year))
          max(year[year > move_year], na.rm = TRUE) - move_year
          else NA_real_)
      ),
      by = user_id
    ]
    tenure_info <- tenure_info[min_pre >= min_tenure_each_side & min_post >= min_tenure_each_side]
    df <- df[user_id %in% tenure_info$user_id]

    # Quartile assignment
    df[, quartile := NA_integer_]
    df[!is.na(mean_loo), quartile := as.integer(ntile(mean_loo, 4)), by = year]

    origin_dest <- df[!is.na(move_year),
      .(
        origin_q = quartile[year == max(year[year < move_year], na.rm = TRUE)],
        dest_q   = quartile[year == min(year[year > move_year], na.rm = TRUE)]
      ),
      by = user_id
    ]
    origin_dest <- origin_dest[!is.na(origin_q) & !is.na(dest_q)]
    df <- merge(df, origin_dest, by = "user_id", all.x = TRUE)
    df <- df[!is.na(origin_q) & !is.na(dest_q)]

    # Aggregate mean profiles
    agg <- df[!is.na(event_time),
              .(mean_pat = mean(n_patents, na.rm = TRUE)),
              by = .(origin_q, dest_q, event_time)]

    # Plot profiles
    for (oq in sort(unique(agg$origin_q))) {
      sub <- agg[origin_q == oq]
      p <- ggplot(sub, aes(x = event_time, y = mean_pat,
                           color = factor(dest_q), group = factor(dest_q))) +
        geom_line(size = 1) +
        geom_vline(xintercept = 0, linetype = "dashed", color = "black") +
        scale_color_brewer(palette = "Dark2", name = "Dest. Quartile") +
        labs(title = paste0("Mean n_patents Profile (Origin Quartile ", oq,
                            " — ", str_replace_all(mover_type, "_", " "), " movers", label_suffix, ")"),
             x = "Years Relative to Job Change",
             y = "Mean n_patents") +
        theme_minimal(base_size = 13) +
        theme(legend.position = "bottom",
              plot.title = element_text(hjust = 0.5, face = "bold"))

      suffix <- ifelse(label_suffix == "", "", paste0("_", str_trim(gsub(" ", "_", label_suffix))))
      out_file <- file.path(OUT_DIR, paste0("symmetry_origin_q", oq, "_", mover_type, suffix, ".png"))
      ggsave(out_file, p, width = 7, height = 5, dpi = 300)
      cat("[INFO] Saved plot:", out_file, "\n")
    }
  }
}

# =========================
# Run for All Inventors
# =========================
cat("\n[INFO] Running full sample quartile analysis...\n")
run_quartile_analysis(df_full, c("firm", "city", "firm_within_city"), "")

# =========================
# Run for Top Inventors
# =========================
cat("\n[INFO] Running quartile analysis for top inventors...\n")
run_quartile_analysis(df_full_top, c("firm", "city", "firm_within_city"), "topinventors")

cat("\n[INFO] ✅ Completed symmetry analyses (full sample + top inventors).\n")

###############################################################################
# GLOBAL VALUE OF CITIES STYLE — SYMMETRY TESTS (firm / city / firm_within_city)
###############################################################################

cat("\n==========================================\n")
cat("[INFO] Running Global-Value-of-Cities-style symmetry scatter analyses...\n")
cat("==========================================\n")

# Configuration for this section
pre_window <- 5
post_window <- 5
mover_types <- c("firm", "city", "firm_within_city")

run_global_value_analysis <- function(df_input, label_suffix = "") {
  for (mover_type in mover_types) {
    cat("\n==========================================\n")
    cat("[INFO] Processing mover type:", mover_type, label_suffix, "\n")
    cat("==========================================\n")

    df <- copy(df_input)
    df[, log_n_patents := log1p(n_patents)]
    df <- make_market_key(df, mover_type)
    df <- tag_first_move(df)

    # keep movers with enough pre/post years
    df[, years_pre := sum(year < move_year, na.rm = TRUE), by = user_id]
    df[, years_post := sum(year > move_year, na.rm = TRUE), by = user_id]
    df <- df[years_pre >= pre_window & years_post >= post_window]

    # compute market means
    market_psi_lvl <- df[, .(psi = mean(n_patents, na.rm = TRUE)), by = .(mkt, year)]
    market_psi_log <- df[, .(psi = mean(log_n_patents, na.rm = TRUE)), by = .(mkt, year)]

    # identify origin/destination markets
    od <- df[!is.na(move_year),
             .(mkt_origin = { yrs <- year[year < move_year]; if (length(yrs)==0) NA_character_ else mkt[year==max(yrs)] },
               mkt_dest   = { yrs <- year[year > move_year]; if (length(yrs)==0) NA_character_ else mkt[year==min(yrs)] },
               year_move  = unique(move_year)),
             by = user_id]
    od <- od[!is.na(mkt_origin) & !is.na(mkt_dest)]

    # helper function
    compute_pair_scatter <- function(df, market_psi, yvar_name, label_suffix) {
      psi_pre <- merge(
        od, market_psi, by.x = "mkt_origin", by.y = "mkt", allow.cartesian = TRUE
      )[year < year_move & year >= (year_move - pre_window),
        .(psi_origin = mean(psi, na.rm = TRUE)),
        by = .(user_id, mkt_origin, mkt_dest, year_move)]

      psi_post <- merge(
        od, market_psi, by.x = "mkt_dest", by.y = "mkt", allow.cartesian = TRUE
      )[year > year_move & year <= (year_move + post_window),
        .(psi_dest = mean(psi, na.rm = TRUE)),
        by = .(user_id, mkt_origin, mkt_dest, year_move)]

      psi_comb <- merge(psi_pre, psi_post,
                        by = c("user_id","mkt_origin","mkt_dest","year_move"))
      psi_comb[, delta_psi := psi_dest - psi_origin]

      pre_post <- df[!is.na(move_year),
                     .(mean_pre  = mean(get(yvar_name)[year %in% ((move_year - pre_window):(move_year - 1))], na.rm = TRUE),
                       mean_post = mean(get(yvar_name)[year %in% ((move_year + 1):(move_year + post_window))], na.rm = TRUE)),
                     by = .(user_id, move_year)]
      pre_post[, delta_y := mean_post - mean_pre]

      res <- merge(psi_comb[, .(user_id, mkt_origin, mkt_dest, delta_psi)],
                   pre_post[, .(user_id, delta_y)], by = "user_id")
      res <- res[is.finite(delta_y) & is.finite(delta_psi)]

      pair_df <- res[, .(
        delta_y = mean(delta_y, na.rm = TRUE),
        delta_psi = mean(delta_psi, na.rm = TRUE),
        n_movers = .N
      ), by = .(mkt_origin, mkt_dest)]
      pair_df <- pair_df[is.finite(delta_y) & is.finite(delta_psi)]

      # trim outliers (1–99%)
      q_low_psi <- quantile(pair_df$delta_psi, 0.01, na.rm = TRUE)
      q_high_psi <- quantile(pair_df$delta_psi, 0.99, na.rm = TRUE)
      q_low_y <- quantile(pair_df$delta_y, 0.01, na.rm = TRUE)
      q_high_y <- quantile(pair_df$delta_y, 0.99, na.rm = TRUE)
      pair_df <- pair_df[delta_psi >= q_low_psi & delta_psi <= q_high_psi &
                         delta_y >= q_low_y & delta_y <= q_high_y]

      # bin by Δψ
      pair_df[, bin := cut(delta_psi, breaks = 200)]
      binned <- pair_df[, .(
        delta_psi = mean(delta_psi, na.rm = TRUE),
        delta_y = mean(delta_y, na.rm = TRUE),
        n_pairs = .N
      ), by = bin]
      binned <- binned[is.finite(delta_y) & is.finite(delta_psi)]

      # slope
      if (nrow(binned) > 5) {
        model <- lm(delta_y ~ delta_psi, data = binned)
        slope <- coef(summary(model))[2, 1]
        se <- coef(summary(model))[2, 2]
      } else {
        slope <- NA; se <- NA
      }
      slope_label <- sprintf("Slope: %.3f (%.3f)\n%d bins", slope, se, nrow(binned))

      # plot
      p <- ggplot(binned, aes(x = delta_psi, y = delta_y, size = n_pairs)) +
        geom_point(alpha = 0.7, color = "black") +
        geom_smooth(method = "lm", se = FALSE, color = "blue", linetype = "dashed") +
        geom_vline(xintercept = 0, linetype = "dotted") +
        geom_abline(intercept = 0, slope = 1, linetype = "dashed", color = "gray40") +
        annotate("text", x = Inf, y = -Inf, label = slope_label,
                 hjust = 1.1, vjust = -0.2, size = 3.2, color = "black") +
        labs(
          title = paste0("Innovation Jump vs. Market Differential — ",
                         str_replace_all(mover_type, "_", " "), " movers (", label_suffix, ")"),
          x = expression(Delta~psi~" (Dest − Orig, avg innovation)"),
          y = expression(Delta~"mean innovation (Post − Pre, ±5 years)")
        ) +
        theme_minimal(base_size = 13) +
        theme(
          plot.title = element_text(hjust = 0.5, size = 13, face = "bold"),
          axis.title = element_text(size = 13),
          legend.position = "right"
        )

      return(list(plot = p))
    }

    # run both specs
    cat("[INFO] Running LEVEL specification...\n")
    lvl_out <- compute_pair_scatter(df, market_psi_lvl, "n_patents", "levels")
    suffix <- ifelse(label_suffix == "", "", paste0("_", str_trim(gsub(" ", "_", label_suffix))))
    out_file_lvl <- file.path(OUT_DIR, paste0("symmetry_scatter_", mover_type, "_pair_levels", suffix, ".png"))
    ggsave(out_file_lvl, lvl_out$plot, width = 6.5, height = 5, dpi = 300)
    cat("[INFO] Saved:", out_file_lvl, "\n")

    cat("[INFO] Running LOG specification...\n")
    log_out <- compute_pair_scatter(df, market_psi_log, "log_n_patents", "log scale")
    out_file_log <- file.path(OUT_DIR, paste0("symmetry_scatter_", mover_type, "_pair_log", suffix, ".png"))
    ggsave(out_file_log, log_out$plot, width = 6.5, height = 5, dpi = 300)
    cat("[INFO] Saved:", out_file_log, "\n")
  }
}

# =========================
# Run Global Analysis (Full Sample)
# =========================
cat("\n[INFO] Running Global-Value-of-Cities analysis for full sample...\n")
run_global_value_analysis(df_full, "fullsample")

# =========================
# Run Global Analysis (Top Inventors)
# =========================
cat("\n[INFO] Running Global-Value-of-Cities analysis for top inventors...\n")
run_global_value_analysis(df_full_top, "topinventors")

cat("\n[INFO] ✅ Global-Value-of-Cities-style scatter analyses completed for full and top inventors.\n")
