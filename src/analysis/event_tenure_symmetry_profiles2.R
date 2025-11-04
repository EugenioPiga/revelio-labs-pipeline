#!/usr/bin/env Rscript
###############################################################################
# GLOBAL VALUE OF CITIES STYLE — SYMMETRY TESTS (FIRM / CITY / FIRM-WITHIN-CITY)
# Computes binned scatter plots of Δ innovation vs. Δ average market innovation
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

pkgs <- c("arrow","data.table","dplyr","ggplot2","stringr","readr","lubridate","viridis")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos="https://cloud.r-project.org", lib=user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow); library(data.table); library(dplyr)
  library(ggplot2); library(stringr); library(readr); library(lubridate)
  library(viridis)
})

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_tenure_symmetry"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

year_min <- 1990
pre_window <- 5
post_window <- 5

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

# =========================
# Load Data
# =========================
cat("[INFO] Loading dataset...\n")
ds <- arrow::open_dataset(INPUT_DIR, format="parquet")
df_full <- ds %>% dplyr::filter(year >= !!year_min) %>% collect()
setDT(df_full)
df_full[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
df_full[, first_city := na_if(first_city, "empty")]
df_full <- df_full[!is.na(first_rcid) & !is.na(first_city)]
df_full[, log_n_patents := log1p(n_patents)]
cat("[INFO] Rows after filter:", nrow(df_full), "\n")

# =========================
# Final Clean Analysis — Global Value of Cities Style
# =========================
mover_types <- c("firm", "city", "firm_within_city")

for (mover_type in mover_types) {
  cat("\n==========================================\n")
  cat("[INFO] Processing mover type:", mover_type, "\n")
  cat("==========================================\n")

  df <- copy(df_full)
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

  # helper
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
  out_file_lvl <- file.path(OUT_DIR, paste0("symmetry_scatter_", mover_type, "_pair_levels.png"))
  ggsave(out_file_lvl, lvl_out$plot, width = 6.5, height = 5, dpi = 300)
  cat("[INFO] Saved:", out_file_lvl, "\n")

  cat("[INFO] Running LOG specification...\n")
  log_out <- compute_pair_scatter(df, market_psi_log, "log_n_patents", "log scale")
  out_file_log <- file.path(OUT_DIR, paste0("symmetry_scatter_", mover_type, "_pair_log.png"))
  ggsave(out_file_log, log_out$plot, width = 6.5, height = 5, dpi = 300)
  cat("[INFO] Saved:", out_file_log, "\n")
}

cat("\n[INFO] ✅ Global-Value-of-Cities-style scatter analyses completed for all mover types.\n")
