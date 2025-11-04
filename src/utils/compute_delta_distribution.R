#!/usr/bin/env Rscript
###############################################################################
# COMPUTE & PLOT DISTRIBUTION OF Δ (delta_loo)
# Author: Eugenio — Lightweight version (no event-study estimation)
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","data.table","dplyr","ggplot2","stringr","readr")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
suppressPackageStartupMessages(lapply(pkgs, library, character.only = TRUE))

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR   <- "/home/epiga/revelio_labs/output/delta_distribution"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)
year_min  <- 1990

# =========================
# Helper Functions
# =========================
make_market_key <- function(dt, mover_type) {
  switch(
    mover_type,
    "firm"            = dt[, mkt := as.character(first_rcid)],
    "city"            = dt[, mkt := as.character(first_city)],
    "firm_within_city"= dt[, mkt := paste0(first_rcid, "||", first_city)],
    stop("Unknown mover_type")
  )
  dt[]
}

tag_first_move <- function(dt) {
  setorder(dt, user_id, year)
  dt[, mkt_lag := shift(mkt), by = user_id]
  dt[, moved := (!is.na(mkt_lag) & mkt != mkt_lag)]
  move_tbl <- dt[moved == TRUE, .(move_year = min(year, na.rm = TRUE)), by = user_id]
  dt[, c("mkt_lag","moved") := NULL]
  merge(dt, move_tbl, by = "user_id", all.x = TRUE)
}

compute_delta <- function(dt) {
  data.table::setorder(dt, user_id, year)

  my_tot <- dt[, .(sum_pat_my = sum(n_patents, na.rm = TRUE),
                   n_obs_my  = .N),
               by = .(mkt, year)]

  imy_tot <- dt[, .(sum_pat_imy = sum(n_patents, na.rm = TRUE),
                    n_obs_imy  = .N),
                by = .(user_id, mkt, year)]

  imy_tot <- imy_tot[my_tot, on = .(mkt, year)]
  imy_tot[, mean_loo_my := (sum_pat_my - sum_pat_imy) / pmax(n_obs_my - n_obs_imy, 1L)]

  im_loo <- imy_tot[, .(mean_loo = mean(mean_loo_my, na.rm = TRUE)), by = .(user_id, mkt)]

  od <- dt[!is.na(move_year),
           .(
             mkt_origin = {
               yrs <- year[year < move_year & !is.na(mkt)]
               if (length(yrs) == 0) NA_character_ else mkt[year == max(yrs)]
             },
             mkt_dest = {
               yrs <- year[year > move_year & !is.na(mkt)]
               if (length(yrs) == 0) NA_character_ else mkt[year == min(yrs)]
             }
           ),
           by = user_id][!is.na(mkt_origin) & !is.na(mkt_dest)]

  od <- merge(od, im_loo, by.x = c("user_id","mkt_origin"),
              by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_origin_loo")

  od <- merge(od, im_loo, by.x = c("user_id","mkt_dest"),
              by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_dest_loo")

  od[, delta_loo := mean_dest_loo - mean_origin_loo]
  unique(od[, .(user_id, delta_loo)])
}

# =========================
# Load Data
# =========================
cat("[INFO] Loading dataset...\n")
ds <- arrow::open_dataset(INPUT_DIR, format = "parquet")
df <- ds %>% dplyr::filter(year >= !!year_min) %>% collect()
setDT(df)
df[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
df[, first_city := na_if(first_city, "empty")]

# =========================
# Compute Δ per mover type
# =========================
for (mover_type in c("city", "firm", "firm_within_city")) {
  cat("\n[INFO] Processing:", mover_type, "\n")

  dt <- switch(
    mover_type,
    "city" = copy(df[!is.na(first_city)]),
    "firm" = copy(df[!is.na(first_rcid)]),
    "firm_within_city" = copy(df[!is.na(first_rcid) & !is.na(first_city)])
  )

  dt <- make_market_key(dt, mover_type)
  dt <- tag_first_move(dt)
  delta_dt <- compute_delta(dt)

  # Summary statistics
  var_delta  <- var(delta_dt$delta_loo, na.rm = TRUE)
  mean_delta <- mean(delta_dt$delta_loo, na.rm = TRUE)
  sd_delta   <- sd(delta_dt$delta_loo, na.rm = TRUE)
  cat("[INFO] Mean(Δ) =", round(mean_delta, 4),
      " Var(Δ) =", round(var_delta, 4),
      " SD(Δ) =", round(sd_delta, 4), "\n")

  # Save summary
  stats_path <- file.path(OUT_DIR, paste0("delta_stats_", mover_type, ".csv"))
  fwrite(data.table(mean_delta, var_delta, sd_delta), stats_path)

  # Plot distribution
  p <- ggplot(delta_dt, aes(x = delta_loo)) +
    geom_histogram(aes(y = ..density..), bins = 60, fill = "#0072B2", alpha = 0.7) +
    geom_vline(xintercept = 0, color = "red", linetype = "dashed") +
    geom_density(color = "black", linewidth = 1) +
    labs(
      title = paste0("Distribution of Δ (", mover_type, " movers)"),
      subtitle = paste0("Var(Δ) = ", round(var_delta, 4)),
      x = "Δ (mean_loo destination − origin)",
      y = "Density"
    ) +
    theme_minimal(base_size = 14)

  out_png <- file.path(OUT_DIR, paste0("delta_distribution_", mover_type, ".png"))
  ggsave(out_png, p, width = 8, height = 5, dpi = 300)
  cat("[INFO] Saved plot to:", out_png, "\n")
}

cat("\n[INFO] ✅ Δ distributions and variances computed successfully.\n")
