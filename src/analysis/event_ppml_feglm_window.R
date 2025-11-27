#!/usr/bin/env Rscript
###############################################################################
# EVENT STUDY (PPML Δ-scaled using alpaca::feglm)
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

pkgs <- c("arrow","data.table","dplyr","broom","ggplot2","stringr","readr","lubridate")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos="https://cloud.r-project.org", lib=user_lib)
  }
}

if (!requireNamespace("remotes", quietly = TRUE)) {
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
}

if (!requireNamespace("alpaca", quietly = TRUE)) {
  cat("[INFO] Installing alpaca from GitHub...\n")
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib, dependencies = TRUE)
}

suppressPackageStartupMessages({
  library(arrow); library(data.table); library(dplyr); library(broom)
  library(ggplot2); library(stringr); library(readr); library(lubridate); library(alpaca)
})
cat("[DEBUG] alpaca loaded:", "alpaca" %in% loadedNamespaces(), "\n")

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_ppml_feglm"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

year_min <- 1990
T_max    <- 8


# =========================
# Parse command-line arguments for SBATCH
# =========================
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop("Usage: Rscript eventstudy_window.R <window_size>")
}
min_pre  <- as.numeric(args[1])
min_post <- as.numeric(args[1])
suffix   <- paste0("t", min_pre)

cat("\n=========================================\n")
cat("[INFO] Running event-study window ±", min_pre, " (suffix:", suffix, ")\n")
cat("=========================================\n\n")

# =========================
# Helper functions
# =========================
make_market_key <- function(dt, mover_type) {
  switch(
    mover_type,
    "firm"  = dt[, mkt := as.character(first_rcid)],
    "state" = dt[, mkt := as.character(first_state)],
    "metro" = dt[, mkt := as.character(first_metro_area)],
    "firm_within_state" = dt[, mkt := paste0(first_rcid, "||", first_state)],
    "firm_within_metro" = dt[, mkt := paste0(first_rcid, "||", first_metro_area)],  # <-- NEW
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

  im_loo <- imy_tot[, .(mean_loo = mean(mean_loo_my, na.rm = TRUE)),
                    by = .(user_id, mkt)]

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

  od <- merge(od, im_loo, by.x = c("user_id","mkt_origin"), by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_origin_loo")

  od <- merge(od, im_loo, by.x = c("user_id","mkt_dest"), by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_dest_loo")

  od[, delta_loo := mean_dest_loo - mean_origin_loo]
  od <- unique(od[, .(user_id, delta_loo)])

  dt <- merge(dt, od, by = "user_id", all.x = TRUE)
  dt[is.na(delta_loo), delta_loo := 0]
  dt[]
}

# =========================
# Main wrapper to run ±T window
# =========================
run_window <- function(min_pre, min_post, suffix) {
  cat("\n==============================\n")
  cat("[INFO] Running window ±", min_pre, " (suffix:", suffix, ")\n")
  cat("==============================\n")

  prep_event_time <- function(dt) {
    setorder(dt, user_id, year)
    dt[, event_time := fifelse(is.na(move_year), NA_integer_, year - move_year)]

    support <- dt[!is.na(event_time),
                  .(min_r = min(event_time, na.rm=TRUE),
                    max_r = max(event_time, na.rm=TRUE)), by=user_id]
    support[, has_support := (min_r <= -min_pre & max_r >= min_post)]
    dt <- merge(dt, support[,.(user_id,has_support)], by="user_id", all.x=TRUE)
    dt <- dt[(has_support==TRUE | is.na(event_time))]

    dt[, event_time := fifelse(is.na(event_time), 0L,
                               pmax(pmin(event_time, T_max), -T_max))]
    dt[, has_support := NULL]
    dt[]
  }

  run_event <- function(df, mover_type) {
    cat("\n[INFO] Running PPML Δ-scaled event study for:", mover_type, "\n")

    df$event_time <- factor(df$event_time, levels = -T_max:T_max)
    ref_val <- ifelse(-1 %in% df$event_time, -1, 0)
    event_levels <- setdiff(levels(df$event_time), as.character(ref_val))
    safe_levels <- gsub("-", "m", event_levels)

    for (i in seq_along(event_levels)) {
      lvl <- event_levels[i]; safe_lvl <- safe_levels[i]
      df[[paste0("evt_", safe_lvl)]] <- as.numeric(df$event_time == lvl)
      df[[paste0("evt_", safe_lvl, "_delta")]] <- df[[paste0("evt_", safe_lvl)]] * df$delta_loo
    }

    rhs_vars <- c(paste0("evt_", safe_levels), paste0("evt_", safe_levels, "_delta"))
    form <- as.formula(paste("n_patents ~", paste(rhs_vars, collapse = " + "), "| user_id + year"))

    est <- alpaca::feglm(formula = form, data = df,
                         family = poisson(link = "log"),
                         control = alpaca::feglmControl(dev.tol=1e-8, center.tol=1e-8, iter.max=100))
    s <- summary(est, type = "sandwich")

    coef_mat <- if ("cm" %in% names(s)) s$cm else s$coefficients
    coef_df <- as.data.frame(coef_mat)
    coef_df <- tibble::rownames_to_column(coef_df, "term")
    names(coef_df) <- tolower(names(coef_df))
    if ("std. error" %in% names(coef_df)) coef_df <- dplyr::rename(coef_df, std.error = `std. error`)

    coef_df <- coef_df %>%
      mutate(ci_low = estimate - 1.96 * std.error,
             ci_high = estimate + 1.96 * std.error) %>%
      mutate(component = case_when(
               str_detect(term, "_delta$") ~ "Delta_scaled",
               str_detect(term, "^evt_") ~ "Mover_main",
               TRUE ~ "Other"
             ),
             event_time = as.numeric(
               case_when(
                 str_detect(term, "evt_m") ~ -as.numeric(str_extract(term, "\\d+")),
                 TRUE ~ as.numeric(str_extract(term, "\\d+"))
               )
             )) %>%
      filter(component != "Other")

    out_csv <- file.path(OUT_DIR, paste0("coefficients_", mover_type, "_", suffix, ".csv"))
    readr::write_csv(coef_df, out_csv)
    cat("[INFO] Saved coefficients to:", out_csv, "\n")
    invisible(coef_df)
  }

  cat("[INFO] Loading dataset...\n")
  ds <- arrow::open_dataset(INPUT_DIR, format="parquet")
  df <- ds %>% dplyr::filter(year >= !!year_min) %>% collect()
  
  # ============================
  # Filter to U.S. Inventors
  # ============================
  df <- df %>%
    filter(first_country == "United States")

  cat("[INFO] Kept only U.S. inventors:", nrow(df), "rows\n")

  # ============================
  # Restrict to top 10% inventors by lifetime patents (U.S.-only)
  # ============================
  cat("[INFO] Computing lifetime patents (U.S.-only) and 90th percentile cutoff...\n")

  inventor_totals <- df %>%
    group_by(user_id) %>%
    summarise(total_patents = sum(n_patents, na.rm = TRUE), .groups = "drop")

  p90_cutoff <- quantile(inventor_totals$total_patents, 0.90, na.rm = TRUE)
  cat("[INFO] U.S. 90th percentile patent threshold:", round(p90_cutoff, 2), "\n")

  top_inventors <- inventor_totals %>%
    filter(total_patents >= p90_cutoff) %>%
    select(user_id)

  df <- df %>% semi_join(top_inventors, by = "user_id")

  cat("[INFO] Filtered to top 10% U.S. inventors. Remaining rows:", nrow(df), "\n")

  setDT(df)
  df[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
  df[, first_state := na_if(first_state, "empty")]
  df[, first_metro_area  := na_if(first_metro_area, "empty")]
  
  df_state   <- df[!is.na(first_state)]
  df_metro   <- df[!is.na(first_metro_area)]  
  df_firm   <- df[!is.na(first_rcid)]
  df_fwstate <- df[!is.na(first_rcid) & !is.na(first_state)]
  df_fwmetro <- df[!is.na(first_rcid) & !is.na(first_metro_area)]

  for (mover_type in c("state","metro","firm","firm_within_state","firm_within_metro")) {
    dt <- switch(mover_type,
                 "state" = copy(df_state),
                 "metro" = copy(df_metro),
                 "firm" = copy(df_firm),
                 "firm_within_state" = copy(df_fwstate),
                 "firm_within_metro" = copy(df_fwmetro) 
                  )
    dt <- make_market_key(dt, mover_type)
    dt <- tag_first_move(dt)
    dt <- prep_event_time(dt)
    dt <- compute_delta(dt)
    cat("[INFO]", mover_type, ": unique movers =", dt[!is.na(move_year), uniqueN(user_id)], "\n")
    run_event(dt, mover_type)
  }
}

# =========================
# Run single event study
# =========================
run_window(min_pre = min_pre, min_post = min_post, suffix = suffix)

cat("[INFO] Event study ±", min_pre, " completed successfully (suffix:", suffix, ")\n")
