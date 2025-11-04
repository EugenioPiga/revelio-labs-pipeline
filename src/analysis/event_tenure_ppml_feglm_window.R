#!/usr/bin/env Rscript
###############################################################################
# EVENT STUDY (PPML Δ-scaled, controlling for tenure using alpaca::feglm)
# Author: Eugenio — Parallel window-ready version
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
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_tenure_ppml_feglm"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

year_min <- 1990
T_max    <- 8

# =========================
# Parse command-line arguments (window size)
# =========================
args <- commandArgs(trailingOnly = TRUE)
if (length(args) == 0) {
  stop("Usage: Rscript event_study_movers_tenure_window.R <window_size>")
}
min_pre  <- as.numeric(args[1])
min_post <- as.numeric(args[1])
suffix   <- paste0("t", min_pre)

cat("\n=========================================\n")
cat("[INFO] Running TENURE event-study window ±", min_pre, " (suffix:", suffix, ")\n")
cat("=========================================\n\n")

# =========================
# Helper functions
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

compute_delta <- function(dt) {
  data.table::setorder(dt, user_id, year)
  mkt_tot <- dt[, .(sum_pat = sum(n_patents, na.rm = TRUE),
                    n_obs  = .N), by = mkt]
  ui_tot <- dt[, .(sum_pat_ui = sum(n_patents, na.rm = TRUE),
                   n_obs_ui  = .N), by = .(user_id, mkt)]
  ui_tot <- ui_tot[mkt_tot, on = "mkt"]
  ui_tot[, mean_loo := (sum_pat - sum_pat_ui) / pmax(n_obs - n_obs_ui, 1L)]
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
           by = user_id]
  od <- od[!is.na(mkt_origin) & !is.na(mkt_dest)]
  od <- merge(od, ui_tot[, .(user_id, mkt, mean_loo)],
              by.x = c("user_id","mkt_origin"),
              by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_origin_loo")
  od <- merge(od, ui_tot[, .(user_id, mkt, mean_loo)],
              by.x = c("user_id","mkt_dest"),
              by.y = c("user_id","mkt"), all.x = TRUE)
  data.table::setnames(od, "mean_loo", "mean_dest_loo")
  od[, delta_loo := mean_dest_loo - mean_origin_loo]
  od <- unique(od[, .(user_id, delta_loo)])
  dt <- merge(dt, od, by = "user_id", all.x = TRUE)
  dt[is.na(delta_loo), delta_loo := 0]
  dt[]
}

prep_event_time <- function(dt) {
  setorder(dt, user_id, year)
  dt[, event_time := fifelse(is.na(move_year), NA_integer_, year - move_year)]
  support <- dt[!is.na(event_time),
                .(min_r=min(event_time,na.rm=TRUE),
                  max_r=max(event_time,na.rm=TRUE)), by=user_id]
  support[, has_support := (min_r <= -min_pre & max_r >= min_post)]
  dt <- merge(dt, support[,.(user_id,has_support)], by="user_id", all.x=TRUE)
  dt <- dt[(has_support==TRUE | is.na(event_time))]
  dt[, event_time := fifelse(is.na(event_time), 0L,
                             pmax(pmin(event_time, T_max), -T_max))]
  dt[, has_support := NULL]
  dt[]
}

# ---- PPML Event Study (alpaca::feglm with tenure controls) ----
run_event <- function(df, mover_type) {
  cat("\n[INFO] Running PPML Δ-scaled event study (with tenure) for:", mover_type, "\n")
  df$event_time <- factor(df$event_time, levels = -T_max:T_max)
  ref_val <- ifelse(-1 %in% df$event_time, -1, 0)
  event_levels <- setdiff(levels(df$event_time), as.character(ref_val))
  safe_levels <- gsub("-", "m", event_levels)
  for (i in seq_along(event_levels)) {
    lvl <- event_levels[i]; safe_lvl <- safe_levels[i]
    df[[paste0("evt_", safe_lvl)]] <- as.numeric(df$event_time == lvl)
    df[[paste0("evt_", safe_lvl, "_delta")]] <- df[[paste0("evt_", safe_lvl)]] * df$delta_loo
  }
  rhs_vars <- c(paste0("evt_", safe_levels),
                paste0("evt_", safe_levels, "_delta"),
                "tenure", "tenure_sq")
  form <- as.formula(paste("n_patents ~", paste(rhs_vars, collapse = " + "), "| user_id + year"))
  est <- alpaca::feglm(formula = form, data = df,
                       family = poisson(link = "log"),
                       control = alpaca::feglmControl(dev.tol=1e-8, center.tol=1e-8,
                                                      iter.max=100, trace=TRUE))
  s <- summary(est, type = "sandwich")
  coef_mat <- if ("cm" %in% names(s)) s$cm else s$coefficients
  coef_df <- as.data.frame(coef_mat)
  coef_df <- tibble::rownames_to_column(coef_df, "term")
  names(coef_df) <- tolower(names(coef_df))
  if ("std. error" %in% names(coef_df))
    coef_df <- dplyr::rename(coef_df, std.error = `std. error`)
  coef_df <- coef_df %>%
    mutate(ci_low = estimate - 1.96 * std.error,
           ci_high = estimate + 1.96 * std.error) %>%
    mutate(component = case_when(
             str_detect(term, "_delta$") ~ "Delta_scaled",
             str_detect(term, "^evt_") ~ "Mover_main",
             TRUE ~ "Other"),
           event_time = as.numeric(case_when(
             str_detect(term, "evt_m") ~ -as.numeric(str_extract(term, "\\d+")),
             TRUE ~ as.numeric(str_extract(term, "\\d+"))))) %>%
    filter(component != "Other")
  out_csv <- file.path(OUT_DIR, paste0("coefficients_", mover_type,
                                       "_tenure_", suffix, ".csv"))
  readr::write_csv(coef_df, out_csv)
  cat("[INFO] Saved coefficients (with SEs & CI) to:", out_csv, "\n")
  invisible(coef_df)
}

# =========================
# Load & Prepare Data
# =========================
cat("[INFO] Loading dataset...\n")
ds <- arrow::open_dataset(INPUT_DIR, format="parquet")
df <- ds %>% dplyr::filter(year >= !!year_min) %>% collect()
setDT(df)
cat("[INFO] Rows after year filter:", nrow(df), "\n")
df[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
df[, first_city := na_if(first_city, "empty")]

# =========================
# Construct Tenure Controls
# =========================
cat("[INFO] Constructing tenure controls...\n")
df[, edu_start_year := year(first_startdate_edu)]
df[, pos_start_year := year(first_startdate_pos)]
df[, tenure := year - edu_start_year + 3]
df[, tenure := fifelse(is.na(tenure) | tenure > 50, year - pos_start_year, tenure)]
df[, tenure := fifelse(tenure > 50 | tenure < 0, NA_real_, tenure)]
df[, tenure_sq := tenure^2]
df <- df[!is.na(tenure)]
cat("[INFO] Tenure controls created. Rows remaining:", nrow(df), "\n")

# =========================
# Subset Markets
# =========================
df_city   <- df[!is.na(first_city)]
df_firm   <- df[!is.na(first_rcid)]
df_fwcity <- df[!is.na(first_rcid) & !is.na(first_city)]
cat("[INFO] Subsets -> City:", nrow(df_city),
    "| Firm:", nrow(df_firm),
    "| Firm-within-city:", nrow(df_fwcity), "\n")

# =========================
# Run Each Event Study
# =========================
for (mover_type in c("city","firm","firm_within_city")) {
  dt <- switch(mover_type,
               "city" = copy(df_city),
               "firm" = copy(df_firm),
               "firm_within_city" = copy(df_fwcity))
  dt <- make_market_key(dt, mover_type)
  dt <- tag_first_move(dt)
  dt <- prep_event_time(dt)
  dt <- compute_delta(dt)
  cat("[INFO]", mover_type, ": unique movers =", dt[!is.na(move_year), uniqueN(user_id)], "\n")
  run_event(dt, mover_type)
}

cat("[INFO] ✅ Event study with tenure controls (feglm) completed successfully.\n")
