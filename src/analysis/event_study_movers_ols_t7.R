#!/usr/bin/env Rscript
###############################################################################
# event_study_movers_ols_t7.R
# OLS version of the Δ-scaled event study (parallel to PPML)
# Author: Eugenio — 2025-10-21
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")
options(fixest.cores = 4)
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow", "data.table", "dplyr", "fixest", "broom", "ggplot2", "stringr", "readr")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos="https://cloud.r-project.org", lib=user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(data.table); library(dplyr); library(fixest)
  library(broom); library(ggplot2); library(stringr); library(readr)
})
data.table::setDTthreads(4)

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_ols_physician"
dir.create(OUT_DIR, showWarnings=FALSE, recursive=TRUE)

year_min  <- 1990
T_max     <- 8
min_pre   <- 7
min_post  <- 7
N_SHARDS  <- 20

args  <- commandArgs(trailingOnly=TRUE)
shard <- ifelse(length(args)==0, 0, as.integer(args[1]))
cat("[INFO] Starting shard", shard, "of", N_SHARDS, "\n")

# =========================
# Helper functions
# =========================

make_market_key <- function(dt, mover_type) {
  if (!"data.table" %in% class(dt)) setDT(dt)
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
  dt <- merge(dt, move_tbl, by = "user_id", all.x = TRUE)
  dt[, c("mkt_lag","moved") := NULL]
  dt[]
}

compute_delta <- function(dt) {
  setorder(dt, user_id, year)

  bad_cols <- grep("^move_year", names(dt), value = TRUE)
  if (length(bad_cols) > 1) dt[, (bad_cols[-1]) := NULL]

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
  cat("[INFO] compute_delta(): valid movers =", nrow(od), "\n")

  mkt_means <- dt[, .(mkt_mean_pat = mean(n_patents, na.rm = TRUE)), by = mkt]
  setkey(mkt_means, mkt)

  od <- merge(od, mkt_means, by.x = "mkt_origin", by.y = "mkt", all.x = TRUE)
  setnames(od, "mkt_mean_pat", "mean_origin")
  od <- merge(od, mkt_means, by.x = "mkt_dest", by.y = "mkt", all.x = TRUE)
  setnames(od, "mkt_mean_pat", "mean_dest")

  od[, delta := mean_dest - mean_origin]
  od <- unique(od[, .(user_id, delta)])
  dt <- merge(dt, od, by = "user_id", all.x = TRUE)
  dt[is.na(delta), delta := 0]
  cat("[INFO] Δ computed. Nonzero deltas:", dt[delta != 0, uniqueN(user_id)], "\n")
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

# ---- OLS Event Study ----
run_event <- function(df, mover_type, shard) {
  cat("\n[INFO] Running OLS Δ-scaled event study for:", mover_type, "\n")

  ref_val <- ifelse(-1 %in% df$event_time, -1, 0)
  df$event_time <- factor(df$event_time, levels = -T_max:T_max)
  df$delta[is.na(df$delta)] <- 0

  cat("[CHECK] Δ summary:\n"); print(summary(df$delta))
  cat("[CHECK] unique Δ values:", length(unique(df$delta)), "\n")

  # === OLS ===
  est <- feols(
    n_patents ~ i(event_time, ref = ref_val) + i(event_time, ref = ref_val):delta |
      user_id + year,
    data = df, cluster = ~user_id, nthreads = 4
  )

  tidy_all <- broom::tidy(est) |>
    mutate(
      component = case_when(
        str_detect(term, "event_time::") & !str_detect(term, "event_time::.*:delta") ~ "Mover_main",
        str_detect(term, "event_time::.*:delta") ~ "Delta_scaled",
        TRUE ~ "Other"
      ),
      event_time = as.integer(str_extract(term, "-?\\d+"))
    ) |>
    filter(component != "Other")

  out_csv <- file.path(OUT_DIR, paste0("coefficients_", mover_type, "_t7", "_shard", shard, ".csv"))
  write_csv(tidy_all, out_csv)
  cat("[INFO] Saved coefficients to:", out_csv, "\n")

  invisible(tidy_all)
}

# =========================
# Load & Clean Data
# =========================
cat("[INFO] Loading shard", shard, "\n")
ds <- arrow::open_dataset(INPUT_DIR, format="parquet")
df <- ds %>%
  dplyr::filter(year >= !!year_min) %>%
  collect()
setDT(df)

cat("[INFO] Raw rows:", nrow(df), "\n")
df <- df[(user_id %% N_SHARDS) == shard]
cat("[INFO] Rows after user-based sharding:", nrow(df), "\n")

df[, n_patents := fifelse(is.na(n_patents), 0, n_patents)]
df[, first_city := na_if(first_city, "empty")]

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
  dt <- switch(
    mover_type,
    "city" = copy(df_city),
    "firm" = copy(df_firm),
    "firm_within_city" = copy(df_fwcity)
  )

  dt <- make_market_key(dt, mover_type)
  dt <- tag_first_move(dt)
  dt <- prep_event_time(dt)
  dt <- compute_delta(dt)

  cat("[INFO]", mover_type, ": unique movers =", dt[!is.na(move_year), uniqueN(user_id)], "\n")
  run_event(dt, mover_type, shard)
}

cat("[INFO] All done ✅\n")
