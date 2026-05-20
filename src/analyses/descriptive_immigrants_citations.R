#!/usr/bin/env Rscript
###############################################################################
# Descriptives — CITATIONS VERSION (Immigrants vs Natives)
#
# What changes vs your patents script:
#  - Top10 filter STILL by total patents (n_patents).
#  - Outcomes + “cluster productivity” use citations:
#       y_it      = log1p(n_us_citations_it)
#       y_it_pp   = log1p(n_us_citations_it / max(1, n_patents_it))   [quality-per-patent]
#       avg_prod  = mean( log1p(n_us_citations_it) ) within cluster-year
#  - Leave-one-out cluster productivity: avg_prod(-g)
#       computed at inventor-year level and then averaged within group×cluster
#  - More stable inference:
#       - min support for group cells
#       - drop high leverage points (hat values) before computing slope tables
#       - robust vcov fallback
#
# Output:
#   OUT_DIR/plots_citations/...
#   OUT_DIR/tables_citations/...
###############################################################################

# =========================
# Setup
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","tidyr","readr","ggplot2","stringr","sandwich","lmtest","scales")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(readr)
  library(ggplot2)
  library(stringr)
  library(sandwich)
  library(lmtest)
  library(scales)
})

set.seed(123)

# =========================
# Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants"

PLOTS_DIR  <- file.path(OUT_DIR, "plots_citations")
TABLES_DIR <- file.path(OUT_DIR, "tables_citations")
dir.create(PLOTS_DIR,  recursive = TRUE, showWarnings = FALSE)
dir.create(TABLES_DIR, recursive = TRUE, showWarnings = FALSE)

MIN_CLUSTER <- 10
USE_LAST_N_YEARS <- 10

# Support / stability
MIN_GROUP_SUPPORT <- 5     # min inventors in (group × cluster-year) cell
MIN_SUBCAT_SUPPORT <- 8    # min inventors in (subcat × cluster-year)
DROP_LEVERAGE <- TRUE
HAT_CUTOFF_RULE <- "min(0.5, 4/n)"  # typical rule of thumb
MIN_N_FOR_SLOPE <- 14
MIN_UNIQUE_X <- 4
MIN_UNIQUE_X_PER_GROUP <- 2

# Tenure
TENURE_MAX <- 50
TENURE_BIN_WIDTH <- 10

# Facets
TOP_N_FACETS  <- 9
TOP_N_ORIGINS <- 9

# Optional: citations are thin in very recent years -> you can skip
SKIP_YEARS <- c(2023, 2024, 2025)
#SKIP_YEARS <- integer(0)

geos <- list(
  state = "first_state",
  metro = "first_metro_area"
)

immig_defs <- c(
  "immig_first_deg_or_job_nonUS"
)

# =========================
# Helpers
# =========================
need_cols <- function(ds, cols) {
  nm <- names(ds)
  missing <- setdiff(cols, nm)
  if (length(missing) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(missing, collapse = ", ")))
}

wmean <- function(x, w) {
  ok <- is.finite(x) & is.finite(w) & !is.na(x) & !is.na(w) & w > 0
  if (!any(ok)) return(NA_real_)
  sum(x[ok] * w[ok]) / sum(w[ok])
}

wquantile <- function(x, w, probs = c(0.1, 0.9)) {
  ok <- is.finite(x) & is.finite(w) & !is.na(x) & !is.na(w) & w > 0
  x <- x[ok]; w <- w[ok]
  if (length(x) == 0) return(rep(NA_real_, length(probs)))
  o <- order(x)
  x <- x[o]; w <- w[o]
  cw <- cumsum(w) / sum(w)
  sapply(probs, function(p) x[which(cw >= p)[1]])
}

compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure)
    )
}

make_tenure_bins <- function(tenure, width = TENURE_BIN_WIDTH, maxv = TENURE_MAX) {
  br <- seq(0, maxv, by = width)
  cut(tenure, breaks = br, right = FALSE, include.lowest = TRUE)
}

# Robust VCOV (fallback chain)
vcov_robust <- function(m) {
  V <- tryCatch(sandwich::vcovHC(m, type = "HC3"), error = function(e) NULL)
  if (is.null(V)) V <- tryCatch(sandwich::vcovHC(m, type = "HC1"), error = function(e) NULL)
  if (is.null(V)) V <- tryCatch(vcov(m), error = function(e) NULL)
  V
}

# Drop high leverage points (hat values)
drop_high_leverage <- function(df, fml, wvar) {
  m0 <- tryCatch(lm(fml, data = df, weights = df[[wvar]]), error = function(e) NULL)
  if (is.null(m0)) return(df)

  h <- tryCatch(hatvalues(m0), error = function(e) NULL)
  if (is.null(h)) return(df)

  n <- length(h)
  cutoff <- min(0.5, 4 / n)
  keep <- is.finite(h) & (h < cutoff)
  df[keep, , drop = FALSE]
}

# Slope table per facet (interaction model), with:
#  - x centered at weighted mean
#  - optional high-leverage drop
slope_table_by_facet <- function(df, xvar, yvar, wvar = "cluster_size") {
  req <- c("group", xvar, yvar, wvar)
  if (!all(req %in% names(df))) return(tibble())

  d <- df %>%
    filter(is.finite(.data[[xvar]]), is.finite(.data[[yvar]]), is.finite(.data[[wvar]]), .data[[wvar]] > 0) %>%
    mutate(group = droplevels(group))

  if (n_distinct(d$group) < 2) return(tibble())
  if (nrow(d) < MIN_N_FOR_SLOPE) return(tibble())
  if (n_distinct(d[[xvar]]) < MIN_UNIQUE_X) return(tibble())

  gx <- d %>% group_by(group) %>% summarise(n=n(), kx=n_distinct(.data[[xvar]]), .groups="drop")
  if (any(gx$n < 2) || any(gx$kx < MIN_UNIQUE_X_PER_GROUP)) return(tibble())

  xbar <- wmean(d[[xvar]], d[[wvar]])
  d <- d %>% mutate(xc = .data[[xvar]] - xbar)

  fml <- as.formula(paste0(yvar, " ~ xc * group"))

  if (DROP_LEVERAGE) {
    d2 <- drop_high_leverage(d, fml, wvar)
    # if we dropped too much, fallback to original
    if (nrow(d2) >= MIN_N_FOR_SLOPE) d <- d2
  }

  m <- tryCatch(lm(fml, data = d, weights = d[[wvar]]), error = function(e) NULL)
  if (is.null(m)) return(tibble())

  b <- coef(m)
  V <- vcov_robust(m)
  if (is.null(V)) return(tibble())

  # robust term lookup
  term_x <- "xc"
  term_ix <- grep("^xc:group", names(b), value = TRUE)
  if (length(term_ix) == 0) return(tibble())
  term_ix <- term_ix[1]

  term_g <- grep("^group", names(b), value = TRUE)
  term_g <- if (length(term_g) > 0) term_g[1] else NA_character_

  rn <- rownames(V)
  if (!(term_x %in% rn && term_ix %in% rn)) return(tibble())

  vx  <- V[term_x, term_x]
  vix <- V[term_ix, term_ix]
  cxi <- V[term_x, term_ix]
  if (!is.finite(vx) || !is.finite(vix) || vx <= 0 || vix < 0) return(tibble())

  slope_native <- unname(b[term_x])
  se_native    <- sqrt(vx)

  slope_immig  <- unname(b[term_x] + b[term_ix])
  se_immig_sq  <- vx + vix + 2*cxi
  if (!is.finite(se_immig_sq) || se_immig_sq <= 0) return(tibble())
  se_immig <- sqrt(se_immig_sq)

  diff_slope <- unname(b[term_ix])
  se_diff_sq <- vix
  if (!is.finite(se_diff_sq) || se_diff_sq <= 0) return(tibble())
  se_diff <- sqrt(se_diff_sq)

  base_gap <- if (!is.na(term_g) && term_g %in% names(b)) unname(b[term_g]) else NA_real_

  qs <- wquantile(d[[xvar]], d[[wvar]], probs = c(0.1, 0.9))
  x10 <- qs[1]; x90 <- qs[2]
  slope_term_p10 <- diff_slope * (x10 - xbar)
  slope_term_p90 <- diff_slope * (x90 - xbar)

  share_slope_p10 <- ifelse(is.finite(base_gap) && is.finite(slope_term_p10) &&
                              (abs(base_gap) + abs(slope_term_p10)) > 0,
                            abs(slope_term_p10) / (abs(base_gap) + abs(slope_term_p10)),
                            NA_real_)

  share_slope_p90 <- ifelse(is.finite(base_gap) && is.finite(slope_term_p90) &&
                              (abs(base_gap) + abs(slope_term_p90)) > 0,
                            abs(slope_term_p90) / (abs(base_gap) + abs(slope_term_p90)),
                            NA_real_)

  tibble(
    slope_native = slope_native,
    se_native = se_native,
    p_native = 2 * pnorm(abs(slope_native / se_native), lower.tail = FALSE),

    slope_immigrant = slope_immig,
    se_immigrant = se_immig,
    p_immigrant = 2 * pnorm(abs(slope_immig / se_immig), lower.tail = FALSE),

    slope_diff = diff_slope,
    se_diff = se_diff,
    p_diff = 2 * pnorm(abs(diff_slope / se_diff), lower.tail = FALSE),

    base_gap_at_meanx = base_gap,
    xbar = xbar,
    x10 = x10,
    x90 = x90,
    slope_term_p10 = slope_term_p10,
    slope_term_p90 = slope_term_p90,
    share_slope_p10 = share_slope_p10,
    share_slope_p90 = share_slope_p90,
    dropped_leverage = DROP_LEVERAGE
  )
}

# =========================
# Plot helpers
# =========================
plot_facet_group_lines <- function(df, facet_var, xvar, yvar, title, subtitle, outpath,
                                  wvar = "cluster_size", psize = "n_group",
                                  ncol = 3, free_y = TRUE) {
  if (nrow(df) == 0) return(invisible(NULL))
  scales_y <- if (free_y) "free_y" else "fixed"

  p <- ggplot(df, aes(x = .data[[xvar]], y = .data[[yvar]], color = group)) +
    geom_point(aes(size = .data[[psize]]), alpha = 0.28) +
    geom_smooth(method = "lm", aes(weight = .data[[wvar]]), se = FALSE) +
    facet_wrap(as.formula(paste0("~", facet_var)), ncol = ncol, scales = scales_y) +
    scale_size_continuous(range = c(0.7, 5.5), guide = "none") +
    labs(title = title, subtitle = subtitle, x = xvar, y = yvar, color = "Group") +
    theme_minimal()

  ggsave(outpath, p, width = 12.5, height = 7.8, dpi = 220)
  invisible(NULL)
}

plot_facet_share <- function(df_one, facet_var, xvar, yvar, title, subtitle, outpath,
                             wvar = "cluster_size", ncol = 3, free_y = FALSE) {
  if (nrow(df_one) == 0) return(invisible(NULL))
  scales_y <- if (free_y) "free_y" else "fixed"

  p <- ggplot(df_one, aes(x = .data[[xvar]], y = .data[[yvar]])) +
    geom_point(aes(size = .data[[wvar]]), alpha = 0.25) +
    geom_smooth(method = "lm", aes(weight = .data[[wvar]]), se = FALSE) +
    facet_wrap(as.formula(paste0("~", facet_var)), ncol = ncol, scales = scales_y) +
    scale_size_continuous(range = c(0.7, 5.5), guide = "none") +
    labs(title = title, subtitle = subtitle, x = xvar, y = yvar) +
    theme_minimal()

  ggsave(outpath, p, width = 12.5, height = 7.8, dpi = 220)
  invisible(NULL)
}

# =========================
# Build base slice for (year, geo, immig_def)
# =========================
build_base_slice <- function(ds, year_val, geo_var, immig_var, first_pos_map) {

  base_cols <- c(
    "user_id","year","n_patents","n_us_citations","first_country",
    geo_var, immig_var,
    "career_modal_section","career_modal_class",
    "first_startdate_edu","first_startdate_pos",
    "first_university_country",
    "first_state","first_metro_area"
  )
  need_cols(ds, c("user_id","year","n_patents","n_us_citations","first_country", geo_var, immig_var))

  base <- ds %>%
    select(any_of(base_cols)) %>%
    filter(year == year_val) %>%
    filter(first_country == "United States") %>%
    filter(!is.na(.data[[geo_var]])) %>%
    mutate(immig_flag = as.integer(.data[[immig_var]])) %>%
    filter(immig_flag %in% c(0L, 1L)) %>%
    collect()

  if (nrow(base) == 0) return(tibble())

  base %>%
    left_join(first_pos_map, by = "user_id") %>%
    mutate(
      geo_level = as.character(.data[[geo_var]]),
      n_patents = as.numeric(n_patents),
      n_us_citations = as.numeric(n_us_citations),
      # outcome measures (recommended)
      y_cites = log1p(pmax(n_us_citations, 0)),
      cites_per_pat = pmax(n_us_citations, 0) / pmax(1, pmax(n_patents, 0)),
      y_cites_pp = log1p(cites_per_pat)
    ) %>%
    filter(!is.na(geo_level), geo_level != "") %>%
    filter(is.finite(y_cites), is.finite(y_cites_pp))
}

# =========================
# Cluster stats (citations-based productivity)
#   avg_prod = mean(log1p(citations)) in cluster-year
#   plus leave-one-out version at inventor-year, merged back later
# =========================
compute_cluster_stats_cites <- function(base, min_cluster = MIN_CLUSTER) {
  cs <- base %>%
    group_by(geo_level) %>%
    summarise(
      cluster_size = n_distinct(user_id),
      sum_y  = sum(y_cites, na.rm = TRUE),
      avg_prod = mean(y_cites, na.rm = TRUE),
      n_immigrant = n_distinct(user_id[immig_flag == 1L]),
      immig_share = ifelse(cluster_size > 0, n_immigrant / cluster_size, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(
      log_cluster = log(cluster_size)
    ) %>%
    filter(cluster_size >= min_cluster) %>%
    filter(is.finite(log_cluster), is.finite(avg_prod))

  cs
}

# inventor-level leave-one-out x: (sum_y - y_i)/(N-1)
attach_loo_prod <- function(base, cluster_stats) {
  base %>%
    inner_join(cluster_stats %>% select(geo_level, cluster_size, sum_y), by = "geo_level") %>%
    mutate(
      loo_avg_prod = ifelse(cluster_size > 1,
                            (sum_y - y_cites) / (cluster_size - 1),
                            NA_real_)
    )
}

# =========================
# Generic subgroup builder (citations outcomes)
# =========================
build_subcat_long <- function(base_with_loo, cluster_stats, cat_var,
                             top_n = TOP_N_FACETS,
                             keep_all_levels = FALSE,
                             cat_filter = NULL,
                             min_group_support = MIN_GROUP_SUPPORT,
                             min_subcat_support = MIN_SUBCAT_SUPPORT) {

  if (!(cat_var %in% names(base_with_loo))) return(list(long = tibble(), one = tibble(), levels = character(0)))

  bb <- base_with_loo %>%
    mutate(cat = as.character(.data[[cat_var]])) %>%
    mutate(cat = str_trim(cat)) %>%
    filter(!is.na(cat), cat != "", cat != "empty")

  if (!is.null(cat_filter)) bb <- bb %>% filter(cat_filter(cat))
  if (nrow(bb) == 0) return(list(long = tibble(), one = tibble(), levels = character(0)))

  # facet levels: top by immigrant counts
  if (keep_all_levels) {
    levs <- sort(unique(bb$cat))
  } else {
    levs <- bb %>%
      filter(immig_flag == 1L) %>%
      group_by(cat) %>%
      summarise(total_imm = n_distinct(user_id), .groups = "drop") %>%
      arrange(desc(total_imm)) %>%
      slice_head(n = top_n) %>%
      pull(cat)
  }
  if (length(levs) == 0) return(list(long = tibble(), one = tibble(), levels = character(0)))

  bb <- bb %>% filter(cat %in% levs)

  # subgroup totals per (cluster x subcat)
  sub_tot <- bb %>%
    group_by(geo_level, cat) %>%
    summarise(
      n_sub_total = n_distinct(user_id),
      n_sub_imm   = n_distinct(user_id[immig_flag == 1L]),
      immig_share_in_sub = ifelse(n_sub_total > 0, n_sub_imm / n_sub_total, NA_real_),
      .groups = "drop"
    ) %>%
    filter(n_sub_total >= min_subcat_support)

  if (nrow(sub_tot) == 0) return(list(long = tibble(), one = tibble(), levels = levs))

  # group means within subgroup
  long <- bb %>%
    mutate(group = factor(ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
                          levels = c("nonimmigrant","immigrant"))) %>%
    group_by(geo_level, cat, group) %>%
    summarise(
      avg_y_cites    = mean(y_cites, na.rm = TRUE),
      avg_y_cites_pp = mean(y_cites_pp, na.rm = TRUE),
      # leave-one-out x, averaged within group
      avg_loo_prod   = mean(loo_avg_prod, na.rm = TRUE),
      n_group        = n_distinct(user_id),
      .groups = "drop"
    ) %>%
    filter(n_group >= min_group_support) %>%
    left_join(cluster_stats, by = "geo_level") %>%
    inner_join(sub_tot, by = c("geo_level","cat")) %>%
    filter(is.finite(avg_y_cites), is.finite(avg_y_cites_pp)) %>%
    mutate(cat = factor(cat, levels = levs))

  one <- sub_tot %>%
    left_join(cluster_stats, by = "geo_level") %>%
    mutate(cat = factor(cat, levels = levs)) %>%
    filter(!is.na(immig_share_in_sub), is.finite(immig_share_in_sub))

  list(long = long, one = one, levels = levs)
}

# =========================
# Origin builder (special: natives don’t have “origin”)
# =========================
build_origin_long <- function(base_with_loo, cluster_stats, top_n = TOP_N_ORIGINS,
                              min_imm_in_origin_cluster = MIN_GROUP_SUPPORT) {

  needed <- c("first_university_country","first_pos_country")
  if (!all(needed %in% names(base_with_loo))) return(list(long = tibble(), one = tibble(), levels = character(0)))

  imm <- base_with_loo %>%
    filter(immig_flag == 1L) %>%
    mutate(
      edu_c = str_trim(as.character(first_university_country)),
      pos_c = str_trim(as.character(first_pos_country)),
      edu_missing = is.na(edu_c) | edu_c %in% c("", "empty"),
      pos_missing = is.na(pos_c) | pos_c %in% c("", "empty"),
      edu_is_us = !edu_missing & edu_c == "United States",
      pos_is_us = !pos_missing & pos_c == "United States",
      edu_nonUS = !edu_missing & !edu_is_us,
      pos_nonUS = !pos_missing & !pos_is_us,
      origin = dplyr::case_when(
        edu_nonUS ~ edu_c,
        !edu_nonUS & pos_nonUS ~ pos_c,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(origin), origin != "")

  if (nrow(imm) == 0) return(list(long = tibble(), one = tibble(), levels = character(0)))

  levs <- imm %>%
    group_by(origin) %>%
    summarise(total_imm = n_distinct(user_id), .groups="drop") %>%
    arrange(desc(total_imm)) %>%
    slice_head(n = top_n) %>%
    pull(origin)

  if (length(levs) == 0) return(list(long = tibble(), one = tibble(), levels = character(0)))

  out_long <- list()
  out_one  <- list()

  for (o in levs) {
    cl_o <- imm %>%
      filter(origin == o) %>%
      group_by(geo_level) %>%
      summarise(n_imm_o = n_distinct(user_id), .groups="drop") %>%
      filter(n_imm_o >= min_imm_in_origin_cluster)

    if (nrow(cl_o) == 0) next

    imm_o <- imm %>%
      filter(origin == o, geo_level %in% cl_o$geo_level) %>%
      group_by(geo_level) %>%
      summarise(
        avg_y_cites    = mean(y_cites, na.rm = TRUE),
        avg_y_cites_pp = mean(y_cites_pp, na.rm = TRUE),
        avg_loo_prod   = mean(loo_avg_prod, na.rm = TRUE),
        n_group        = n_distinct(user_id),
        .groups = "drop"
      ) %>%
      mutate(group = "immigrant")

    nat_o <- base_with_loo %>%
      filter(immig_flag == 0L, geo_level %in% cl_o$geo_level) %>%
      group_by(geo_level) %>%
      summarise(
        avg_y_cites    = mean(y_cites, na.rm = TRUE),
        avg_y_cites_pp = mean(y_cites_pp, na.rm = TRUE),
        avg_loo_prod   = mean(loo_avg_prod, na.rm = TRUE),
        n_group        = n_distinct(user_id),
        .groups = "drop"
      ) %>%
      mutate(group = "nonimmigrant")

    long_o <- bind_rows(nat_o, imm_o) %>%
      left_join(cluster_stats, by = "geo_level") %>%
      mutate(
        cat = o,
        group = factor(group, levels = c("nonimmigrant","immigrant"))
      ) %>%
      filter(is.finite(avg_y_cites), is.finite(avg_y_cites_pp))

    one_o <- cl_o %>%
      left_join(cluster_stats, by = "geo_level") %>%
      mutate(cat = o, immig_share_in_sub = immig_share)

    out_long[[o]] <- long_o
    out_one[[o]]  <- one_o
  }

  long <- bind_rows(out_long) %>% mutate(cat = factor(cat, levels = levs))
  one  <- bind_rows(out_one)  %>% mutate(cat = factor(cat, levels = levs))

  list(long = long, one = one, levels = levs)
}

# =========================
# (1) Open dataset + Top10 inventors by patents
# =========================
cat("\n[INFO] Opening Arrow dataset...\n")
ds0 <- open_dataset(INPUT, format = "parquet")
need_cols(ds0, c("user_id","n_patents","year","first_country","n_us_citations"))

cat("[INFO] Computing top 10% inventors by total patents...\n")
inv_tot <- ds0 %>%
  select(user_id, n_patents) %>%
  group_by(user_id) %>%
  summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop") %>%
  collect()

p90   <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)
cat("[INFO] Top10 inventors:", length(top10), "\n")

ds <- ds0 %>% filter(user_id %in% top10)
rm(ds0, inv_tot)

years_all <- ds %>% select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
if (length(years_all) == 0) stop("[ERROR] No years found in dataset.")

years_to_run <- tail(years_all, USE_LAST_N_YEARS)
years_to_run <- setdiff(years_to_run, SKIP_YEARS)
cat("[INFO] Running years:", paste(years_to_run, collapse = ", "), "\n")

# =========================
# (2) Compute first_pos_country map
# =========================
cat("\n[INFO] Computing first_pos_country mapping...\n")
need_cols(ds, c("first_country","last_country"))

pos_df <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(pos_country_y = dplyr::coalesce(last_country, first_country)) %>%
  filter(!is.na(pos_country_y), pos_country_y != "") %>%
  distinct(user_id, year, pos_country_y) %>%
  collect()

first_pos_map <- pos_df %>%
  group_by(user_id) %>%
  slice_min(order_by = year, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  transmute(user_id, first_pos_country = pos_country_y)

rm(pos_df)

# =========================
# Main loop
# =========================
for (immig_def in immig_defs) {
  for (geo_name in names(geos)) {

    geo_var <- geos[[geo_name]]

    for (yy in years_to_run) {

      cat("\n====================================================\n")
      cat("[INFO] Year:", yy, "| Geo:", geo_name, "| Immig def:", immig_def, "\n")
      cat("====================================================\n")

      base <- build_base_slice(ds, yy, geo_var, immig_def, first_pos_map)
      if (nrow(base) == 0) {
        cat("[WARN] Empty base slice. Skipping.\n")
        next
      }

      # Cluster stats based on citations productivity (mean log1p citations)
      cluster_stats <- compute_cluster_stats_cites(base, min_cluster = MIN_CLUSTER)
      if (nrow(cluster_stats) < 10) {
        cat("[WARN] Too few clusters after MIN_CLUSTER. Skipping.\n")
        next
      }

      # keep only clusters passing cutoff
      base <- base %>% semi_join(cluster_stats %>% select(geo_level), by = "geo_level")

      # Attach leave-one-out cluster productivity at inventor-year level
      base_loo <- attach_loo_prod(base, cluster_stats)

      tag <- paste0("y", yy, "__", geo_name, "__", immig_def)
      subtitle_lines <- paste0(
        "Lines: weighted OLS (weights = overall cluster_size). ",
        "Dots: size = subgroup support (n_group). ",
        "x uses leave-one-out avg_prod(-g) when available."
      )

      out_plot_root  <- file.path(PLOTS_DIR, tag)
      out_table_root <- file.path(TABLES_DIR, tag)
      dir.create(out_plot_root,  recursive = TRUE, showWarnings = FALSE)
      dir.create(out_table_root, recursive = TRUE, showWarnings = FALSE)

      # ==========================================================
      # CORE (no facets): immigrant share vs size / productivity
      # ==========================================================
      core <- cluster_stats

      pA1 <- ggplot(core, aes(x = log_cluster, y = immig_share, size = cluster_size)) +
        geom_point(alpha = 0.30) +
        geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
        scale_size_continuous(range = c(0.8, 6), guide = "none") +
        labs(
          title = paste0("Immigrant share vs log(cluster size) (", tag, ")"),
          subtitle = "Cluster productivity uses citations elsewhere; this panel is just shares.",
          x = "log(cluster size)",
          y = "Immigrant share"
        ) +
        theme_minimal()
      ggsave(file.path(out_plot_root, paste0("scatter_share_vs_logcluster__", tag, ".png")),
             pA1, width = 8.8, height = 5.6, dpi = 220)

      pA2 <- ggplot(core, aes(x = avg_prod, y = immig_share, size = cluster_size)) +
        geom_point(alpha = 0.30) +
        geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
        scale_size_continuous(range = c(0.8, 6), guide = "none") +
        labs(
          title = paste0("Immigrant share vs avg cluster productivity (citations) (", tag, ")"),
          subtitle = "avg_prod = mean(log1p(n_us_citations)) within cluster-year.",
          x = "avg_prod (mean log1p citations)",
          y = "Immigrant share"
        ) +
        theme_minimal()
      ggsave(file.path(out_plot_root, paste0("scatter_share_vs_avgprod_cites__", tag, ".png")),
             pA2, width = 8.8, height = 5.6, dpi = 220)

      # ==========================================================
      # Build “group mean outcomes” at cluster-year level
      #   y_cites: log1p(citations)
      #   y_cites_pp: log1p(citations per patent)
      #   x_loo: avg_loo_prod (mean of inventor-level loo x within group×cluster)
      # ==========================================================
      pat_long <- base_loo %>%
        mutate(group = factor(ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
                              levels = c("nonimmigrant","immigrant"))) %>%
        group_by(geo_level, group) %>%
        summarise(
          avg_y_cites    = mean(y_cites, na.rm = TRUE),
          avg_y_cites_pp = mean(y_cites_pp, na.rm = TRUE),
          avg_loo_prod   = mean(loo_avg_prod, na.rm = TRUE),
          n_group        = n_distinct(user_id),
          .groups = "drop"
        ) %>%
        filter(n_group >= MIN_GROUP_SUPPORT) %>%
        left_join(cluster_stats, by = "geo_level") %>%
        filter(is.finite(avg_y_cites), is.finite(avg_y_cites_pp), is.finite(immig_share), is.finite(log_cluster))

      # Plot bundle (overall): group means vs x’s
      if (nrow(pat_long) > 0) {

        # Use leave-one-out x for “productivity” plots
        pB1 <- ggplot(pat_long, aes(x = log_cluster, y = avg_y_cites, color = group)) +
          geom_point(aes(size = n_group), alpha = 0.30) +
          geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
          scale_size_continuous(range = c(0.8, 6), guide = "none") +
          labs(
            title = paste0("Avg log1p(citations) vs log(cluster size) (", tag, ")"),
            subtitle = "Dots sized by group support; lines weighted by overall cluster_size.",
            x = "log(cluster size)",
            y = "Avg log1p(citations)",
            color = "Group"
          ) +
          theme_minimal()
        ggsave(file.path(out_plot_root, paste0("scatter_avg_cites_vs_logcluster__", tag, ".png")),
               pB1, width = 8.8, height = 5.6, dpi = 220)

        pB2 <- ggplot(pat_long, aes(x = avg_loo_prod, y = avg_y_cites, color = group)) +
          geom_point(aes(size = n_group), alpha = 0.30) +
          geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
          scale_size_continuous(range = c(0.8, 6), guide = "none") +
          labs(
            title = paste0("Avg log1p(citations) vs leave-one-out cluster productivity avg_prod(-g) (", tag, ")"),
            subtitle = "x = mean of inventor-level LOO productivity within group×cluster.",
            x = "avg_prod(-g)  [mean log1p citations, leave-one-out]",
            y = "Avg log1p(citations)",
            color = "Group"
          ) +
          theme_minimal()
        ggsave(file.path(out_plot_root, paste0("scatter_avg_cites_vs_loo_avgprod__", tag, ".png")),
               pB2, width = 8.8, height = 5.6, dpi = 220)

        pB3 <- ggplot(pat_long, aes(x = immig_share, y = avg_y_cites, color = group)) +
          geom_point(aes(size = n_group), alpha = 0.30) +
          geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
          scale_size_continuous(range = c(0.8, 6), guide = "none") +
          labs(
            title = paste0("Avg log1p(citations) vs immigrant share (", tag, ")"),
            subtitle = "Lines weighted by overall cluster_size.",
            x = "Immigrant share",
            y = "Avg log1p(citations)",
            color = "Group"
          ) +
          theme_minimal()
        ggsave(file.path(out_plot_root, paste0("scatter_avg_cites_vs_immigshare__", tag, ".png")),
               pB3, width = 8.8, height = 5.6, dpi = 220)

        # Per-patent quality analogue
        pQ2 <- ggplot(pat_long, aes(x = avg_loo_prod, y = avg_y_cites_pp, color = group)) +
          geom_point(aes(size = n_group), alpha = 0.30) +
          geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
          scale_size_continuous(range = c(0.8, 6), guide = "none") +
          labs(
            title = paste0("Avg log1p(citations per patent) vs avg_prod(-g) (", tag, ")"),
            subtitle = "y = log1p(citations/patents). x uses leave-one-out productivity.",
            x = "avg_prod(-g)  [mean log1p citations, leave-one-out]",
            y = "Avg log1p(citations per patent)",
            color = "Group"
          ) +
          theme_minimal()
        ggsave(file.path(out_plot_root, paste0("scatter_avg_cites_perpat_vs_loo_avgprod__", tag, ".png")),
               pQ2, width = 8.8, height = 5.6, dpi = 220)
      }

      # ==========================================================
      # FACETS: Sections, Tenure bins, Career class, G/H classes, Origins
      #   (Each facet: same 3 x-axes; plus share-in-subgroup plots)
      # ==========================================================

      run_facets <- function(obj, name_prefix) {
        if (nrow(obj$long) == 0) return(invisible(NULL))

        # 1) y = avg_y_cites (log1p citations)
        plot_facet_group_lines(
          obj$long, "cat", "avg_loo_prod", "avg_y_cites",
          title = paste0("Returns vs avg_prod(-g) — ", name_prefix, " (", tag, ")"),
          subtitle = subtitle_lines,
          outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__avgcites_vs_loo_avgprod__", tag, ".png"))
        )
        plot_facet_group_lines(
          obj$long, "cat", "log_cluster", "avg_y_cites",
          title = paste0("Returns vs log(cluster size) — ", name_prefix, " (", tag, ")"),
          subtitle = subtitle_lines,
          outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__avgcites_vs_logcluster__", tag, ".png"))
        )
        plot_facet_group_lines(
          obj$long, "cat", "immig_share", "avg_y_cites",
          title = paste0("Returns vs immigrant share — ", name_prefix, " (", tag, ")"),
          subtitle = subtitle_lines,
          outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__avgcites_vs_immigshare__", tag, ".png"))
        )

        # 2) y = avg_y_cites_pp (log1p citations per patent)
        plot_facet_group_lines(
          obj$long, "cat", "avg_loo_prod", "avg_y_cites_pp",
          title = paste0("Quality (cites/pat) vs avg_prod(-g) — ", name_prefix, " (", tag, ")"),
          subtitle = subtitle_lines,
          outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__avgcitespp_vs_loo_avgprod__", tag, ".png"))
        )

        # 3) Subgroup immigrant share
        if (nrow(obj$one) > 0) {
          plot_facet_share(
            obj$one, "cat", "avg_prod", "immig_share_in_sub",
            title = paste0("Immigrant share within subgroup vs avg_prod (citations) — ", name_prefix, " (", tag, ")"),
            subtitle = "y = (# immigrant inventors in subgroup)/(# inventors in subgroup) within cluster-year.",
            outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__shareinsub_vs_avgprod__", tag, ".png"))
          )
          plot_facet_share(
            obj$one, "cat", "log_cluster", "immig_share_in_sub",
            title = paste0("Immigrant share within subgroup vs log(cluster size) — ", name_prefix, " (", tag, ")"),
            subtitle = "Lines weighted by overall cluster_size.",
            outpath = file.path(out_plot_root, paste0("facet_", name_prefix, "__shareinsub_vs_logcluster__", tag, ".png"))
          )
        }

        # 4) Slope tables (use x = avg_loo_prod, y = avg_y_cites and avg_y_cites_pp)
	safe_slopes <- function(d, yvar) {
	  res_list <- d %>%
	    group_by(cat) %>%
    	    group_split()

  	out <- bind_rows(lapply(res_list, function(dd) {
    	cc <- unique(as.character(dd$cat))[1]
    	st <- slope_table_by_facet(dd, xvar = "avg_loo_prod", yvar = yvar, wvar = "cluster_size")
    	if (is.null(st) || nrow(st) == 0) return(NULL)
    	st %>% mutate(cat = cc)
  	}))

  	if (nrow(out) == 0) return(tibble())
  	out %>% arrange(p_diff, desc(abs(slope_diff)))
	}

	slopes1 <- safe_slopes(obj$long, "avg_y_cites")
	slopes2 <- safe_slopes(obj$long, "avg_y_cites_pp")

	if (nrow(slopes1) > 0) write_csv(slopes1,
  	file.path(out_table_root, paste0("slopes_", name_prefix, "__y_avgcites__x_loo_avgprod__", tag, ".csv"))
	)
	if (nrow(slopes2) > 0) write_csv(slopes2,
  	file.path(out_table_root, paste0("slopes_", name_prefix, "__y_avgcitespp__x_loo_avgprod__", tag, ".csv"))
	)
	}
      # 1) SECTIONS
      sec <- build_subcat_long(
        base_with_loo = base_loo,
        cluster_stats = cluster_stats,
        cat_var = "career_modal_section",
        top_n = TOP_N_FACETS,
        keep_all_levels = FALSE
      )
      run_facets(sec, "section")

      # 2) TENURE BINS
      base_t <- base_loo %>%
        mutate(
          tenure = compute_tenure(.)$tenure,
          tenure_bin = make_tenure_bins(tenure)
        )

      ten <- build_subcat_long(
        base_with_loo = base_t,
        cluster_stats = cluster_stats,
        cat_var = "tenure_bin",
        top_n = TOP_N_FACETS,
        keep_all_levels = TRUE
      )
      run_facets(ten, "tenure")

      # 3) CAREER MODAL CLASS (top 9 among immigrants)
      cls <- build_subcat_long(
        base_with_loo = base_loo,
        cluster_stats = cluster_stats,
        cat_var = "career_modal_class",
        top_n = TOP_N_FACETS,
        keep_all_levels = FALSE
      )
      run_facets(cls, "class")

      # 4) G classes
      gcls <- build_subcat_long(
        base_with_loo = base_loo,
        cluster_stats = cluster_stats,
        cat_var = "career_modal_class",
        top_n = TOP_N_FACETS,
        keep_all_levels = FALSE,
        cat_filter = function(x) substr(x, 1, 1) == "G"
      )
      run_facets(gcls, "Gclasses")

      # 5) H classes
      hcls <- build_subcat_long(
        base_with_loo = base_loo,
        cluster_stats = cluster_stats,
        cat_var = "career_modal_class",
        top_n = TOP_N_FACETS,
        keep_all_levels = FALSE,
        cat_filter = function(x) substr(x, 1, 1) == "H"
      )
      run_facets(hcls, "Hclasses")

      # 6) ORIGIN facets
      ori <- build_origin_long(base_with_loo = base_loo, cluster_stats = cluster_stats, top_n = TOP_N_ORIGINS)
      run_facets(ori, "origin")

      cat("[INFO] Saved citations plots + tables under:\n  ", out_plot_root, "\n  ", out_table_root, "\n")
    }
  }
}

cat("\n[DONE] All CITATIONS descriptives written to:\n  ", PLOTS_DIR, "\n  ", TABLES_DIR, "\n\n")
