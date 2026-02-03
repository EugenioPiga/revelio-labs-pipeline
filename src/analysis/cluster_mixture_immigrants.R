#!/usr/bin/env Rscript
###############################################################################
# Cluster Mixture Decomposition (NOT Oaxaca)
# Immigrants vs Nonimmigrants:
#   ybar_ct = yN_ct + (yI_ct - yN_ct) * Lambda_ct
#           = eps_ct + beta_ct * Lambda_ct
#
# Midpoint change decomposition (year-to-year):
#   d_ybar  = d_eps + beta_bar * d_Lambda + Lambda_bar * d_beta
#
# alpha1/2/3 shares per cluster-change:
#   alpha1 = d_eps / d_ybar
#   alpha2 = (beta_bar * d_Lambda) / d_ybar
#   alpha3 = (Lambda_bar * d_beta) / d_ybar
#
# Also computes variance-share alphas:
#   A_k = Cov(d_ybar, component_k) / Var(d_ybar)
# which sum to ~1 by construction.
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","tidyr","readr","stringr","tibble")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(stringr)
library(tibble)

# ============================
# Paths (match your project)
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants/alpha_decomposition"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ============================
# Knobs
# ============================
MIN_CLUSTER <- 50
USE_TOP10   <- TRUE

# if you want changes only between two years, set MODE="pair"
MODE  <- "adjacent"   # "adjacent" or "pair"
YEAR0 <- 2018
YEAR1 <- 2024

geos <- list(
  state = "first_state",
  city  = "first_city",
  metro = "first_metro_area"
)

immig_defs <- c(
  "immig_job_first_nonUS",
  "immig_deg_first_nonUS",
  "immig_first_deg_or_job_nonUS"
)

# ============================
# Weighted moments (variance-share alphas)
# ============================
w_mean <- function(x, w) {
  ok <- is.finite(x) & is.finite(w) & (w > 0)
  x <- x[ok]; w <- w[ok]
  sum(w * x) / sum(w)
}

w_cov <- function(x, y, w) {
  ok <- is.finite(x) & is.finite(y) & is.finite(w) & (w > 0)
  x <- x[ok]; y <- y[ok]; w <- w[ok]
  mx <- sum(w * x) / sum(w)
  my <- sum(w * y) / sum(w)
  sum(w * (x - mx) * (y - my)) / sum(w)
}

w_var <- function(x, w) w_cov(x, x, w)

# ============================
# Load minimal data once
# ============================
cat("\n[INFO] Loading data...\n")

df0 <- open_dataset(INPUT, format = "parquet") %>%
  select(
    user_id, year, n_patents, first_country,
    first_state, first_city, first_metro_area,
    immig_job_first_nonUS,
    immig_deg_first_nonUS,
    immig_first_deg_or_job_nonUS
  ) %>%
  collect() %>%
  filter(first_country == "United States")

cat("[INFO] Rows (US):", nrow(df0), "\n")

# ============================
# Top 10% inventors (same logic as your PPML scripts)
# ============================
if (USE_TOP10) {
  cat("[INFO] Computing top 10% inventors by total patents...\n")
  inv_tot <- df0 %>%
    group_by(user_id) %>%
    summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop")

  p90   <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
  top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)

  df0 <- df0 %>% filter(user_id %in% top10)
  cat("[INFO] Rows after top10:", nrow(df0), " | Users:", n_distinct(df0$user_id), "\n")
}

# ============================
# Build cluster-year stats
# ============================
build_cluster_year <- function(df, immig_def, geo_var) {

  df %>%
    mutate(
      immig_flag = as.integer(.data[[immig_def]]),
      cluster    = as.character(.data[[geo_var]])
    ) %>%
    filter(!is.na(cluster), cluster != "") %>%
    filter(!is.na(immig_flag), immig_flag %in% c(0L, 1L)) %>%
    group_by(cluster, year) %>%
    summarise(
      cluster_size = n_distinct(user_id),
      n_immig      = n_distinct(user_id[immig_flag == 1L]),
      n_nonimm     = n_distinct(user_id[immig_flag == 0L]),
      Lambda       = ifelse(cluster_size > 0, n_immig / cluster_size, NA_real_),

      ybar = mean(n_patents, na.rm = TRUE),
      yI   = ifelse(n_immig  > 0, mean(n_patents[immig_flag == 1L], na.rm = TRUE), NA_real_),
      yN   = ifelse(n_nonimm > 0, mean(n_patents[immig_flag == 0L], na.rm = TRUE), NA_real_),

      .groups = "drop"
    ) %>%
    mutate(
      beta = yI - yN,
      eps  = yN
    ) %>%
    # to make beta defined (needed when Lambda changes), require both groups present
    filter(cluster_size >= MIN_CLUSTER, n_immig > 0, n_nonimm > 0) %>%
    filter(is.finite(ybar), is.finite(beta), is.finite(eps), is.finite(Lambda))
}

# ============================
# Compute change decomposition
# ============================
compute_changes <- function(cy, mode = "adjacent", year0 = 2018, year1 = 2024) {

  cy <- cy %>% arrange(cluster, year)

  if (mode == "pair") {
    # keep only year0 and year1, then compute differences within cluster
    cy2 <- cy %>% filter(year %in% c(year0, year1)) %>%
      group_by(cluster) %>%
      filter(n() == 2) %>%
      arrange(year) %>%
      summarise(
        year0 = first(year),
        year1 = last(year),

        ybar0 = first(ybar), ybar1 = last(ybar),
        eps0  = first(eps),  eps1  = last(eps),
        beta0 = first(beta), beta1 = last(beta),
        L0    = first(Lambda), L1 = last(Lambda),

        size0 = first(cluster_size), size1 = last(cluster_size),
        .groups = "drop"
      ) %>%
      mutate(
        d_ybar  = ybar1 - ybar0,
        d_eps   = eps1  - eps0,
        d_beta  = beta1 - beta0,
        d_L     = L1    - L0,

        beta_bar = 0.5 * (beta1 + beta0),
        L_bar    = 0.5 * (L1 + L0),

        comp1 = d_eps,
        comp2 = beta_bar * d_L,
        comp3 = L_bar * d_beta,

        residual = d_ybar - (comp1 + comp2 + comp3),
        w = 0.5 * (size0 + size1),

        # per-cluster shares (undefined if d_ybar==0)
        alpha1 = ifelse(abs(d_ybar) > 1e-12, comp1 / d_ybar, NA_real_),
        alpha2 = ifelse(abs(d_ybar) > 1e-12, comp2 / d_ybar, NA_real_),
        alpha3 = ifelse(abs(d_ybar) > 1e-12, comp3 / d_ybar, NA_real_)
      )

    return(cy2)
  }

  # adjacent year-to-year changes
  ch <- cy %>%
    group_by(cluster) %>%
    mutate(
      ybar_l = dplyr::lag(ybar),
      eps_l  = dplyr::lag(eps),
      beta_l = dplyr::lag(beta),
      L_l    = dplyr::lag(Lambda),
      size_l = dplyr::lag(cluster_size),
      year_l = dplyr::lag(year)
    ) %>%
    ungroup() %>%
    filter(!is.na(year_l)) %>%
    mutate(
      d_ybar = ybar - ybar_l,
      d_eps  = eps  - eps_l,
      d_beta = beta - beta_l,
      d_L    = Lambda - L_l,

      beta_bar = 0.5 * (beta + beta_l),
      L_bar    = 0.5 * (Lambda + L_l),

      comp1 = d_eps,
      comp2 = beta_bar * d_L,
      comp3 = L_bar * d_beta,

      residual = d_ybar - (comp1 + comp2 + comp3),
      w = 0.5 * (cluster_size + size_l),

      alpha1 = ifelse(abs(d_ybar) > 1e-12, comp1 / d_ybar, NA_real_),
      alpha2 = ifelse(abs(d_ybar) > 1e-12, comp2 / d_ybar, NA_real_),
      alpha3 = ifelse(abs(d_ybar) > 1e-12, comp3 / d_ybar, NA_real_)
    ) %>%
    select(cluster, year_l, year, w,
           d_ybar, comp1, comp2, comp3, residual,
           alpha1, alpha2, alpha3,
           beta_l, beta, L_l, Lambda)

  ch
}

# ============================
# Summaries: variance-share and slope-share
# ============================
summarise_alphas <- function(ch) {

  # drop degenerate cases
  d <- ch %>% filter(is.finite(d_ybar), is.finite(comp1), is.finite(comp2), is.finite(comp3), is.finite(w), w > 0)

  if (nrow(d) < 50) return(NULL)

  # variance-share alphas (weighted)
  vYw <- w_var(d$d_ybar, d$w)
  a1w <- w_cov(d$d_ybar, d$comp1, d$w) / vYw
  a2w <- w_cov(d$d_ybar, d$comp2, d$w) / vYw
  a3w <- w_cov(d$d_ybar, d$comp3, d$w) / vYw

  # variance-share alphas (unweighted)
  vYu <- var(d$d_ybar)
  a1u <- cov(d$d_ybar, d$comp1) / vYu
  a2u <- cov(d$d_ybar, d$comp2) / vYu
  a3u <- cov(d$d_ybar, d$comp3) / vYu

  # slope-share (no intercept) as a robustness check
  # compk ~ 0 + d_ybar
  s1w <- coef(lm(comp1 ~ 0 + d_ybar, data = d, weights = w))[[1]]
  s2w <- coef(lm(comp2 ~ 0 + d_ybar, data = d, weights = w))[[1]]
  s3w <- coef(lm(comp3 ~ 0 + d_ybar, data = d, weights = w))[[1]]

  tibble(
    n_changes = nrow(d),
    mean_abs_residual = mean(abs(d$residual), na.rm = TRUE),

    alpha1_varshare_w = a1w,
    alpha2_varshare_w = a2w,
    alpha3_varshare_w = a3w,
    sum_varshare_w    = a1w + a2w + a3w,

    alpha1_varshare_u = a1u,
    alpha2_varshare_u = a2u,
    alpha3_varshare_u = a3u,
    sum_varshare_u    = a1u + a2u + a3u,

    alpha1_slope0_w = s1w,
    alpha2_slope0_w = s2w,
    alpha3_slope0_w = s3w,
    sum_slope0_w    = s1w + s2w + s3w
  )
}

# ============================
# Run everything
# ============================
all_summ <- list()

for (immig_def in immig_defs) {
  for (geo_name in names(geos)) {

    geo_var <- geos[[geo_name]]
    tag <- paste(immig_def, geo_name, sep="__")

    cat("\n[INFO] Building cluster-year stats:", tag, "\n")
    cy <- build_cluster_year(df0, immig_def, geo_var)

    if (nrow(cy) < 200) {
      cat("[WARN] Too few cluster-years for:", tag, "\n")
      next
    }

    # if MODE="pair", ensure years exist
    if (MODE == "pair") {
      yrs <- sort(unique(cy$year))
      if (!(YEAR0 %in% yrs && YEAR1 %in% yrs)) {
        cat("[WARN] Missing YEAR0/YEAR1 in data for:", tag, " | available years:", paste(head(yrs,3), collapse=","), "...", tail(yrs,1), "\n")
        next
      }
    }

    ch <- compute_changes(cy, mode = MODE, year0 = YEAR0, year1 = YEAR1)

    # Save panel
    out_panel <- file.path(OUT_DIR, paste0("alpha_panel__", tag, "__", MODE, ".csv"))
    write_csv(ch, out_panel)
    cat("[OK] Wrote panel:", out_panel, " | rows:", nrow(ch), "\n")

    # Summary
    summ <- summarise_alphas(ch)
    if (!is.null(summ)) {
      summ <- summ %>% mutate(
        immig_def = immig_def,
        geo_name  = geo_name,
        geo_var   = geo_var,
        mode      = MODE,
        year0     = ifelse(MODE=="pair", YEAR0, NA_integer_),
        year1     = ifelse(MODE=="pair", YEAR1, NA_integer_)
      )
      all_summ[[tag]] <- summ
      cat("[OK] Summary alphas (weighted var-share):",
          " a1=", round(summ$alpha1_varshare_w,3),
          " a2=", round(summ$alpha2_varshare_w,3),
          " a3=", round(summ$alpha3_varshare_w,3),
          " sum=", round(summ$sum_varshare_w,3), "\n")
    }
  }
}

if (length(all_summ) > 0) {
  summ_df <- bind_rows(all_summ)
  out_sum <- file.path(OUT_DIR, paste0("alpha_summary__", MODE, ".csv"))
  write_csv(summ_df, out_sum)
  cat("\n[INFO] Wrote master summary:", out_sum, "\n")
} else {
  cat("\n[WARN] No summaries produced.\n")
}

cat("\n[DONE] Alpha decomposition completed.\n")
