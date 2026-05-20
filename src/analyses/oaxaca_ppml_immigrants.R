#!/usr/bin/env Rscript
###############################################################################
# Oaxaca Decomposition — Immigrants vs Non-Immigrants (PPML with FEs)
#
# Uses saved PPML models from:
#   /home/epiga/revelio_labs/output/regressions/ppml_immigrants
#
# Computes Oaxaca-style decomposition on the MEAN LINEAR PREDICTOR (log mean):
#   gap = mean(eta_I) - mean(eta_N)
# where eta = x'beta + FE_user + FE_geo + FE_year (+ FE_firm)
#
# Decomposes into:
#   Explained (exposure/sorting): differences in shares across clusters/years/firms
#   Unexplained (returns): differences in FE values / coefficients
#
# Also reports:
#   Baseline inventor heterogeneity = difference in mean user FE
#
# Output:
#   /home/epiga/revelio_labs/output/regressions/oaxaca_immigrants
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","alpaca","tibble","stringr")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
if (!requireNamespace("remotes", quietly = TRUE))
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
if (!requireNamespace("alpaca", quietly = TRUE)) {
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib)
}

library(arrow)
library(dplyr)
library(readr)
library(alpaca)
library(tibble)
library(stringr)

# ============================
# Paths
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"

PPML_DIR <- "/home/epiga/revelio_labs/output/regressions/ppml_immigrants"
OUT_DIR  <- "/home/epiga/revelio_labs/output/regressions/oaxaca_immigrants"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

# ============================
# Config (same as PPML runner)
# ============================
USE_TOP10 <- TRUE

geos <- list(
  state = "first_state",
  city  = "first_city",
  metro = "first_metro_area"
)

firm_fes <- list(
  rcid   = "first_rcid",
  parent = "first_parent_rcid",
  nofirm = NULL
)

immig_defs <- c("immig_job_first_nonUS", "immig_deg_first_nonUS", "immig_first_deg_or_job_nonUS")

# ============================
# Tenure construction (same)
# ============================
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = as.numeric(substr(first_startdate_edu, 1, 4)),
      pos_year = as.numeric(substr(first_startdate_pos, 1, 4)),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > 50, year - pos_year, tenure),
      tenure   = ifelse(tenure > 50 | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    ) %>%
    filter(!is.na(tenure))
}

# ============================
# Helper: safe binary filter
# ============================
filter_group <- function(df, immig_var, group_value) {
  df %>%
    mutate(immig_flag = as.integer(.data[[immig_var]])) %>%
    filter(!is.na(immig_flag), immig_flag %in% c(0L, 1L)) %>%
    filter(immig_flag == as.integer(group_value))
}

# ============================
# Helpers for Oaxaca math
# ============================
mean_xbeta <- function(df, beta) {
  xbar <- c(
    tenure    = mean(df$tenure, na.rm = TRUE),
    tenure_sq = mean(df$tenure_sq, na.rm = TRUE)
  )
  # Ensure same ordering
  xbar <- xbar[names(beta)]
  sum(xbar * beta)
}

share_vec <- function(x) {
  x <- as.character(x)
  tab <- table(x)
  s <- as.numeric(tab) / sum(tab)
  names(s) <- names(tab)
  s
}

mean_fe_contrib <- function(df, fe_vec, varname) {
  # df[[varname]] -> level shares; fe_vec is named numeric with same level labels (as characters)
  s <- share_vec(df[[varname]])
  fe <- fe_vec[names(s)]
  if (any(is.na(fe))) {
    # shouldn’t happen if common support enforced, but just in case
    ok <- !is.na(fe)
    s <- s[ok]; fe <- fe[ok]
  }
  sum(s * as.numeric(fe))
}

oaxaca_cat <- function(dfI, dfN, feI, feN, varname) {
  # shares on common support assumed
  sI <- share_vec(dfI[[varname]])
  sN <- share_vec(dfN[[varname]])

  L <- intersect(names(sI), names(sN))
  sI <- sI[L]; sN <- sN[L]
  feI <- feI[L]; feN <- feN[L]

  # Explained (exposure) using nonimmigrant FE as reference
  E <- sum((sI - sN) * feN)

  # Unexplained (returns) evaluated at immigrant exposure
  U <- sum(sI * (feI - feN))

  list(E = E, U = U)
}

load_model <- function(immig_def, group_label, geo_name, firm_name) {
  tag <- paste(immig_def, group_label, geo_name, firm_name, sep = "__")
  path <- file.path(PPML_DIR, paste0("ppml_top10__", tag, ".rds"))
  if (!file.exists(path)) return(NULL)
  readRDS(path)
}

# ============================
# Load data once (same cols)
# ============================
cat("\n=======================================\n")
cat("[INFO] Loading data once from parquet\n")
cat("=======================================\n\n")

df0 <- open_dataset(INPUT, format = "parquet") %>%
  select(
    user_id, n_patents, year,
    first_country,
    first_startdate_edu, first_startdate_pos,
    first_state, first_city, first_metro_area,
    first_rcid, first_parent_rcid,
    immig_job_first_nonUS,
    immig_deg_first_nonUS,
    immig_first_deg_or_job_nonUS
  ) %>%
  collect() %>%
  filter(first_country == "United States")

cat("[INFO] Data loaded:", nrow(df0), "rows\n")

# Top 10% filter (same)
if (USE_TOP10) {
  cat("[INFO] Computing top 10% inventors...\n")
  inv_tot <- df0 %>%
    group_by(user_id) %>%
    summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop")

  p90  <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
  top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)

  df0 <- df0 %>% filter(user_id %in% top10)
  cat("[INFO] Remaining after top 10% filter:", nrow(df0), "\n")
}

# Tenure (same)
df0 <- compute_tenure(df0)

# ============================
# Main: Oaxaca per combo
# ============================
run_oaxaca_one <- function(df0, immig_def, geo_name, geo_var, firm_name, firm_var) {

  # Load models
  mI <- load_model(immig_def, "immigrant",    geo_name, firm_name)
  mN <- load_model(immig_def, "nonimmigrant", geo_name, firm_name)

  if (is.null(mI) || is.null(mN)) {
    cat("[WARN] Missing model(s) for:", immig_def, geo_name, firm_name, "\n")
    return(NULL)
  }

  # Build samples (match PPML data requirements)
  dI <- filter_group(df0, immig_def, 1)
  dN <- filter_group(df0, immig_def, 0)

  # Drop NAs in FE IDs used
  dI <- dI %>% filter(!is.na(.data[[geo_var]]), !is.na(year), !is.na(user_id))
  dN <- dN %>% filter(!is.na(.data[[geo_var]]), !is.na(year), !is.na(user_id))

  if (!is.null(firm_var)) {
    dI <- dI %>% filter(!is.na(.data[[firm_var]]))
    dN <- dN %>% filter(!is.na(.data[[firm_var]]))
  }

  nI_full <- nrow(dI); nN_full <- nrow(dN)
  if (nI_full < 1000 || nN_full < 1000) {
    cat("[WARN] Too few obs for stable Oaxaca:", immig_def, geo_name, firm_name, "\n")
    return(NULL)
  }

  # Common support on categorical FE levels (geo/year/firm)
  common_geo  <- intersect(unique(as.character(dI[[geo_var]])),  unique(as.character(dN[[geo_var]])))
  common_year <- intersect(unique(as.character(dI$year)),        unique(as.character(dN$year)))

  dI_cs <- dI %>% filter(as.character(.data[[geo_var]]) %in% common_geo,
                         as.character(year) %in% common_year)
  dN_cs <- dN %>% filter(as.character(.data[[geo_var]]) %in% common_geo,
                         as.character(year) %in% common_year)

  if (!is.null(firm_var)) {
    common_firm <- intersect(unique(as.character(dI[[firm_var]])), unique(as.character(dN[[firm_var]])))
    dI_cs <- dI_cs %>% filter(as.character(.data[[firm_var]]) %in% common_firm)
    dN_cs <- dN_cs %>% filter(as.character(.data[[firm_var]]) %in% common_firm)
  }

  keep_I <- nrow(dI_cs) / nI_full
  keep_N <- nrow(dN_cs) / nN_full

  if (nrow(dI_cs) < 1000 || nrow(dN_cs) < 1000) {
    cat("[WARN] After common-support restriction too small:", immig_def, geo_name, firm_name, "\n")
    return(NULL)
  }

  # Extract betas
  betaI <- coef(mI)[c("tenure","tenure_sq")]
  betaN <- coef(mN)[c("tenure","tenure_sq")]

  # Extract fixed effects
  feI <- alpaca::getFEs(mI)
  feN <- alpaca::getFEs(mN)

  # Pull FE vectors (as named numeric)
  fe_user_I <- feI[["user_id"]]
  fe_user_N <- feN[["user_id"]]

  fe_geo_I  <- feI[[geo_var]]
  fe_geo_N  <- feN[[geo_var]]

  fe_year_I <- feI[["year"]]
  fe_year_N <- feN[["year"]]

  fe_firm_I <- NULL
  fe_firm_N <- NULL
  if (!is.null(firm_var)) {
    fe_firm_I <- feI[[firm_var]]
    fe_firm_N <- feN[[firm_var]]
  }

  # Mean eta by components (log-mean)
  xbI <- mean_xbeta(dI_cs, betaI)
  xbN <- mean_xbeta(dN_cs, betaN)

  # FE mean contributions (exact on linear predictor scale)
  userI <- mean_fe_contrib(dI_cs, fe_user_I, "user_id")
  userN <- mean_fe_contrib(dN_cs, fe_user_N, "user_id")

  geoI  <- mean_fe_contrib(dI_cs, fe_geo_I,  geo_var)
  geoN  <- mean_fe_contrib(dN_cs, fe_geo_N,  geo_var)

  yearI <- mean_fe_contrib(dI_cs, fe_year_I, "year")
  yearN <- mean_fe_contrib(dN_cs, fe_year_N, "year")

  firmI <- 0
  firmN <- 0
  if (!is.null(firm_var)) {
    firmI <- mean_fe_contrib(dI_cs, fe_firm_I, firm_var)
    firmN <- mean_fe_contrib(dN_cs, fe_firm_N, firm_var)
  }

  mean_eta_I <- xbI + userI + geoI + yearI + firmI
  mean_eta_N <- xbN + userN + geoN + yearN + firmN
  gap_eta    <- mean_eta_I - mean_eta_N

  # Oaxaca: covariates
  xbarI <- c(tenure = mean(dI_cs$tenure), tenure_sq = mean(dI_cs$tenure_sq))
  xbarN <- c(tenure = mean(dN_cs$tenure), tenure_sq = mean(dN_cs$tenure_sq))
  xbarI <- xbarI[names(betaN)]
  xbarN <- xbarN[names(betaN)]

  E_x <- sum((xbarI - xbarN) * betaN)
  U_x <- sum(xbarI * (betaI - betaN))

  # Oaxaca: FE blocks (geo/year/firm)
  ox_geo  <- oaxaca_cat(dI_cs, dN_cs, fe_geo_I,  fe_geo_N,  geo_var)
  ox_year <- oaxaca_cat(dI_cs, dN_cs, fe_year_I, fe_year_N, "year")

  E_geo  <- ox_geo$E
  U_geo  <- ox_geo$U
  E_year <- ox_year$E
  U_year <- ox_year$U

  E_firm <- 0
  U_firm <- 0
  if (!is.null(firm_var)) {
    ox_firm <- oaxaca_cat(dI_cs, dN_cs, fe_firm_I, fe_firm_N, firm_var)
    E_firm <- ox_firm$E
    U_firm <- ox_firm$U
  }

  # Baseline (inventor heterogeneity): mean user FE difference
  baseline_user <- userI - userN

  # Residual (should be ~0; captures normalization interactions if any)
  explained_total   <- E_x + E_geo + E_year + E_firm
  unexplained_total <- U_x + U_geo + U_year + U_firm

  residual <- gap_eta - (baseline_user + explained_total + unexplained_total)

  # Assemble results
  tag <- paste(immig_def, geo_name, firm_name, sep="__")
  out <- tibble(
    immig_def = immig_def,
    geo_name  = geo_name,
    geo_var   = geo_var,
    firm_name = firm_name,
    firm_var  = ifelse(is.null(firm_var), NA_character_, firm_var),

    nI_full = nI_full, nN_full = nN_full,
    nI_cs   = nrow(dI_cs), nN_cs = nrow(dN_cs),
    keep_I  = keep_I, keep_N = keep_N,

    mean_eta_I = mean_eta_I,
    mean_eta_N = mean_eta_N,
    gap_eta    = gap_eta,

    baseline_user = baseline_user,

    E_x    = E_x,    U_x    = U_x,
    E_geo  = E_geo,  U_geo  = U_geo,
    E_year = E_year, U_year = U_year,
    E_firm = E_firm, U_firm = U_firm,

    explained_total   = explained_total,
    unexplained_total = unexplained_total,
    residual = residual
  )

  # Save per-combo
  write_csv(out, file.path(OUT_DIR, paste0("oaxaca__", tag, ".csv")))
  cat("[OK] Oaxaca saved:", tag,
      " | gap_eta=", round(gap_eta,4),
      " | keep_I=", round(keep_I,3),
      " | keep_N=", round(keep_N,3), "\n")

  out
}

# ============================
# Run all combos
# ============================
results <- list()

for (immig_def in immig_defs) {
  cat("\n=====================================================\n")
  cat("[INFO] Oaxaca for immig_def:", immig_def, "\n")
  cat("=====================================================\n")

  for (geo_name in names(geos)) {
    for (firm_name in names(firm_fes)) {

      geo_var  <- geos[[geo_name]]
      firm_var <- firm_fes[[firm_name]]

      res <- run_oaxaca_one(df0, immig_def, geo_name, geo_var, firm_name, firm_var)
      if (!is.null(res)) {
        key <- paste(immig_def, geo_name, firm_name, sep="__")
        results[[key]] <- res
      }
    }
  }
}

if (length(results) > 0) {
  all_res <- bind_rows(results)
  write_csv(all_res, file.path(OUT_DIR, "oaxaca_all_results.csv"))
  cat("\n[INFO] Master Oaxaca table written to:", file.path(OUT_DIR, "oaxaca_all_results.csv"), "\n")
} else {
  cat("\n[WARN] No Oaxaca results produced.\n")
}

cat("\n==============================\n")
cat("[INFO] OAXACA COMPLETED\n")
cat("==============================\n\n")
