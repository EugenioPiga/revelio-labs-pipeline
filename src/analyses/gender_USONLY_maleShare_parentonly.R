#!/usr/bin/env Rscript
##############################################################################
# US-ONLY inventor-year sample (first_country == "United States") — Parent only
# ONLY gender spec, but using SHARE OF MALE (not HHI / not 1-HHI).
#
# Cluster measure (time-invariant, last-10y):
#   male_share_10y = avg over years of ( #Male / (#Male + #Female) ) within cluster-year
#   cluster_size_10y = avg cluster size over last-10y (unique inventors per cluster-year)
#
# Regressions (PPML + OLS), both:
#   - With inventor FE: user_id + year FE
#   - No inventor FE: year FE only
# And both:
#   - UW (unweighted)
#   - W  (weights = cluster_size_10y)
#
# Controls: cluster_size_10y + tenure + tenure_sq (NO immigrant share controls)
#
# Sample splits (by inventor gender):
#   - MALE only
#   - FEMALE only
#   - ALL
#
# No arrow: uses duckdb to read parquet.
##############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("duckdb","DBI","dplyr","readr","stringr","data.table","fixest","tibble","parallel")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(duckdb); library(DBI)
  library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(tibble)
})

set.seed(123)
data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))

# =========================
# 1) Paths + knobs
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_BASE  <- "/home/epiga/revelio_labs/output/gender_USONLY_maleShare_parentonly"

PARENT_VAR <- "first_parent_rcid"
US_COUNTRY <- "United States"

YEAR_START <- 2010
YEAR_END   <- 2024

LAST10_N <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END
YEARS_LAST10 <- LAST10_START:LAST10_END

MIN_CLUSTER <- 10
TENURE_MAX  <- 50

GENDER_VAR <- "au_sex_predicted"  # values should be "Male" or "Female" (others treated as missing)

dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)
DIRS <- list(
  tables       = file.path(OUT_BASE, "tables"),
  cluster_meas = file.path(OUT_BASE, "cluster_measures_last10y")
)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

# =========================
# 2) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}

extract_fixest_terms <- function(m, model_tag) {
  ct <- fixest::coeftable(m)
  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(model = model_tag, term = character(0), estimate = numeric(0),
                  se = numeric(0), stat = numeric(0), p = numeric(0), stars = character(0)))
  }
  tibble(
    model    = model_tag,
    term     = rownames(ct),
    estimate = as.numeric(ct[,1]),
    se       = as.numeric(ct[,2]),
    stat     = as.numeric(ct[,3]),
    p        = as.numeric(ct[,4]),
    stars    = sig_stars(as.numeric(ct[,4]))
  )
}

compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    )
}

# =========================
# 3) Cluster-level male share (last-10y)
# =========================
cluster_male_share_last10y <- function(df_us, min_cluster = MIN_CLUSTER) {
  ts_msg("Building cluster male-share (last-10y):", LAST10_START, "-", LAST10_END)

  keep <- c("user_id","year","cluster_id", GENDER_VAR)
  dt <- as.data.table(df_us[, keep])

  dt[, user_id := as.character(user_id)]
  dt[, year := as.integer(year)]
  dt[, cluster_id := as.character(cluster_id)]
  dt[, g := trimws(as.character(get(GENDER_VAR)))]

  dt <- dt[year %in% YEARS_LAST10]
  dt <- dt[!is.na(user_id) & !is.na(year) & !is.na(cluster_id) & cluster_id != ""]

  # cluster size: all inventors (regardless of gender missing)
  comp_all <- unique(dt[, .(user_id, year, cluster_id)])
  size_y <- comp_all[, .(cluster_size = uniqueN(user_id)), by = .(cluster_id, year)]
  size_y <- size_y[cluster_size >= min_cluster]

  # male share: only among valid genders {Male,Female}
  dt_valid <- unique(dt[g %in% c("Male","Female"), .(user_id, year, cluster_id, g)])
  if (nrow(dt_valid) == 0) {
    out_y <- merge(size_y, data.table(cluster_id=size_y$cluster_id, year=size_y$year, male_share=NA_real_),
                   by=c("cluster_id","year"), all.x=TRUE)
  } else {
    # counts among valid genders
    g_y <- dt_valid[, .(n = uniqueN(user_id)), by = .(cluster_id, year, g)]
    g_y[, tot := sum(n), by = .(cluster_id, year)]
    g_y[, male_n := sum(n[g == "Male"]), by = .(cluster_id, year)]
    g_y <- unique(g_y[, .(cluster_id, year, tot, male_n)])
    g_y[, male_share := ifelse(tot > 0, male_n / tot, NA_real_)]

    out_y <- merge(size_y, g_y[, .(cluster_id, year, male_share)], by=c("cluster_id","year"), all.x=TRUE)
  }

  write_csv(as_tibble(out_y),
            file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_year_maleShare__", LAST10_START, "_", LAST10_END, ".csv")))

  out_c <- as_tibble(out_y) %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      male_share_10y   = mean(male_share, na.rm = TRUE),
      years_used_10y   = dplyr::n(),
      .groups = "drop"
    ) %>%
    filter(is.finite(cluster_size_10y), cluster_size_10y > 0)

  write_csv(out_c,
            file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_const_maleShare__", LAST10_START, "_", LAST10_END, ".csv")))
  out_c
}

# =========================
# 4) Read parquet via duckdb (no arrow)
# =========================
ts_msg("Reading parquet via duckdb (no arrow)...")

con <- dbConnect(duckdb(), dbdir=":memory:")

cols_needed <- c(
  "user_id","year","n_patents",
  "first_country",
  "first_startdate_edu","first_startdate_pos",
  PARENT_VAR,
  GENDER_VAR
)
cols_needed <- unique(cols_needed)
select_sql <- paste(cols_needed, collapse = ", ")

sql <- sprintf("
  SELECT %s
  FROM parquet_scan('%s/**/*.parquet')
  WHERE year BETWEEN %d AND %d
", select_sql, INPUT_DIR, YEAR_START, YEAR_END)

df_base <- dbGetQuery(con, sql) %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    first_startdate_edu = as.character(first_startdate_edu),
    first_startdate_pos = as.character(first_startdate_pos),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!GENDER_VAR := str_trim(as.character(.data[[GENDER_VAR]]))
  )

dbDisconnect(con, shutdown=TRUE)
ts_msg("Raw df loaded:", nrow(df_base))

# Tenure
df_base <- df_base %>%
  compute_tenure() %>%
  filter(!is.na(tenure))

# US-only + parent cluster
df_us <- df_base %>%
  filter(!is.na(first_country), first_country == US_COUNTRY) %>%
  mutate(
    cluster_id = as.character(.data[[PARENT_VAR]]),
    gender = str_trim(as.character(.data[[GENDER_VAR]]))
  ) %>%
  filter(!is.na(cluster_id), cluster_id != "")

ts_msg("US-only df:", nrow(df_us))

# Cluster measures (last-10y)
meas_const <- cluster_male_share_last10y(df_us)

# Regression dataset
dd <- df_us %>%
  filter(!is.na(n_patents), n_patents >= 0,
         is.finite(tenure), is.finite(tenure_sq)) %>%
  left_join(meas_const, by="cluster_id") %>%
  filter(is.finite(male_share_10y),
         is.finite(cluster_size_10y), cluster_size_10y > 0) %>%
  mutate(
    user_id = as.factor(user_id),
    year_fe = as.factor(year),
    cluster_id = as.factor(cluster_id)
  )

ts_msg("Merged dd:", nrow(dd))

# =========================
# 5) Regressions (PPML + OLS), UW and W
# =========================
fe_inventor   <- "user_id + year_fe"
fe_noinventor <- "year_fe"
w_formula <- ~ cluster_size_10y

fit_fepois <- function(fml, data, use_weights) {
  if (use_weights) {
    fixest::fepois(fml, data = data, vcov = ~cluster_id, weights = w_formula, notes = FALSE, warn = FALSE)
  } else {
    fixest::fepois(fml, data = data, vcov = ~cluster_id, notes = FALSE, warn = FALSE)
  }
}

fit_feols <- function(fml, data, use_weights) {
  if (use_weights) {
    fixest::feols(fml, data = data, vcov = ~cluster_id, weights = w_formula, notes = FALSE, warn = FALSE)
  } else {
    fixest::feols(fml, data = data, vcov = ~cluster_id, notes = FALSE, warn = FALSE)
  }
}

run_sample <- function(dsub, sample_tag) {

  rhs <- "male_share_10y + cluster_size_10y + tenure + tenure_sq"
  out_list <- list()

  for (use_w in c(FALSE, TRUE)) {
    w_tag <- if (use_w) "W" else "UW"

    f_ppml_fe   <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ppml_nofe <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))
    f_ols_fe    <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ols_nofe  <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))

    m_ppml_fe   <- fit_fepois(f_ppml_fe,   dsub, use_w)
    m_ppml_nofe <- fit_fepois(f_ppml_nofe, dsub, use_w)
    m_ols_fe    <- fit_feols (f_ols_fe,    dsub, use_w)
    m_ols_nofe  <- fit_feols (f_ols_nofe,  dsub, use_w)

    out_list[[w_tag]] <- bind_rows(
      extract_fixest_terms(m_ppml_fe,   paste0("USONLY__", sample_tag, "__maleShare__base__", w_tag, "__PPML_inventorFE")),
      extract_fixest_terms(m_ppml_nofe, paste0("USONLY__", sample_tag, "__maleShare__base__", w_tag, "__PPML_noInventorFE")),
      extract_fixest_terms(m_ols_fe,    paste0("USONLY__", sample_tag, "__maleShare__base__", w_tag, "__OLS_inventorFE")),
      extract_fixest_terms(m_ols_nofe,  paste0("USONLY__", sample_tag, "__maleShare__base__", w_tag, "__OLS_noInventorFE"))
    )
  }

  bind_rows(out_list) %>%
    mutate(analysis="USONLY", sample=sample_tag, spec="maleShare", controls="base", n_obs=nrow(dsub))
}

# Split by inventor gender (not immigrant status)
dd_male   <- dd %>% filter(gender == "Male")
dd_female <- dd %>% filter(gender == "Female")
dd_all    <- dd

ts_msg("Running samples: MALE =", nrow(dd_male), "| FEMALE =", nrow(dd_female), "| ALL =", nrow(dd_all))

tab_all <- bind_rows(
  run_sample(dd_male,   "MALE"),
  run_sample(dd_female, "FEMALE"),
  run_sample(dd_all,    "ALL")
)

write_csv(tab_all, file.path(DIRS$tables, "USONLY__maleShare_gender_only__coef_table.csv"))

ts_msg("DONE. Outputs in:", OUT_BASE)
