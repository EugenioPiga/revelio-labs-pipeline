##############################################################################
# US-ONLY inventor-year sample (first_country == US) — Parent only
# Run regressions:
#   (A) Split by immigrant vs native
#   (B) ALSO run the same specifications on the FULL sample (no split)
#
# Diversity types (UPDATED):
#   - origin (constructed origin_country)
#   - job (first_pos_country)
#   - edu (first_university_country)
#   - gender (au_sex_predicted)
#   - ethnicity (au_ethnicity_predicted)
#   - maxdegree (edu_last_degree)
#
# Requested features preserved:
#   (i) Immigrant share-only specs (no diversity regressor), for NATIVE/IMMIGRANT/ALL:
#         n_patents ~ immig_share_10y (+ immig_share_sq_10y optional)
#                   + cluster_size_10y + tenure + tenure_sq
#       PPML + OLS; inventor FE and no inventor FE.
#   (ii) Apply weights = total cluster size (cluster_size_10y) in EVERY regression,
#       and also report UW versions (as in your previous script).
#
# No arrow: uses duckdb to read parquet.
##############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("duckdb","DBI","dplyr","readr","stringr","data.table","fixest","tidyr","tibble","parallel")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(duckdb); library(DBI)
  library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(tidyr); library(tibble)
})

set.seed(123)
data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))

# =========================
# 1) Paths + knobs
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_BASE  <- "/home/epiga/revelio_labs/output/diversity_USONLY_splitImm_demo_degree"

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
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

# UPDATED categorical variables for diversity
GENDER_VAR    <- "au_sex_predicted"
ETHNIC_VAR    <- "au_ethnicity_predicted"
MAXDEGREE_VAR <- "edu_last_degree"

INCLUDE_MISSING_AS_CATEGORY <- FALSE

dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)
DIRS <- list(
  tables       = file.path(OUT_BASE, "tables"),
  cluster_meas = file.path(OUT_BASE, "cluster_measures_last10y"),
  fe           = file.path(OUT_BASE, "fixed_effects_cluster")
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

# =========================
# 3) Derived variables
# =========================
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

make_origin_country <- function(df) {
  df %>%
    mutate(
      uni_c  = str_trim(as.character(first_university_country)),
      pos_c  = str_trim(as.character(first_pos_country)),
      uni_ok = !is.na(uni_c) & uni_c != "" & uni_c != US_COUNTRY,
      pos_ok = !is.na(pos_c) & pos_c != "" & pos_c != US_COUNTRY,
      origin_country = case_when(
        .data[[IMMIG_VAR]] == 0L ~ US_COUNTRY,
        .data[[IMMIG_VAR]] == 1L & uni_ok ~ uni_c,
        .data[[IMMIG_VAR]] == 1L & !uni_ok & pos_ok ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# =========================
# 4) Diversity functions (1 - HHI)
# =========================
cluster_year_div_any <- function(df, cluster_var, cat_var, div_prefix, min_cluster = MIN_CLUSTER) {

  keep <- c("user_id","year", IMMIG_VAR, cluster_var, cat_var)
  dt <- as.data.table(df[, keep])

  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, cat_var, "v_raw")

  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]

  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year) & !is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]

  dt[, v_raw := trimws(as.character(v_raw))]
  dt[v_raw == "" | v_raw == "empty", v_raw := NA_character_]

  comp <- unique(dt[, .(user_id, year, cluster_id, imm, v_raw)])

  size_df <- comp[, .(cluster_size = uniqueN(user_id),
                      immig_share = mean(imm, na.rm = TRUE)),
                  by = .(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]

  comp <- merge(comp, size_df[, .(cluster_id, year)], by = c("cluster_id","year"), all = FALSE)

  if (INCLUDE_MISSING_AS_CATEGORY) {
    comp[, v := fifelse(is.na(v_raw), "MISSING", v_raw)]
  } else {
    comp <- comp[!is.na(v_raw)]
    comp[, v := v_raw]
  }

  if (nrow(comp) == 0) {
    out <- size_df
    out[, div := NA_real_]
    setnames(out, "div", paste0(div_prefix, "_div"))
    return(as_tibble(out))
  }

  tmp <- comp[, .(n = uniqueN(user_id)), by = .(cluster_id, year, v)]
  tmp[, tot := sum(n), by = .(cluster_id, year)]
  tmp[, share_sq := (n / tot)^2]
  out <- tmp[, .(div = 1 - sum(share_sq)), by = .(cluster_id, year)]

  out <- merge(size_df, out, by = c("cluster_id","year"), all.x = TRUE)
  setnames(out, "div", paste0(div_prefix, "_div"))

  as_tibble(out)
}

cluster_year_div_by_group <- function(df, cluster_var, country_var, div_prefix, min_cluster = MIN_CLUSTER) {

  keep <- c("user_id","year",IMMIG_VAR,cluster_var,country_var)
  dt <- as.data.table(df[, keep])

  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "cvar_raw")

  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]

  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year) & !is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]

  comp <- unique(dt[, .(user_id, year, cluster_id, imm, cvar_raw)])
  size_df <- comp[, .(cluster_size = uniqueN(user_id), immig_share = mean(imm, na.rm = TRUE)),
                  by = .(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]

  div_one <- function(xdt) {
    xdt <- xdt[!is.na(cvar_raw) & cvar_raw != "" & cvar_raw != "empty"]
    if (nrow(xdt) == 0) return(NULL)
    tmp <- xdt[, .(n = uniqueN(user_id)), by = .(cluster_id, year, v = cvar_raw)]
    tmp[, tot := sum(n), by = .(cluster_id, year)]
    tmp[, share_sq := (n / tot)^2]
    out <- tmp[, .(div = 1 - sum(share_sq)), by = .(cluster_id, year)]
    out
  }

  div_all <- div_one(comp)
  div_nat <- div_one(comp[imm == 0L])
  div_imm <- div_one(comp[imm == 1L])

  out <- merge(size_df, div_all, by = c("cluster_id","year"), all.x = TRUE)
  setnames(out, "div", paste0(div_prefix, "_div_all"))

  if (!is.null(div_nat)) {
    out <- merge(out, div_nat, by = c("cluster_id","year"), all.x = TRUE)
    setnames(out, "div", paste0(div_prefix, "_div_nat"))
  } else out[, paste0(div_prefix, "_div_nat") := NA_real_]

  if (!is.null(div_imm)) {
    out <- merge(out, div_imm, by = c("cluster_id","year"), all.x = TRUE)
    setnames(out, "div", paste0(div_prefix, "_div_imm"))
  } else out[, paste0(div_prefix, "_div_imm") := NA_real_]

  as_tibble(out)
}

make_timeinvariant_measures_USONLY <- function(df_us) {
  ts_msg("Building last-10y measures in US-only sample:", LAST10_START, "-", LAST10_END)
  df10 <- df_us %>% filter(year %in% YEARS_LAST10)

  # Countries: keep the by-group version (but we will use the _all mean for regressions)
  origin_y <- cluster_year_div_by_group(df10, "cluster_id", "origin_country", "origin", MIN_CLUSTER)
  job_y    <- cluster_year_div_by_group(df10, "cluster_id", "first_pos_country", "job", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("job_"))
  edu_y    <- cluster_year_div_by_group(df10, "cluster_id", "first_university_country", "edu", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("edu_"))

  # NEW: gender / ethnicity / maxdegree (overall only)
  gender_y <- cluster_year_div_any(df10, "cluster_id", GENDER_VAR, "gender", MIN_CLUSTER) %>%
    select(cluster_id, year, gender_div)
  ethn_y   <- cluster_year_div_any(df10, "cluster_id", ETHNIC_VAR, "ethnicity", MIN_CLUSTER) %>%
    select(cluster_id, year, ethnicity_div)
  deg_y    <- cluster_year_div_any(df10, "cluster_id", MAXDEGREE_VAR, "maxdegree", MIN_CLUSTER) %>%
    select(cluster_id, year, maxdegree_div)

  stats_y <- origin_y %>%
    left_join(job_y,    by = c("cluster_id","year")) %>%
    left_join(edu_y,    by = c("cluster_id","year")) %>%
    left_join(gender_y, by = c("cluster_id","year")) %>%
    left_join(ethn_y,   by = c("cluster_id","year")) %>%
    left_join(deg_y,    by = c("cluster_id","year"))

  write_csv(stats_y, file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_year__", LAST10_START, "_", LAST10_END, ".csv")))

  stats_c <- stats_y %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      immig_share_10y  = mean(immig_share, na.rm = TRUE),
      immig_share_sq_10y = immig_share_10y^2,

      origin_div_10y = mean(origin_div_all, na.rm = TRUE),
      job_div_10y    = mean(job_div_all, na.rm = TRUE),
      edu_div_10y    = mean(edu_div_all, na.rm = TRUE),

      gender_div_10y   = mean(gender_div,   na.rm = TRUE),
      ethnicity_div_10y= mean(ethnicity_div,na.rm = TRUE),
      maxdegree_div_10y= mean(maxdegree_div,na.rm = TRUE),

      years_used_10y = dplyr::n(),
      .groups = "drop"
    ) %>%
    mutate(
      log_size_10y = ifelse(is.finite(cluster_size_10y) & cluster_size_10y > 0, log(cluster_size_10y), NA_real_)
    ) %>%
    filter(is.finite(cluster_size_10y), cluster_size_10y > 0,
           is.finite(immig_share_10y))

  write_csv(stats_c, file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_const__", LAST10_START, "_", LAST10_END, ".csv")))
  stats_c
}

# =========================
# 5) Read parquet via duckdb (no arrow)
# =========================
ts_msg("Reading parquet via duckdb (no arrow)...")

con <- dbConnect(duckdb(), dbdir=":memory:")

cols_needed <- c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  "first_parent_rcid","last_parent_rcid",
  "immig_first_deg_or_job_nonUS",
  GENDER_VAR, ETHNIC_VAR, MAXDEGREE_VAR
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
    last_country  = str_trim(as.character(last_country)),

    first_university_country = str_trim(as.character(first_university_country)),
    first_startdate_edu = as.character(first_startdate_edu),
    first_startdate_pos = as.character(first_startdate_pos),

    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),

    # NEW vars (as strings; cleaned later inside div fn too)
    !!GENDER_VAR := str_trim(as.character(.data[[GENDER_VAR]])),
    !!ETHNIC_VAR := str_trim(as.character(.data[[ETHNIC_VAR]])),
    !!MAXDEGREE_VAR := str_trim(as.character(.data[[MAXDEGREE_VAR]]))
  )

dbDisconnect(con, shutdown=TRUE)
ts_msg("Raw df loaded:", nrow(df_base))

# Compute first_pos_country within regression window (as before)
ts_msg("Computing first_pos_country within regression window...")
df_base <- df_base %>%
  mutate(
    pos_country_y = ifelse(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = ifelse(pos_country_y == "", NA_character_, pos_country_y)
  )

first_pos_tbl <- df_base %>%
  filter(!is.na(pos_country_y), !is.na(user_id), !is.na(year)) %>%
  group_by(user_id) %>%
  summarise(first_pos_year = min(year, na.rm = TRUE), .groups="drop") %>%
  inner_join(df_base %>% select(user_id, year, pos_country_y),
             by=c("user_id"="user_id", "first_pos_year"="year")) %>%
  transmute(user_id, first_pos_country = pos_country_y) %>%
  distinct(user_id, .keep_all=TRUE)

df_base <- df_base %>%
  left_join(first_pos_tbl, by="user_id")

# Tenure + origin
df_base <- df_base %>%
  compute_tenure() %>%
  filter(!is.na(tenure)) %>%
  make_origin_country()

# =========================
# 6) US-only sample + measures
# =========================
df_us <- df_base %>%
  filter(!is.na(first_country), first_country == US_COUNTRY) %>%
  mutate(cluster_id = as.character(.data[[PARENT_VAR]])) %>%
  filter(!is.na(cluster_id), cluster_id != "")

ts_msg("US-only df:", nrow(df_us))

meas_const <- make_timeinvariant_measures_USONLY(df_us)

dd <- df_us %>%
  filter(!is.na(n_patents), n_patents >= 0,
         is.finite(tenure), is.finite(tenure_sq)) %>%
  left_join(meas_const, by="cluster_id") %>%
  mutate(
    user_id = as.factor(user_id),
    year_fe = as.factor(year),
    cluster_id = as.factor(cluster_id),
    imm = as.integer(.data[[IMMIG_VAR]])
  )

ts_msg("Merged dd:", nrow(dd))

# =========================
# 7) Run regressions: NATIVE, IMMIGRANT, and ALL
# =========================
SPECS <- list(
  origin    = "origin_div_10y",
  job       = "job_div_10y",
  edu       = "edu_div_10y",
  gender    = "gender_div_10y",
  ethnicity = "ethnicity_div_10y",
  maxdegree = "maxdegree_div_10y"
)

fe_inventor   <- "user_id + year_fe"
fe_noinventor <- "year_fe"

fit_fepois <- function(fml, data, vcov, use_weights, w_formula) {
  if (use_weights) {
    fixest::fepois(fml, data = data, vcov = vcov, weights = w_formula, notes = FALSE, warn = FALSE)
  } else {
    fixest::fepois(fml, data = data, vcov = vcov, notes = FALSE, warn = FALSE)
  }
}

fit_feols <- function(fml, data, vcov, use_weights, w_formula) {
  if (use_weights) {
    fixest::feols(fml, data = data, vcov = vcov, weights = w_formula, notes = FALSE, warn = FALSE)
  } else {
    fixest::feols(fml, data = data, vcov = vcov, notes = FALSE, warn = FALSE)
  }
}

# Weights in ALL regressions: total cluster size (NOT subgroup size)
w_formula <- ~ cluster_size_10y

run_one <- function(dsub, spec_nm, div_var, sample_tag, add_imm_share_ctrl = FALSE) {

  rhs <- paste0(div_var, " + cluster_size_10y + tenure + tenure_sq")
  if (add_imm_share_ctrl) rhs <- paste0(rhs, " + immig_share_10y")

  dsub <- dsub %>%
    filter(is.finite(.data[[div_var]]),
           is.finite(cluster_size_10y), cluster_size_10y > 0,
           is.finite(immig_share_10y),
           is.finite(tenure), is.finite(tenure_sq))

  if (nrow(dsub) == 0) return(tibble())

  ctrl_tag <- if (add_imm_share_ctrl) "base_plus_immShare10y" else "base"

  out_list <- list()
  for (use_w in c(FALSE, TRUE)) {
    w_tag <- if (use_w) "W" else "UW"

    f_ppml_fe   <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ppml_nofe <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))
    f_ols_fe    <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ols_nofe  <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))

    m_ppml_fe   <- fit_fepois(f_ppml_fe,   dsub, ~cluster_id, use_w, w_formula)
    m_ppml_nofe <- fit_fepois(f_ppml_nofe, dsub, ~cluster_id, use_w, w_formula)
    m_ols_fe    <- fit_feols (f_ols_fe,    dsub, ~cluster_id, use_w, w_formula)
    m_ols_nofe  <- fit_feols (f_ols_nofe,  dsub, ~cluster_id, use_w, w_formula)

    out_list[[w_tag]] <- bind_rows(
      extract_fixest_terms(m_ppml_fe,   paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__", w_tag, "__PPML_inventorFE")),
      extract_fixest_terms(m_ppml_nofe, paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__", w_tag, "__PPML_noInventorFE")),
      extract_fixest_terms(m_ols_fe,    paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__", w_tag, "__OLS_inventorFE")),
      extract_fixest_terms(m_ols_nofe,  paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__", w_tag, "__OLS_noInventorFE"))
    )
  }

  bind_rows(out_list) %>%
    mutate(analysis="USONLY", sample=sample_tag, spec=spec_nm, controls=ctrl_tag, n_obs=nrow(dsub))
}

# Immigrant share ONLY (no diversity regressor)
run_immshare_only <- function(dsub, sample_tag, add_sq = FALSE) {

  rhs <- "immig_share_10y + cluster_size_10y + tenure + tenure_sq"
  ctrl_tag <- "immShare_only"
  if (add_sq) {
    rhs <- paste0(rhs, " + immig_share_sq_10y")
    ctrl_tag <- "immShare_only_sq"
  }

  need_vars <- c("n_patents","immig_share_10y","cluster_size_10y","tenure","tenure_sq")
  if (add_sq) need_vars <- c(need_vars, "immig_share_sq_10y")

  dsub <- dsub %>%
    filter(across(all_of(need_vars), ~ is.finite(.x))) %>%
    filter(cluster_size_10y > 0)

  if (nrow(dsub) == 0) return(tibble())

  out_list <- list()
  for (use_w in c(FALSE, TRUE)) {
    w_tag <- if (use_w) "W" else "UW"

    f_ppml_fe   <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ppml_nofe <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))
    f_ols_fe    <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor))
    f_ols_nofe  <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor))

    m_ppml_fe   <- fit_fepois(f_ppml_fe,   dsub, ~cluster_id, use_w, w_formula)
    m_ppml_nofe <- fit_fepois(f_ppml_nofe, dsub, ~cluster_id, use_w, w_formula)
    m_ols_fe    <- fit_feols (f_ols_fe,    dsub, ~cluster_id, use_w, w_formula)
    m_ols_nofe  <- fit_feols (f_ols_nofe,  dsub, ~cluster_id, use_w, w_formula)

    out_list[[w_tag]] <- bind_rows(
      extract_fixest_terms(m_ppml_fe,   paste0("USONLY__", sample_tag, "__IMM_SHARE_ONLY__", ctrl_tag, "__", w_tag, "__PPML_inventorFE")),
      extract_fixest_terms(m_ppml_nofe, paste0("USONLY__", sample_tag, "__IMM_SHARE_ONLY__", ctrl_tag, "__", w_tag, "__PPML_noInventorFE")),
      extract_fixest_terms(m_ols_fe,    paste0("USONLY__", sample_tag, "__IMM_SHARE_ONLY__", ctrl_tag, "__", w_tag, "__OLS_inventorFE")),
      extract_fixest_terms(m_ols_nofe,  paste0("USONLY__", sample_tag, "__IMM_SHARE_ONLY__", ctrl_tag, "__", w_tag, "__OLS_noInventorFE"))
    )
  }

  bind_rows(out_list) %>%
    mutate(analysis="USONLY", sample=sample_tag, spec="IMM_SHARE_ONLY", controls=ctrl_tag, n_obs=nrow(dsub))
}

all_tabs <- list()

for (spec_nm in names(SPECS)) {
  div_var <- SPECS[[spec_nm]]

  d_nat <- dd %>% filter(imm == 0L)
  d_imm <- dd %>% filter(imm == 1L)
  d_all <- dd

  ts_msg("Spec:", spec_nm, "| nat:", nrow(d_nat), "| imm:", nrow(d_imm), "| all:", nrow(d_all))

  # Split samples
  all_tabs[[paste0(spec_nm,"__nat__base")]]     <- run_one(d_nat, spec_nm, div_var, "NATIVE",   FALSE)
  all_tabs[[paste0(spec_nm,"__imm__base")]]     <- run_one(d_imm, spec_nm, div_var, "IMMIGRANT",FALSE)
  all_tabs[[paste0(spec_nm,"__nat__immShare")]] <- run_one(d_nat, spec_nm, div_var, "NATIVE",   TRUE)
  all_tabs[[paste0(spec_nm,"__imm__immShare")]] <- run_one(d_imm, spec_nm, div_var, "IMMIGRANT",TRUE)

  # FULL sample (NEW)
  all_tabs[[paste0(spec_nm,"__all__base")]]     <- run_one(d_all, spec_nm, div_var, "ALL", FALSE)
  all_tabs[[paste0(spec_nm,"__all__immShare")]] <- run_one(d_all, spec_nm, div_var, "ALL", TRUE)

  write_csv(bind_rows(all_tabs), file.path(DIRS$tables, "USONLY__coef_table_PROGRESS.csv"))
}

# Immigrant share by itself — NATIVE, IMMIGRANT, ALL
ts_msg("Running immig_share-only specs (no diversity regressor), weighted by total cluster size...")

d_nat_all <- dd %>% filter(imm == 0L)
d_imm_all <- dd %>% filter(imm == 1L)
d_all_all <- dd

# Linear
all_tabs[["IMM_SHARE_ONLY__nat__lin"]] <- run_immshare_only(d_nat_all, "NATIVE",   add_sq = FALSE)
all_tabs[["IMM_SHARE_ONLY__imm__lin"]] <- run_immshare_only(d_imm_all, "IMMIGRANT",add_sq = FALSE)
all_tabs[["IMM_SHARE_ONLY__all__lin"]] <- run_immshare_only(d_all_all, "ALL",      add_sq = FALSE)

# Quadratic
all_tabs[["IMM_SHARE_ONLY__nat__sq"]]  <- run_immshare_only(d_nat_all, "NATIVE",   add_sq = TRUE)
all_tabs[["IMM_SHARE_ONLY__imm__sq"]]  <- run_immshare_only(d_imm_all, "IMMIGRANT",add_sq = TRUE)
all_tabs[["IMM_SHARE_ONLY__all__sq"]]  <- run_immshare_only(d_all_all, "ALL",      add_sq = TRUE)

tab_all <- bind_rows(all_tabs)
write_csv(tab_all, file.path(DIRS$tables, "USONLY__coef_table_ALL.csv"))

# =========================
# 8) Baseline cluster FE (US-only) — weighted (and UW for comparison)
# =========================
ts_msg("Estimating baseline PPML with cluster FE (US-only), both UW and W...")

for (use_w in c(FALSE, TRUE)) {
  w_tag <- if (use_w) "W" else "UW"

  if (use_w) {
    m_base <- fixest::fepois(
      as.formula("n_patents ~ tenure + tenure_sq | user_id + year_fe + cluster_id"),
      data = dd, weights = w_formula, notes = FALSE, warn = FALSE
    )
  } else {
    m_base <- fixest::fepois(
      as.formula("n_patents ~ tenure + tenure_sq | user_id + year_fe + cluster_id"),
      data = dd, notes = FALSE, warn = FALSE
    )
  }

  fe_cluster <- fixest::fixef(m_base)[["cluster_id"]]
  if (!is.null(fe_cluster)) {
    write_csv(
      tibble(cluster_id = names(fe_cluster), fe_value = as.numeric(fe_cluster)),
      file.path(DIRS$fe, paste0("FE_cluster__USONLY__PPMLbaseline__", w_tag, ".csv"))
    )
  }
}

ts_msg("DONE. Outputs in:", OUT_BASE)
