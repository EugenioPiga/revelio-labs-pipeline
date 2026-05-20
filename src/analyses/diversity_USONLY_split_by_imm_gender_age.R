##############################################################################
# US-ONLY inventor-year sample (first_country == US) — Parent only
# Goal:
#  (i) run diversity regressions separately for natives vs immigrants
# (ii) within same US-only sample, also compute gender and tenure ("age") diversity
# Stage B removed.
##############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","tidyr","tibble","parallel")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(tidyr); library(tibble)
})

set.seed(123)
data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))

# =========================
# 1) Paths + knobs
# =========================
INPUT    <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_BASE <- "/home/epiga/revelio_labs/output/diversity_USONLY_split_by_imm_gender_age"

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

# "Age diversity" via tenure bins (edit if you prefer different bins)
TENURE_BIN_WIDTH <- 5L   # 5-year bins: 0-4, 5-9, ...
TENURE_BIN_MAX   <- 50L  # cap

INCLUDE_MISSING_AS_CATEGORY <- FALSE

# =========================
# 2) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}

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

make_dirs <- function() {
  dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)
  dirs <- list(
    tables       = file.path(OUT_BASE, "tables"),
    cluster_meas = file.path(OUT_BASE, "cluster_measures_last10y"),
    fe           = file.path(OUT_BASE, "fixed_effects_cluster")
  )
  lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE)
  dirs
}
DIRS <- make_dirs()

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

make_tenure_bin <- function(df) {
  df %>%
    mutate(
      tenure_bin = ifelse(is.na(tenure), NA_integer_,
                          pmin(as.integer(floor(tenure / TENURE_BIN_WIDTH) * TENURE_BIN_WIDTH),
                               TENURE_BIN_MAX)),
      tenure_bin = as.character(tenure_bin)  # categorical for HHI
    )
}

# =========================
# 4) Generic 1-HHI cluster-year diversity for any categorical var
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

  # one row per user-year-cluster
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

# Keep your existing by-immigrant-group diversity for origin/job/edu (optional, not required for split regressions)
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

# =========================
# 5) Build last-10y time-invariant cluster constants (US-only sample)
# =========================
make_timeinvariant_measures_USONLY <- function(df_us, cluster_var) {
  ts_msg("Building last-10y (", LAST10_START, "-", LAST10_END, ") cluster measures within US-only sample")

  df10 <- df_us %>% filter(year %in% YEARS_LAST10)

  # origin/job/edu diversity (overall + nat/imm versions kept)
  origin_y <- cluster_year_div_by_group(df10, cluster_var, "origin_country", "origin", MIN_CLUSTER)
  job_y    <- cluster_year_div_by_group(df10, cluster_var, "first_pos_country", "job", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("job_"))
  edu_y    <- cluster_year_div_by_group(df10, cluster_var, "first_university_country", "edu", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("edu_"))

  # gender diversity (overall only)
  gender_y <- cluster_year_div_any(df10, cluster_var, "sex_predicted", "gender", MIN_CLUSTER) %>%
    select(cluster_id, year, gender_div)

  # tenure-bin ("age") diversity (overall only)
  age_y <- cluster_year_div_any(df10, cluster_var, "tenure_bin", "tenurebin", MIN_CLUSTER) %>%
    select(cluster_id, year, tenurebin_div)

  stats_y <- origin_y %>%
    left_join(job_y, by = c("cluster_id","year")) %>%
    left_join(edu_y, by = c("cluster_id","year")) %>%
    left_join(gender_y, by = c("cluster_id","year")) %>%
    left_join(age_y, by = c("cluster_id","year"))

  write_csv(stats_y, file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_year__", LAST10_START, "_", LAST10_END, ".csv")))

  # collapse to cluster constants
  stats_c <- stats_y %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      immig_share_10y  = mean(immig_share, na.rm = TRUE),
      immig_share_sq_10y = immig_share_10y^2,

      # baseline diversity (overall)
      origin_div_10y = mean(origin_div_all, na.rm = TRUE),
      job_div_10y    = mean(job_div_all, na.rm = TRUE),
      edu_div_10y    = mean(edu_div_all, na.rm = TRUE),

      # nat/imm diversity versions (kept for reference / optional)
      origin_div_nat_10y = mean(origin_div_nat, na.rm = TRUE),
      job_div_nat_10y    = mean(job_div_nat, na.rm = TRUE),
      edu_div_nat_10y    = mean(edu_div_nat, na.rm = TRUE),
      origin_div_imm_10y = mean(origin_div_imm, na.rm = TRUE),
      job_div_imm_10y    = mean(job_div_imm, na.rm = TRUE),
      edu_div_imm_10y    = mean(edu_div_imm, na.rm = TRUE),

      # new diversity types
      gender_div_10y   = mean(gender_div, na.rm = TRUE),
      tenurebin_div_10y = mean(tenurebin_div, na.rm = TRUE),

      years_used_10y = dplyr::n(),
      .groups = "drop"
    ) %>%
    mutate(
      log_size_10y = ifelse(is.finite(cluster_size_10y) & cluster_size_10y > 0, log(cluster_size_10y), NA_real_)
    ) %>%
    filter(
      is.finite(cluster_size_10y), cluster_size_10y > 0,
      is.finite(immig_share_10y)
    )

  write_csv(stats_c, file.path(DIRS$cluster_meas, paste0("USONLY__parent_cluster_const__", LAST10_START, "_", LAST10_END, ".csv")))
  stats_c
}

# =========================
# 6) Load Arrow + compute first_pos_country from FULL panel (same as your code)
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")

need_cols(names(ds), c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  "sex_predicted",
  IMMIG_VAR, PARENT_VAR
))

ts_msg("Computing first_pos_country from FULL panel...")
ds_pos <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(
    pos_country_y = if_else(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = if_else(pos_country_y == "", NA_character_, pos_country_y)
  ) %>%
  filter(!is.na(user_id), !is.na(year), !is.na(pos_country_y))

ds_first_year <- ds_pos %>%
  group_by(user_id) %>%
  summarise(first_pos_year = min(year, na.rm = TRUE), .groups = "drop")

first_pos_tbl <- ds_pos %>%
  inner_join(ds_first_year, by = "user_id") %>%
  filter(year == first_pos_year) %>%
  select(user_id, first_pos_country = pos_country_y) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    first_pos_country = str_trim(as.character(first_pos_country))
  ) %>%
  distinct(user_id, .keep_all = TRUE)

ts_msg("Collecting regression window:", YEAR_START, "to", YEAR_END)
df_base <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(
    user_id, year, n_patents,
    first_country,
    first_university_country,
    first_startdate_edu, first_startdate_pos,
    sex_predicted,
    !!sym(IMMIG_VAR), !!sym(PARENT_VAR)
  ) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    sex_predicted = str_trim(as.character(sex_predicted)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    first_university_country = str_trim(as.character(first_university_country)),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]])
  ) %>%
  left_join(first_pos_tbl, by = "user_id")

ts_msg("Deriving tenure + origin_country ...")
df_base <- df_base %>%
  compute_tenure() %>%
  filter(!is.na(tenure)) %>%
  make_origin_country() %>%
  make_tenure_bin()

# =========================
# 7) Restrict to US-only inventor-year sample
# =========================
df_us <- df_base %>%
  filter(!is.na(first_country), first_country == US_COUNTRY) %>%
  mutate(
    cluster_id = as.character(.data[[PARENT_VAR]])
  ) %>%
  filter(!is.na(cluster_id), cluster_id != "")

ts_msg("US-only df size:", nrow(df_us))

# =========================
# 8) Build cluster constants in US-only sample + run regressions split by imm
# =========================
meas_const <- make_timeinvariant_measures_USONLY(df_us, PARENT_VAR)

dd <- df_us %>%
  filter(!is.na(user_id), user_id != "",
         !is.na(year), year >= YEAR_START, year <= YEAR_END,
         !is.na(n_patents), n_patents >= 0,
         is.finite(tenure), is.finite(tenure_sq)) %>%
  left_join(meas_const, by = "cluster_id") %>%
  mutate(
    user_id = as.factor(user_id),
    year_fe = as.factor(year),
    cluster_id = as.factor(cluster_id),
    imm = as.integer(.data[[IMMIG_VAR]])
  )

# RHS specs (you can add immig_share_10y control if you want; I include both options)
SPECS <- list(
  origin = "origin_div_10y",
  job    = "job_div_10y",
  edu    = "edu_div_10y",
  gender = "gender_div_10y",
  age    = "tenurebin_div_10y"
)

fe_inventor   <- "user_id + year_fe"
fe_noinventor <- "year_fe"

# run helper: one spec, one subsample, with/without inventor FE, PPML+OLS
run_one <- function(dsub, spec_nm, div_var, sample_tag, add_imm_share_ctrl = FALSE) {
  rhs <- paste0(div_var, " + cluster_size_10y + tenure + tenure_sq")
  if (add_imm_share_ctrl) rhs <- paste0(rhs, " + immig_share_10y")

  # ensure vars finite
  dsub <- dsub %>%
    filter(is.finite(.data[[div_var]]),
           is.finite(cluster_size_10y),
           is.finite(immig_share_10y))

  if (nrow(dsub) == 0) return(tibble())

  m_ppml_fe <- fixest::fepois(
    as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor)),
    data = dsub, vcov = ~cluster_id, notes = FALSE, warn = FALSE
  )
  m_ppml_nofe <- fixest::fepois(
    as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor)),
    data = dsub, vcov = ~cluster_id, notes = FALSE, warn = FALSE
  )
  m_ols_fe <- fixest::feols(
    as.formula(paste0("n_patents ~ ", rhs, " | ", fe_inventor)),
    data = dsub, vcov = ~cluster_id, notes = FALSE, warn = FALSE
  )
  m_ols_nofe <- fixest::feols(
    as.formula(paste0("n_patents ~ ", rhs, " | ", fe_noinventor)),
    data = dsub, vcov = ~cluster_id, notes = FALSE, warn = FALSE
  )

  ctrl_tag <- if (add_imm_share_ctrl) "base_plus_immShare10y" else "base"

  bind_rows(
    extract_fixest_terms(m_ppml_fe,   paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__PPML_inventorFE")),
    extract_fixest_terms(m_ppml_nofe, paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__PPML_noInventorFE")),
    extract_fixest_terms(m_ols_fe,    paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__OLS_inventorFE")),
    extract_fixest_terms(m_ols_nofe,  paste0("USONLY__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__OLS_noInventorFE"))
  ) %>%
    mutate(
      analysis = "USONLY",
      sample = sample_tag,
      spec = spec_nm,
      controls = ctrl_tag,
      n_obs = nrow(dsub)
    )
}

all_tabs <- list()

for (spec_nm in names(SPECS)) {
  div_var <- SPECS[[spec_nm]]

  # split by imm status
  d_nat <- dd %>% filter(imm == 0L)
  d_imm <- dd %>% filter(imm == 1L)

  ts_msg("Running spec:", spec_nm, "| natives N=", nrow(d_nat), "| immigrants N=", nrow(d_imm))

  # base (no immig_share control)
  all_tabs[[paste0(spec_nm, "__nat__base")]] <- run_one(d_nat, spec_nm, div_var, "NATIVE", add_imm_share_ctrl = FALSE)
  all_tabs[[paste0(spec_nm, "__imm__base")]] <- run_one(d_imm, spec_nm, div_var, "IMMIGRANT", add_imm_share_ctrl = FALSE)

  # optional: add immig_share_10y as control (sometimes useful even when splitting)
  all_tabs[[paste0(spec_nm, "__nat__immShare")]] <- run_one(d_nat, spec_nm, div_var, "NATIVE", add_imm_share_ctrl = TRUE)
  all_tabs[[paste0(spec_nm, "__imm__immShare")]] <- run_one(d_imm, spec_nm, div_var, "IMMIGRANT", add_imm_share_ctrl = TRUE)

  write_csv(bind_rows(all_tabs), file.path(DIRS$tables, "USONLY__coef_table_PROGRESS.csv"))
}

tab_all <- bind_rows(all_tabs)
write_csv(tab_all, file.path(DIRS$tables, "USONLY__coef_table_ALL.csv"))

# Save baseline PPML cluster FE (within US-only sample)
ts_msg("Estimating baseline PPML with cluster FE (US-only)...")
m_base <- fixest::fepois(
  as.formula("n_patents ~ tenure + tenure_sq | user_id + year_fe + cluster_id"),
  data = dd, notes = FALSE, warn = FALSE
)
fe_cluster <- fixest::fixef(m_base)[["cluster_id"]]
if (!is.null(fe_cluster)) {
  write_csv(
    tibble(cluster_id = names(fe_cluster), fe_value = as.numeric(fe_cluster)),
    file.path(DIRS$fe, "FE_cluster__USONLY__PPMLbaseline.csv")
  )
}

ts_msg("DONE. Outputs in:", OUT_BASE)
##############################################################################
