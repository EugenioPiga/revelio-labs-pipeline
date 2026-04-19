#!/usr/bin/env Rscript

##############################################################################
# Extended PPML tenure FE ladder analyses
#
#   1) NEVER control for time-invariant size in PPML ladder regressions.
#   2) Tenure-bin coefficient plots with baseline, +parent FE, +metro FE,
#      +metro-parent FE, and optional +parent-metro-year FE.
#   3) Robustness window 2000-2019 in addition to main 2010-2019.
#   4) OLS decomposition using estimated metro-parent fixed effects as LHS.
#   5) Separate analyses by gender, immigrant status, and ethnicity.
#   6) Report average patents in tenure 0-5 whenever relevant.
#   7) Separate analyses for natives by school-location-size top/bottom 20%.
#   8) Separate analyses for top/bottom 20% of early-career patents
#      within 5y and 10y from graduation (fallback: first position year).
#
# Notes / assumptions:
#   - Weighting: all regressions are UNWEIGHTED.
#   - Gender variable: prefer au_sex_predicted, fallback to sex_predicted.
#   - Ethnicity variable: prefer au_ethnicity_predicted, fallback to ethnicity_predicted.
#   - Immigrant split: use immig_first_deg_or_job_nonUS exactly.
#   - School-location-size split: uses edu_first_university_location; if missing,
#     the inventor is excluded from that particular heterogeneity exercise.
#   - Early-career patent windows use reference year:
#       grad_year = first_enddate_edu year if available,
#                   else edu_last_end_year,
#                   else first_startdate_edu year + 3,
#                   else first_startdate_pos year.
#     Cum patents in k years are summed over years [ref_year, ref_year + k - 1].
#   - For parent-metro-year FE, year FE are nested, so the spec uses:
#       user_id + parent_metro_year_fe
##############################################################################

# =========================
# 0) Threading / stability
# =========================
Sys.setenv(
  OMP_NUM_THREADS = "1",
  OPENBLAS_NUM_THREADS = "1",
  MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1",
  NUMEXPR_NUM_THREADS = "1"
)

# =========================
# 1) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c(
  "arrow", "dplyr", "readr", "stringr", "data.table", "fixest",
  "tidyr", "tibble", "ggplot2", "purrr", "forcats"
)
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(data.table)
  library(fixest)
  library(tidyr)
  library(tibble)
  library(ggplot2)
  library(purrr)
  library(forcats)
})

set.seed(123)
data.table::setDTthreads(1)
if ("setFixest_nthreads" %in% getNamespaceExports("fixest")) {
  fixest::setFixest_nthreads(1)
}

# =========================
# 2) Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR <- "/home/epiga/revelio_labs/output/ppml_tenure_ladder_extended"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

PARENT_VAR <- "first_parent_rcid"
IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
US_COUNTRY <- "United States"

WINDOWS <- list(
  `2010_2019` = c(2010L, 2019L),
  `2000_2019` = c(2000L, 2019L)
)

CHAR_FULL_START <- 2000L
CHAR_FULL_END   <- 2024L

TENURE_MAX      <- 50L
TENURE_BIN_STEP <- 5L
MIN_CLUSTER     <- 10L

TOPBOT_P <- 0.20

METRO_CANDIDATES <- c(
  "first_metro", "metro", "metro_name",
  "first_metro_area", "msa", "msa_name",
  "first_cbsa", "cbsa", "cbsa_name"
)

GENDER_CANDIDATES <- c("au_sex_predicted", "sex_predicted")
ETHNICITY_CANDIDATES <- c("au_ethnicity_predicted", "ethnicity_predicted")

# Stay consistent with uploaded ppml_tenure_ladder:
WEIGHTS <- list(
  unweighted = NULL
)

# =========================
# 3) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}

pick_first_existing <- function(candidates, available, what = "variable") {
  hit <- intersect(candidates, available)
  if (length(hit) == 0) {
    stop(paste0(
      "[ERROR] Could not find any candidate ", what, " columns. Tried: ",
      paste(candidates, collapse = ", ")
    ))
  }
  hit[1]
}

sig_stars <- function(p) {
  dplyr::case_when(
    !is.finite(p) ~ "",
    p < 0.01 ~ "***",
    p < 0.05 ~ "**",
    p < 0.1  ~ "*",
    TRUE ~ ""
  )
}

safe_name <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "X", x)
}

year_from_any <- function(x) {
  out <- suppressWarnings(as.numeric(substr(as.character(x), 1, 4)))
  out[!is.finite(out)] <- NA_real_
  out
}


standardize_ethnicity <- function(x) {
  y_raw <- str_trim(as.character(x))
  y <- str_to_lower(y_raw)

  case_when(
    is.na(y) | y == "" ~ NA_character_,
    y %in% c("unknown", "missing", "na", "n/a", "other/unknown") ~ NA_character_,
    str_detect(y, "white") ~ "White",
    str_detect(y, "asian|api|pacific islander|pacific") ~ "Asian/API",
    str_detect(y, "hispanic|latino") ~ "Hispanic",
    str_detect(y, "black|african") ~ "Black",
    str_detect(y, "native|american indian|alaska") ~ "Native",
    str_detect(y, "multiple|multiracial|multi-racial|two or more|mixed") ~ "Multiple",
    str_detect(y, "other") ~ "Other",
    TRUE ~ str_to_title(y_raw)
  )
}

preferred_ethnicity_order <- function() {
  c("White", "Asian/API", "Hispanic", "Black", "Native", "Multiple", "Other")
}

choose_reference_group <- function(x, preferred = NULL) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  ux <- unique(x)
  if (!is.null(preferred) && preferred %in% ux) return(preferred)
  names(sort(table(x), decreasing = TRUE))[1]
}

make_dirs <- function() {
  dirs <- list(
    root        = OUT_DIR,
    tables      = file.path(OUT_DIR, "tables"),
    plots       = file.path(OUT_DIR, "plots"),
    logs        = file.path(OUT_DIR, "logs"),
    fixef       = file.path(OUT_DIR, "fixef"),
    diagnostics = file.path(OUT_DIR, "diagnostics")
  )
  invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
  dirs
}
dirs <- make_dirs()

compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = year_from_any(first_startdate_edu),
      pos_year = year_from_any(first_startdate_pos),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA_real_, tenure),
      tenure_sq = tenure^2
    )
}

make_reference_year <- function(df) {
  df %>%
    mutate(
      edu_end_year_1 = year_from_any(first_enddate_edu),
      edu_end_year_2 = suppressWarnings(as.numeric(edu_last_end_year)),
      edu_start_year = year_from_any(first_startdate_edu),
      pos_start_year = year_from_any(first_startdate_pos),
      ref_year = dplyr::coalesce(
        edu_end_year_1,
        edu_end_year_2,
        ifelse(is.finite(edu_start_year), edu_start_year + 3, NA_real_),
        pos_start_year
      )
    )
}

make_tenure_bins <- function(x, max_tenure = TENURE_MAX, step = TENURE_BIN_STEP) {
  x <- suppressWarnings(as.numeric(x))
  breaks <- c(-1, seq(step, max_tenure, by = step))
  lowers <- seq(0, max_tenure - step, by = step) + c(0, rep(1, length(seq(step, max_tenure, by = step)) - 1))
  uppers <- seq(step, max_tenure, by = step)
  labels <- ifelse(lowers == 0, paste0("0-", step), paste0(lowers, "-", uppers))
  cut(x, breaks = breaks, labels = labels, include.lowest = TRUE, right = TRUE)
}

ordered_tenure_levels <- function() {
  lowers <- seq(0, TENURE_MAX - TENURE_BIN_STEP, by = TENURE_BIN_STEP) + c(0, rep(1, length(seq(TENURE_BIN_STEP, TENURE_MAX, by = TENURE_BIN_STEP)) - 1))
  uppers <- seq(TENURE_BIN_STEP, TENURE_MAX, by = TENURE_BIN_STEP)
  ifelse(lowers == 0, paste0("0-", TENURE_BIN_STEP), paste0(lowers, "-", uppers))
}

extract_fixest_terms <- function(m, model_tag) {
  if (is.null(m)) {
    return(tibble(
      model = model_tag,
      term = "__MODEL_FAILED__",
      estimate = NA_real_,
      se = NA_real_,
      stat = NA_real_,
      p = NA_real_,
      stars = "",
      collinear_dropped = ""
    ))
  }

  ct <- tryCatch(fixest::coeftable(m), error = function(e) NULL)
  dropped <- if (!is.null(m$collin.var) && length(m$collin.var) > 0) {
    paste(m$collin.var, collapse = ";")
  } else {
    ""
  }

  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(
      model = model_tag,
      term = "__NO_ESTIMABLE_COEFFICIENTS__",
      estimate = NA_real_,
      se = NA_real_,
      stat = NA_real_,
      p = NA_real_,
      stars = "",
      collinear_dropped = dropped
    ))
  }

  tibble(
    model    = model_tag,
    term     = rownames(ct),
    estimate = as.numeric(ct[, 1]),
    se       = as.numeric(ct[, 2]),
    stat     = as.numeric(ct[, 3]),
    p        = as.numeric(ct[, 4]),
    stars    = sig_stars(as.numeric(ct[, 4])),
    collinear_dropped = dropped
  )
}

run_ppml <- function(rhs, fe_part, data, wform = NULL, cluster_var = "vcov_cluster") {
  fml <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_part))
  vcov_fml <- stats::as.formula(paste0("~", cluster_var))
  tryCatch({
    fixest::fepois(
      fml,
      data = data,
      vcov = vcov_fml,
      weights = wform,
      notes = FALSE,
      warn = FALSE
    )
  }, error = function(e) {
    ts_msg("MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

run_ols <- function(lhs, rhs, fe_part, data, wform = NULL, cluster_var = "vcov_cluster") {
  fml <- as.formula(paste0(lhs, " ~ ", rhs, " | ", fe_part))
  vcov_fml <- stats::as.formula(paste0("~", cluster_var))
  tryCatch({
    fixest::feols(
      fml,
      data = data,
      vcov = vcov_fml,
      weights = wform,
      notes = FALSE,
      warn = FALSE
    )
  }, error = function(e) {
    ts_msg("OLS MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

parse_tenure_terms <- function(tab) {
  levs <- ordered_tenure_levels()
  out <- tab %>%
    filter(grepl("^tenure_bin::", term) | grepl("^tenure_bin = ", term) | grepl("^tenure_bin", term)) %>%
    mutate(
      tenure_bin = dplyr::case_when(
        grepl("::", term) ~ sub("^.*::", "", term),
        grepl(" = ", term) ~ sub("^.* = ", "", term),
        TRUE ~ sub("^tenure_bin", "", term)
      ),
      tenure_bin = gsub("^\\s+|\\s+$", "", tenure_bin),
      tenure_bin = gsub("`", "", tenure_bin),
      tenure_bin = factor(tenure_bin, levels = levs, ordered = TRUE),
      conf_low  = estimate - 1.96 * se,
      conf_high = estimate + 1.96 * se,
      p_label   = ifelse(is.finite(p), sprintf("p=%.3f", p), NA_character_)
    ) %>%
    filter(!is.na(tenure_bin))
  out
}

mean_patents_0_5 <- function(df, split_name, window_tag) {
  df %>%
    filter(!is.na(tenure_bin), as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)) %>%
    summarise(
      mean_patents_0_5 = mean(n_patents, na.rm = TRUE),
      n_obs_0_5        = n(),
      .groups = "drop"
    ) %>%
    mutate(split = split_name, window = window_tag)
}

make_quantile_split <- function(x, p = TOPBOT_P) {
  q_low <- suppressWarnings(as.numeric(stats::quantile(x, probs = p, na.rm = TRUE, type = 7)))
  q_hi  <- suppressWarnings(as.numeric(stats::quantile(x, probs = 1 - p, na.rm = TRUE, type = 7)))
  case_when(
    !is.finite(x) ~ NA_character_,
    x <= q_low    ~ "bottom20",
    x >= q_hi     ~ "top20",
    TRUE          ~ "middle60"
  )
}

save_plot <- function(p, path, width = 11, height = 7) {
  ggplot2::ggsave(path, p, width = width, height = height, dpi = 300)
}

# exact cross-group difference within a given FE spec

run_group_interaction_ppml <- function(data, group_var, group_ref, group_alt, fe_part, label_stub, fe_name = label_stub) {
  dsub <- data %>%
    filter(.data[[group_var]] %in% c(group_ref, group_alt)) %>%
    mutate(group2 = factor(.data[[group_var]], levels = c(group_ref, group_alt)))

  if (nrow(dsub) == 0) return(tibble())

  fml <- as.formula(
    paste0(
      "n_patents ~ i(tenure_bin, group2, ref = \"0-", TENURE_BIN_STEP, "\", ref2 = \"", group_ref, "\") | ",
      fe_part
    )
  )

  fit <- tryCatch({
    fixest::fepois(
      fml,
      data = dsub,
      vcov = ~vcov_cluster,
      notes = FALSE,
      warn = FALSE
    )
  }, error = function(e) {
    ts_msg("INTERACTION MODEL FAILED:", label_stub, "|", conditionMessage(e))
    NULL
  })

  if (is.null(fit)) return(tibble())

  extract_fixest_terms(fit, label_stub) %>%
    filter(grepl("tenure_bin", term)) %>%
    mutate(
      tenure_bin = sub("^.*tenure_bin::([^:]+).*$", "\\1", term),
      tenure_bin = factor(tenure_bin, levels = ordered_tenure_levels(), ordered = TRUE),
      conf_low  = estimate - 1.96 * se,
      conf_high = estimate + 1.96 * se,
      p_label   = ifelse(is.finite(p), sprintf("p=%.3f", p), NA_character_),
      group_var = group_var,
      group_ref = group_ref,
      group_alt = group_alt,
      fe_spec = label_stub,
      fe_name = fe_name
    ) %>%
    filter(!is.na(tenure_bin))
}


# =========================
# 4) Load data from Arrow
# =========================
ts_msg("Opening Arrow dataset.")
ds <- open_dataset(INPUT, format = "parquet")
ds_names <- names(ds)

METRO_VAR     <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro")
GENDER_VAR    <- pick_first_existing(GENDER_CANDIDATES, ds_names, what = "gender")
ETHNICITY_VAR <- pick_first_existing(ETHNICITY_CANDIDATES, ds_names, what = "ethnicity")
ts_msg("Using metro variable:", METRO_VAR)
ts_msg("Using gender variable:", GENDER_VAR)
ts_msg("Using ethnicity variable:", ETHNICITY_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents",
  "first_country", "last_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_enddate_edu", "edu_last_end_year",
  "edu_first_university_location",
  "last_university_country",
  PARENT_VAR, IMMIG_VAR, METRO_VAR, GENDER_VAR, ETHNICITY_VAR
))

cols_pull <- unique(c(
  "user_id", "year", "n_patents",
  "first_country", "last_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_enddate_edu", "edu_last_end_year",
  "edu_first_university_location",
  "last_university_country",
  PARENT_VAR, IMMIG_VAR, METRO_VAR, GENDER_VAR, ETHNICITY_VAR
))

ts_msg("Computing first_pos_country from full panel:", CHAR_FULL_START, "to", CHAR_FULL_END)
ds_pos <- ds %>%
  filter(year >= CHAR_FULL_START, year <= CHAR_FULL_END) %>%
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

ts_msg("Collecting main data:", CHAR_FULL_START, "to", CHAR_FULL_END)
df_full <- ds %>%
  filter(year >= CHAR_FULL_START, year <= CHAR_FULL_END) %>%
  select(all_of(cols_pull)) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    last_country  = str_trim(as.character(last_country)),
    first_startdate_edu = as.character(first_startdate_edu),
    first_startdate_pos = as.character(first_startdate_pos),
    first_enddate_edu   = as.character(first_enddate_edu),
    edu_last_end_year   = suppressWarnings(as.numeric(edu_last_end_year)),
    edu_first_university_location = str_trim(as.character(edu_first_university_location)),
    last_university_country = str_trim(as.character(last_university_country)),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!IMMIG_VAR  := as.integer(.data[[IMMIG_VAR]]),
    !!METRO_VAR     := str_trim(as.character(.data[[METRO_VAR]])),
    !!GENDER_VAR    := str_trim(as.character(.data[[GENDER_VAR]])),
    !!ETHNICITY_VAR := str_trim(as.character(.data[[ETHNICITY_VAR]]))
  ) %>%
  left_join(first_pos_tbl, by = "user_id") %>%
  compute_tenure() %>%
  make_reference_year() %>%
  mutate(
    gender_std = case_when(
      .data[[GENDER_VAR]] %in% c("Male", "Female") ~ .data[[GENDER_VAR]],
      TRUE ~ NA_character_
    ),
    ethnicity_std = standardize_ethnicity(.data[[ETHNICITY_VAR]]),
    immigrant_status = case_when(
      .data[[IMMIG_VAR]] == 1L ~ "Immigrant",
      .data[[IMMIG_VAR]] == 0L ~ "Native",
      TRUE ~ NA_character_
    ),
    parent_fe_id = str_trim(as.character(.data[[PARENT_VAR]])),
    metro_fe_id  = str_trim(as.character(.data[[METRO_VAR]])),
    mp_fe_id = case_when(
      !is.na(parent_fe_id) & parent_fe_id != "" &
        !is.na(metro_fe_id) & metro_fe_id != "" ~ paste0(parent_fe_id, "__", metro_fe_id),
      TRUE ~ NA_character_
    ),
    pmy_fe_id = case_when(
      !is.na(mp_fe_id) & mp_fe_id != "" & !is.na(year) ~ paste0(mp_fe_id, "__", year),
      TRUE ~ NA_character_
    )
  )

rm(ds, ds_pos, ds_first_year)
gc()

ts_msg("Full panel rows loaded before last education in US restriction:", nrow(df_full))

df_full <- df_full %>%
  filter(
    !is.na(last_university_country),
    last_university_country == US_COUNTRY
  )

ts_msg("Rows after restricting to last education in US:", nrow(df_full))

write_csv(
  tibble(
    metric = c("n_rows_full", "n_users_full"),
    value  = c(nrow(df_full), n_distinct(df_full$user_id))
  ),
  file.path(dirs$diagnostics, "load_diagnostics.csv")
)

# =========================
# 5) Build inventor-level heterogeneity splits
# =========================
ts_msg("Building inventor-level split variables.")

# inventor-level reference info
first_nonmissing_chr <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  as.character(x[1])
}
first_nonmissing_num <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  as.numeric(x[1])
}

inventor_ref <- df_full %>%
  group_by(user_id) %>%
  summarise(
    immigrant_status = first_nonmissing_chr(immigrant_status),
    gender_std       = first_nonmissing_chr(gender_std),
    ethnicity_std    = first_nonmissing_chr(ethnicity_std),
    school_location  = first_nonmissing_chr(edu_first_university_location),
    ref_year         = first_nonmissing_num(ref_year),
    .groups = "drop"
  )

# school location size among natives only
native_school_loc <- inventor_ref %>%
  filter(immigrant_status == "Native", !is.na(school_location), school_location != "") %>%
  count(school_location, name = "school_loc_size")

inventor_ref <- inventor_ref %>%
  left_join(native_school_loc, by = "school_location") %>%
  mutate(
    school_loc_size_split_native = case_when(
      immigrant_status != "Native" ~ NA_character_,
      is.na(school_loc_size) ~ NA_character_,
      TRUE ~ make_quantile_split(school_loc_size, p = TOPBOT_P)
    )
  )

# early-career patent totals using full panel
panel_end <- max(df_full$year, na.rm = TRUE)

inventor_year_pat <- df_full %>%
  filter(!is.na(ref_year), !is.na(n_patents), !is.na(year)) %>%
  mutate(
    within_5y  = year >= ref_year & year <= ref_year + 4,
    within_10y = year >= ref_year & year <= ref_year + 9
  ) %>%
  group_by(user_id) %>%
  summarise(
    ref_year = first_nonmissing_num(ref_year),
    patents_5y = sum(n_patents[within_5y], na.rm = TRUE),
    patents_10y = sum(n_patents[within_10y], na.rm = TRUE),
    complete_5y  = first(ref_year) <= (panel_end - 4),
    complete_10y = first(ref_year) <= (panel_end - 9),
    .groups = "drop"
  )

inventor_ref <- inventor_ref %>%
  left_join(inventor_year_pat, by = "user_id", suffix = c("", "_y")) %>%
  mutate(
    patents5_split = case_when(
      is.na(complete_5y) | !complete_5y ~ NA_character_,
      TRUE ~ make_quantile_split(patents_5y, p = TOPBOT_P)
    ),
    patents10_split = case_when(
      is.na(complete_10y) | !complete_10y ~ NA_character_,
      TRUE ~ make_quantile_split(patents_10y, p = TOPBOT_P)
    )
  )

write_csv(inventor_ref, file.path(dirs$diagnostics, "inventor_level_split_variables.csv"))

available_ethnicity_levels <- inventor_ref %>%
  filter(!is.na(ethnicity_std), ethnicity_std != "") %>%
  count(ethnicity_std, name = "n_users", sort = TRUE) %>%
  mutate(pref_order = match(ethnicity_std, preferred_ethnicity_order())) %>%
  arrange(pref_order, desc(n_users), ethnicity_std) %>%
  pull(ethnicity_std)

ethnicity_reference <- choose_reference_group(inventor_ref$ethnicity_std, preferred = "White")

# =========================
# 6) Core regression runner for one sample window and one subsample
# =========================
fe_ladder <- list(
  list(spec_name = "baseline_user_year",       fe = "user_id + year_fe",          show_in_main_plot = TRUE),
  list(spec_name = "plus_parent_fe",           fe = "user_id + year_fe + parent_fe", show_in_main_plot = TRUE),
  list(spec_name = "plus_metro_fe",            fe = "user_id + year_fe + metro_fe",  show_in_main_plot = TRUE),
  list(spec_name = "plus_mp_fe",               fe = "user_id + year_fe + mp_fe",     show_in_main_plot = TRUE),
  list(spec_name = "plus_pmy_fe",              fe = "user_id + pmy_fe",               show_in_main_plot = FALSE)
)

run_ladder_for_split <- function(df_window, split_name, subset_expr = NULL, window_tag) {

  dd0 <- df_window

  split_cols_needed <- c(
    "gender_std",
    "immigrant_status",
    "ethnicity_std",
    "school_loc_size_split_native",
    "patents5_split",
    "patents10_split"
  )

  missing_split_cols <- setdiff(split_cols_needed, names(dd0))

  if (length(missing_split_cols) > 0) {
    dd0 <- dd0 %>%
      left_join(
        inventor_ref %>% select(user_id, all_of(missing_split_cols)),
        by = "user_id"
      )
  }

  if (!is.null(subset_expr)) {
    dd0 <- dd0 %>% filter(!!rlang::parse_expr(subset_expr))
  }

  dd0 <- dd0 %>%
    filter(
      !is.na(first_country), first_country == US_COUNTRY,
      !is.na(user_id), user_id != "",
      !is.na(year),
      !is.na(n_patents), n_patents >= 0,
      is.finite(tenure), is.finite(tenure_sq),
      !is.na(parent_fe_id), parent_fe_id != "",
      !is.na(metro_fe_id), metro_fe_id != "",
      !is.na(mp_fe_id), mp_fe_id != "",
      !is.na(pmy_fe_id), pmy_fe_id != ""
    ) %>%
    mutate(
      tenure_bin = make_tenure_bins(tenure),
      user_id    = as.factor(user_id),
      year_fe    = as.factor(year),
      parent_fe  = as.factor(parent_fe_id),
      metro_fe   = as.factor(metro_fe_id),
      mp_fe      = as.factor(mp_fe_id),
      pmy_fe     = as.factor(pmy_fe_id),
      vcov_cluster = as.factor(mp_fe_id)
    ) %>%
    filter(!is.na(tenure_bin))

  if (nrow(dd0) == 0) {
    ts_msg("Skipping empty split:", split_name, "|", window_tag)
    return(list(
      data = dd0,
      coef_table = tibble(),
      means_0_5 = tibble(),
      mp_fixef = tibble(),
      fe_decomp_table = tibble()
    ))
  }

  out_list <- list()
  k <- 0L

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]

    for (sp in fe_ladder) {
      ts_msg("Running:", window_tag, "|", split_name, "|", sp$spec_name, "|", wtag, "| N =", nrow(dd0))

      fit <- run_ppml(
        rhs = paste0("i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\")"),
        fe_part = sp$fe,
        data = dd0,
        wform = wform,
        cluster_var = "vcov_cluster"
      )

      k <- k + 1L
      out_list[[k]] <- extract_fixest_terms(
        fit,
        paste0(window_tag, "__", split_name, "__", sp$spec_name, "__", wtag)
      ) %>%
        mutate(
          split      = split_name,
          window     = window_tag,
          spec_name  = sp$spec_name,
          weight     = wtag,
          n_obs      = nrow(dd0),
          n_users    = dplyr::n_distinct(dd0$user_id),
          n_clusters = dplyr::n_distinct(dd0$vcov_cluster)
        )
    }
  }

  coef_table <- bind_rows(out_list)

  means_0_5 <- mean_patents_0_5(dd0, split_name = split_name, window_tag = window_tag)

  # FE decomposition:
  # 1) Estimate PPML with user + year + mp FE, no tenure controls.
  # 2) Regress estimated mp FE values on tenure bins via OLS.
  ts_msg("Running FE decomposition:", window_tag, "|", split_name)
  m_fe_only <- run_ppml(
    rhs = "1",
    fe_part = "user_id + year_fe + mp_fe",
    data = dd0,
    wform = NULL,
    cluster_var = "vcov_cluster"
  )

  mp_fixef <- tibble()
  fe_decomp_table <- tibble()

  if (!is.null(m_fe_only)) {
    fe_mp <- tryCatch(fixest::fixef(m_fe_only)[["mp_fe"]], error = function(e) NULL)

    if (!is.null(fe_mp)) {
      mp_fixef <- tibble(
        mp_fe_id = names(fe_mp),
        mp_fe_value = as.numeric(fe_mp),
        split = split_name,
        window = window_tag
      )

      write_csv(
        mp_fixef,
        file.path(dirs$fixef, paste0(window_tag, "__", safe_name(split_name), "__mp_fixef.csv"))
      )

      dd_fe <- dd0 %>%
        left_join(mp_fixef %>% select(mp_fe_id, mp_fe_value), by = "mp_fe_id") %>%
        filter(is.finite(mp_fe_value))

      m_fe_decomp <- run_ols(
        lhs = "mp_fe_value",
        rhs = paste0("i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\")"),
        fe_part = "user_id + year_fe",
        data = dd_fe,
        cluster_var = "vcov_cluster"
      )

      fe_decomp_table <- extract_fixest_terms(
        m_fe_decomp,
        paste0(window_tag, "__", split_name, "__mp_fe_decomp_ols")
      ) %>%
        mutate(
          split = split_name,
          window = window_tag,
          spec_name = "mp_fe_decomp_ols",
          n_obs = nrow(dd_fe),
          n_users = dplyr::n_distinct(dd_fe$user_id)
        )
    }
  }

  list(
    data = dd0,
    coef_table = coef_table,
    means_0_5 = means_0_5,
    mp_fixef = mp_fixef,
    fe_decomp_table = fe_decomp_table
  )
}

# =========================
# 7) Plotting helpers
# =========================
plot_ladder <- function(coef_table, means_0_5_tbl, window_tag, split_name, include_pmy = FALSE) {
  tabp <- coef_table %>%
    parse_tenure_terms() %>%
    filter(weight == "unweighted")

  if (!include_pmy) {
    tabp <- tabp %>% filter(spec_name != "plus_pmy_fe")
  }

  if (nrow(tabp) == 0) return(NULL)

  mean_note <- means_0_5_tbl %>%
    filter(window == window_tag, split == split_name) %>%
    mutate(txt = paste0("Mean patents, tenure 0-5: ", sprintf("%.3f", mean_patents_0_5), " (N=", n_obs_0_5, ")")) %>%
    pull(txt)

  p <- ggplot(tabp, aes(x = tenure_bin, y = estimate, color = spec_name, group = spec_name)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray50") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, alpha = 0.7, position = position_dodge(width = 0.35)) +
    geom_point(position = position_dodge(width = 0.35), size = 2.2) +
    geom_line(position = position_dodge(width = 0.35), linewidth = 0.6) +
    geom_text(
      aes(label = p_label),
      position = position_dodge(width = 0.35),
      vjust = -0.7,
      size = 2.5,
      show.legend = FALSE
    ) +
    labs(
      title = paste0("PPML tenure-bin coefficients — ", split_name, " — ", window_tag),
      subtitle = ifelse(length(mean_note) > 0, mean_note[1], NULL),
      x = "Tenure bin",
      y = "Coefficient (relative to tenure 0-5)",
      color = "Specification",
      caption = "Unweighted PPML. Standard errors clustered by metro-parent cell. Labels show coefficient p-values."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom"
    )

  p
}

plot_mp_fe_decomp <- function(fe_decomp_table, means_0_5_tbl, window_tag, split_name) {
  tabp <- fe_decomp_table %>%
    parse_tenure_terms()

  if (nrow(tabp) == 0) return(NULL)

  mean_note <- means_0_5_tbl %>%
    filter(window == window_tag, split == split_name) %>%
    mutate(txt = paste0("Mean patents, tenure 0-5: ", sprintf("%.3f", mean_patents_0_5), " (N=", n_obs_0_5, ")")) %>%
    pull(txt)

  ggplot(tabp, aes(x = tenure_bin, y = estimate, group = 1)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray50") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.15, alpha = 0.8) +
    geom_point(size = 2.2) +
    geom_line(linewidth = 0.6) +
    geom_text(aes(label = p_label), vjust = -0.7, size = 2.5) +
    labs(
      title = paste0("OLS on estimated metro-parent FE — ", split_name, " — ", window_tag),
      subtitle = ifelse(length(mean_note) > 0, mean_note[1], NULL),
      x = "Tenure bin",
      y = "OLS coefficient on estimated metro-parent FE",
      caption = "First stage: PPML with user + year + metro-parent FE, no tenure controls. Second stage: OLS of estimated metro-parent FE on tenure bins with user + year FE."
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
}

# =========================
# 8) Run all requested exercises
# =========================
all_coef_tables      <- list()
all_means_0_5        <- list()
all_fe_decomp_tables <- list()
all_group_diff       <- list()

interaction_fe_specs <- list(
  list(key = "baseline", fe = "user_id + year_fe",      label = "baseline_user_year"),
  list(key = "parent",   fe = "user_id + year_fe + parent_fe", label = "plus_parent_fe"),
  list(key = "metro",    fe = "user_id + year_fe + metro_fe",  label = "plus_metro_fe"),
  list(key = "mp",       fe = "user_id + year_fe + mp_fe",     label = "plus_mp_fe"),
  list(key = "pmy",      fe = "user_id + pmy_fe",              label = "plus_pmy_fe")
)

for (window_tag in names(WINDOWS)) {
  yr <- WINDOWS[[window_tag]]
  y0 <- yr[1]
  y1 <- yr[2]

  ts_msg("============================================================")
  ts_msg("WINDOW:", window_tag, "(", y0, "-", y1, ")")
  ts_msg("============================================================")

  df_window <- df_full %>%
    filter(year >= y0, year <= y1) %>%
    left_join(
      inventor_ref %>%
        select(user_id, immigrant_status, gender_std, ethnicity_std,
               school_loc_size, school_loc_size_split_native,
               patents_5y, patents_10y, patents5_split, patents10_split),
      by = "user_id"
    )

  ethnicity_split_defs <- purrr::map(
    available_ethnicity_levels,
    ~ list(
      name = paste0("ETHNICITY_", safe_name(.x)),
      expr = paste0("ethnicity_std == '", .x, "'")
    )
  )

  split_defs <- c(
    list(
      list(name = "ALL", expr = NULL),
      list(name = "MALE", expr = "gender_std == 'Male'"),
      list(name = "FEMALE", expr = "gender_std == 'Female'"),
      list(name = "NATIVE", expr = "immigrant_status == 'Native'"),
      list(name = "IMMIGRANT", expr = "immigrant_status == 'Immigrant'")
    ),
    ethnicity_split_defs,
    list(
      list(name = "NATIVE_SCHOOLLOC_BOTTOM20", expr = "immigrant_status == 'Native' & school_loc_size_split_native == 'bottom20'"),
      list(name = "NATIVE_SCHOOLLOC_TOP20",    expr = "immigrant_status == 'Native' & school_loc_size_split_native == 'top20'"),
      list(name = "PATENTS5_BOTTOM20", expr = "patents5_split == 'bottom20'"),
      list(name = "PATENTS5_TOP20",    expr = "patents5_split == 'top20'"),
      list(name = "PATENTS10_BOTTOM20", expr = "patents10_split == 'bottom20'"),
      list(name = "PATENTS10_TOP20",    expr = "patents10_split == 'top20'")
    )
  )

  split_results <- vector("list", length(split_defs))
  names(split_results) <- purrr::map_chr(split_defs, "name")

  for (i in seq_along(split_defs)) {
    sp <- split_defs[[i]]
    res <- run_ladder_for_split(df_window, split_name = sp$name, subset_expr = sp$expr, window_tag = window_tag)
    split_results[[i]] <- res

    all_coef_tables[[paste0(window_tag, "__", sp$name)]]      <- res$coef_table
    all_means_0_5[[paste0(window_tag, "__", sp$name)]]        <- res$means_0_5
    all_fe_decomp_tables[[paste0(window_tag, "__", sp$name)]] <- res$fe_decomp_table

    if (nrow(res$coef_table) > 0) {
      p_main <- plot_ladder(res$coef_table, res$means_0_5, window_tag, sp$name, include_pmy = FALSE)
      p_all  <- plot_ladder(res$coef_table, res$means_0_5, window_tag, sp$name, include_pmy = TRUE)
      p_fe   <- plot_mp_fe_decomp(res$fe_decomp_table, res$means_0_5, window_tag, sp$name)

      if (!is.null(p_main)) save_plot(p_main, file.path(dirs$plots, paste0(window_tag, "__", safe_name(sp$name), "__tenure_ladder_main.png")))
      if (!is.null(p_all))  save_plot(p_all,  file.path(dirs$plots, paste0(window_tag, "__", safe_name(sp$name), "__tenure_ladder_with_pmy.png")))
      if (!is.null(p_fe))   save_plot(p_fe,   file.path(dirs$plots, paste0(window_tag, "__", safe_name(sp$name), "__mp_fe_decomp_ols.png")))
    }
  }

  # pooled cross-group difference models for exact significance
  base_df <- split_results[["ALL"]]$data
  if (nrow(base_df) > 0) {
    for (fei in interaction_fe_specs) {
      all_group_diff[[paste0(window_tag, "__gender__", fei$key)]] <- run_group_interaction_ppml(
        data = base_df,
        group_var = "gender_std",
        group_ref = "Male",
        group_alt = "Female",
        fe_part = fei$fe,
        label_stub = paste0(window_tag, "__gender_diff__", fei$label),
        fe_name = fei$label
      )
    }

    for (fei in interaction_fe_specs) {
      all_group_diff[[paste0(window_tag, "__imm__", fei$key)]] <- run_group_interaction_ppml(
        data = base_df,
        group_var = "immigrant_status",
        group_ref = "Native",
        group_alt = "Immigrant",
        fe_part = fei$fe,
        label_stub = paste0(window_tag, "__immigrant_diff__", fei$label),
        fe_name = fei$label
      )
    }

    eth_ref_here <- choose_reference_group(base_df$ethnicity_std, preferred = ethnicity_reference)
    eth_alts_here <- setdiff(unique(stats::na.omit(base_df$ethnicity_std)), eth_ref_here)

    if (length(eth_alts_here) > 0) {
      for (eth_alt in eth_alts_here) {
        for (fei in interaction_fe_specs) {
          key_name <- paste0(window_tag, "__ethnicity__", safe_name(eth_ref_here), "__vs__", safe_name(eth_alt), "__", fei$key)
          all_group_diff[[key_name]] <- run_group_interaction_ppml(
            data = base_df,
            group_var = "ethnicity_std",
            group_ref = eth_ref_here,
            group_alt = eth_alt,
            fe_part = fei$fe,
            label_stub = paste0(window_tag, "__ethnicity_", safe_name(eth_ref_here), "_vs_", safe_name(eth_alt), "__", fei$label),
            fe_name = fei$label
          )
        }
      }
    }
  }
}

coef_all      <- bind_rows(all_coef_tables)
means_0_5_all <- bind_rows(all_means_0_5)
fe_decomp_all <- bind_rows(all_fe_decomp_tables)
group_diff_all <- bind_rows(all_group_diff)

# =========================
# 9) Save outputs
# =========================
write_csv(coef_all,      file.path(dirs$tables, "ALL__ppml_tenure_ladder_extended__all_terms.csv"))
write_csv(means_0_5_all, file.path(dirs$tables, "ALL__mean_patents_0_5.csv"))
write_csv(fe_decomp_all, file.path(dirs$tables, "ALL__mp_fe_decomposition_ols__all_terms.csv"))
write_csv(group_diff_all, file.path(dirs$tables, "ALL__group_difference_interactions.csv"))

# focus tables: tenure-bin only
coef_focus <- coef_all %>% parse_tenure_terms()
fe_focus   <- fe_decomp_all %>% parse_tenure_terms()

write_csv(coef_focus, file.path(dirs$tables, "ALL__ppml_tenure_ladder_extended__tenure_bin_focus.csv"))
write_csv(fe_focus,   file.path(dirs$tables, "ALL__mp_fe_decomposition_ols__tenure_bin_focus.csv"))

# "What fixed effects kill differences?" summaries
group_diff_summary_by_contrast <- group_diff_all %>%
  group_by(group_var, group_ref, group_alt, fe_name) %>%
  summarise(
    n_bins_p_lt_0_10 = sum(is.finite(p) & p < 0.10, na.rm = TRUE),
    n_bins_p_lt_0_05 = sum(is.finite(p) & p < 0.05, na.rm = TRUE),
    avg_abs_diff     = mean(abs(estimate), na.rm = TRUE),
    .groups = "drop"
  )

group_diff_summary_collapsed <- group_diff_all %>%
  group_by(group_var, fe_name) %>%
  summarise(
    n_bins_p_lt_0_10 = sum(is.finite(p) & p < 0.10, na.rm = TRUE),
    n_bins_p_lt_0_05 = sum(is.finite(p) & p < 0.05, na.rm = TRUE),
    avg_abs_diff     = mean(abs(estimate), na.rm = TRUE),
    .groups = "drop"
  )

write_csv(group_diff_summary_by_contrast, file.path(dirs$tables, "ALL__group_difference_summary_by_contrast.csv"))
write_csv(group_diff_summary_collapsed,   file.path(dirs$tables, "ALL__group_difference_summary.csv"))

# run manifest
write_csv(
  tibble(
    item = c(
      "weighting",
      "gender_var",
      "ethnicity_var",
      "immigrant_var",
      "school_location_var",
      "cluster_for_vcov",
      "ethnicity_reference",
      "notes"
    ),
    value = c(
      "unweighted",
      GENDER_VAR,
      ETHNICITY_VAR,
      IMMIG_VAR,
      "edu_first_university_location",
      "metro-parent",
      ethnicity_reference,
      "No time-invariant size control included in PPML ladder."
    )
  ),
  file.path(dirs$diagnostics, "run_manifest.csv")
)

ts_msg("DONE. Outputs in:", OUT_DIR)
