#!/usr/bin/env Rscript

##############################################################################
# Separate-group PPML top-vs-complement lifecycle profiles
#
# What this script does:
#   1) Loads inventor-year data from the merged Revelio/LinkedIn patent panel.
#   2) Constructs tenure, tenure bins, reference graduation/start year, parent,
#      metro, metro-parent, and metro-parent-year identifiers.
#   3) Builds the same top-vs-complement splits used in the top-complement
#      analysis:
#        - CAREER_TOPCOMP: top 20% versus complement by average career patents
#        - FIRSTMETRO_SIZE_TOPCOMP: top 20% versus complement by first-metro size
#        - FIRSTMETRO_PROD_TOPCOMP: top 20% versus complement by first-metro
#          patent productivity
#   4) Runs lifecycle PPML tenure-bin ladders separately within each group:
#        - top20 sample only
#        - complement sample only
#      The top20 and complement profiles are therefore estimated from separate
#      PPML regressions, not from one pooled top/complement model.
#   5) Runs the top20-minus-complement interaction/difference profile using the
#      old interaction design:
#        n_patents ~ i(tenure_bin, group2, ref = "0-5", ref2 = "complement") | FE
#      This interaction regression is used only for the difference profile.
#   6) Estimates all FE families already used in the top-complement script:
#        - with_user_fe: user/year-based FE ladder
#        - no_user_fe: no-user-FE ladder
#   7) Runs all windows, samples, cohort restrictions, FE decompositions, tables,
#      diagnostics, and plots already defined in the top-complement workflow.
#   8) Prints detailed progress, sample counts, group counts, model formulas,
#      and output locations to the Slurm .out log.
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
  "tidyr", "tibble", "ggplot2", "purrr", "forcats", "rlang"
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
  library(rlang)
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
OUT_DIR <- "/home/epiga/revelio_labs/output/ppml_tenure_ladder_rewrite_minimal_topcomp_separate"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

PARENT_VAR <- "first_parent_rcid"
US_COUNTRY <- "United States"
FIRST_CITY_VAR <- "first_city"

WINDOWS <- list(
  `2010_2019` = c(2010L, 2019L),
  `2000_2019` = c(2000L, 2019L)
)

CHAR_FULL_START <- 2000L
CHAR_FULL_END   <- 2024L
TENURE_MAX      <- 50L
TENURE_BIN_STEP <- 5L
TOP_P           <- 0.20
CITY_BASE_YEAR  <- 2000L

COHORTS <- list(
  `grad_1980_1985` = c(1980L, 1985L),
  `grad_1985_1990` = c(1985L, 1990L),
  `grad_1990_1995` = c(1990L, 1995L)
)

METRO_CANDIDATES <- c(
  "first_metro", "metro", "metro_name",
  "first_metro_area", "msa", "msa_name",
  "first_cbsa", "cbsa", "cbsa_name"
)
GENDER_CANDIDATES <- c("au_sex_predicted", "sex_predicted")

WEIGHTS <- list(unweighted = NULL)

# Chunked execution controls
CHUNK_MODE <- Sys.getenv("CHUNK_MODE", unset = "all")
WINDOW_TAGS_ENV <- Sys.getenv("WINDOW_TAGS", unset = paste(names(WINDOWS), collapse = ","))
RUN_NO_USER_FE <- identical(Sys.getenv("RUN_NO_USER_FE", unset = "1"), "1")
RUN_COHORTS <- identical(Sys.getenv("RUN_COHORTS", unset = "1"), "1")
RUN_FE_DECOMP <- identical(Sys.getenv("RUN_FE_DECOMP", unset = "1"), "1")

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
  if (length(hit) == 0) stop(paste0("[ERROR] Missing ", what, ". Tried: ", paste(candidates, collapse = ", ")))
  hit[1]
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

parse_windows_env <- function(x) {
  out <- unlist(strsplit(x, ",", fixed = TRUE), use.names = FALSE)
  out <- trimws(out)
  out[out != ""]
}

make_dirs <- function() {
  dirs <- list(
    root              = OUT_DIR,
    tables            = file.path(OUT_DIR, "tables"),
    plots             = file.path(OUT_DIR, "plots"),
    diagnostics       = file.path(OUT_DIR, "diagnostics"),
    fixef             = file.path(OUT_DIR, "fixef"),
    chunk_tables      = file.path(OUT_DIR, "chunk_outputs", "tables"),
    chunk_diagnostics = file.path(OUT_DIR, "chunk_outputs", "diagnostics")
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

save_plot <- function(p, path, width = 11, height = 7) {
  ts_msg("Saving plot:", path)
  ggplot2::ggsave(path, p, width = width, height = height, dpi = 300)
}

make_top_complement_split <- function(x, p = TOP_P) {
  q_hi <- suppressWarnings(as.numeric(stats::quantile(x, probs = 1 - p, na.rm = TRUE, type = 7)))
  dplyr::case_when(
    !is.finite(x) ~ NA_character_,
    x >= q_hi     ~ "top20",
    TRUE          ~ "complement"
  )
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
  ts_msg("Running PPML formula:", deparse(fml))
  ts_msg("  Rows:", nrow(data),
         "| users:", dplyr::n_distinct(data$user_id),
         "| clusters:", dplyr::n_distinct(data[[cluster_var]]),
         "| positive outcome rows:", sum(data$n_patents > 0, na.rm = TRUE))
  tryCatch({
    fixest::fepois(fml, data = data, vcov = vcov_fml, weights = wform, notes = FALSE, warn = FALSE)
  }, error = function(e) {
    ts_msg("MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

run_ols <- function(lhs, rhs, fe_part, data, wform = NULL, cluster_var = "vcov_cluster") {
  fml <- as.formula(paste0(lhs, " ~ ", rhs, " | ", fe_part))
  vcov_fml <- stats::as.formula(paste0("~", cluster_var))
  ts_msg("Running OLS formula:", deparse(fml))
  ts_msg("  Rows:", nrow(data),
         "| users:", dplyr::n_distinct(data$user_id),
         "| clusters:", dplyr::n_distinct(data[[cluster_var]]))
  tryCatch({
    fixest::feols(fml, data = data, vcov = vcov_fml, weights = wform, notes = FALSE, warn = FALSE)
  }, error = function(e) {
    ts_msg("OLS MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

extract_tenure_bin_from_term <- function(term) {
  out <- dplyr::case_when(
    grepl("tenure_bin::", term) ~ sub("^.*tenure_bin::([^:]+).*$", "\\1", term),
    grepl("tenure_bin = ", term) ~ sub("^.*tenure_bin = ([^:]+).*$", "\\1", term),
    TRUE ~ sub("^tenure_bin", "", term)
  )
  out <- gsub("^\\s+|\\s+$", "", out)
  out <- gsub("`", "", out)
  out
}

parse_tenure_terms <- function(tab) {
  levs <- ordered_tenure_levels()
  tab %>%
    filter(grepl("tenure_bin", term)) %>%
    mutate(
      raw_term = term,
      tenure_bin_chr = extract_tenure_bin_from_term(term),
      tenure_bin = factor(tenure_bin_chr, levels = levs, ordered = TRUE),
      conf_low  = estimate - 1.96 * se,
      conf_high = estimate + 1.96 * se,
      p_label   = ifelse(is.finite(p), sprintf("p=%.3f", p), NA_character_)
    ) %>%
    filter(!is.na(tenure_bin)) %>%
    select(-tenure_bin_chr)
}

prepare_analysis_sample <- function(dd0) {
  dd0 %>%
    filter(
      !is.na(first_country), first_country == US_COUNTRY,
      !is.na(user_id), user_id != "",
      !is.na(year),
      !is.na(n_patents), n_patents >= 0,
      is.finite(tenure),
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
}

add_zero_bin_to_profile_terms <- function(tab) {
  if (nrow(tab) == 0) return(tab)
  levs <- ordered_tenure_levels()
  id_cols <- c(
    "analysis", "window", "sample_tag", "cohort_tag", "user_fe_tag",
    "profile", "group_value", "spec_name", "weight", "model",
    "n_obs", "n_users", "n_clusters", "n_positive", "fe_part"
  )
  zero_row <- tab %>%
    distinct(across(any_of(id_cols))) %>%
    mutate(
      term = paste0("__REF_0_", TENURE_BIN_STEP, "__"),
      raw_term = term,
      tenure_bin = factor(paste0("0-", TENURE_BIN_STEP), levels = levs, ordered = TRUE),
      estimate = 0,
      se = 0,
      stat = NA_real_,
      p = NA_real_,
      stars = "",
      collinear_dropped = "",
      conf_low = 0,
      conf_high = 0,
      p_label = NA_character_
    )
  bind_rows(tab, zero_row) %>% arrange(spec_name, profile, tenure_bin)
}

# =========================
# 4) Load data
# =========================
ts_msg("Opening Arrow dataset.")
ds <- open_dataset(INPUT, format = "parquet")
ds_names <- names(ds)

METRO_VAR  <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro")
GENDER_VAR <- pick_first_existing(GENDER_CANDIDATES, ds_names, what = "gender")
ts_msg("Using metro variable:", METRO_VAR)
ts_msg("Using gender variable:", GENDER_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents",
  "first_country", "last_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_enddate_edu", "edu_last_end_year",
  "last_university_country", FIRST_CITY_VAR,
  PARENT_VAR, METRO_VAR, GENDER_VAR
))

cols_pull <- unique(c(
  "user_id", "year", "n_patents",
  "first_country", "last_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_enddate_edu", "edu_last_end_year",
  "last_university_country", FIRST_CITY_VAR,
  PARENT_VAR, METRO_VAR, GENDER_VAR
))

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
    last_university_country = str_trim(as.character(last_university_country)),
    !!FIRST_CITY_VAR := str_trim(as.character(.data[[FIRST_CITY_VAR]])),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!METRO_VAR  := str_trim(as.character(.data[[METRO_VAR]])),
    !!GENDER_VAR := str_trim(as.character(.data[[GENDER_VAR]]))
  ) %>%
  mutate(
    country_year = dplyr::coalesce(
      na_if(last_country, ""),
      na_if(first_country, "")
    )
  ) %>%
  filter(
    !is.na(country_year),
    country_year == US_COUNTRY
  ) %>%
  compute_tenure() %>%
  make_reference_year() %>%
  mutate(
    gender_std = case_when(.data[[GENDER_VAR]] %in% c("Male", "Female") ~ .data[[GENDER_VAR]], TRUE ~ NA_character_),
    parent_fe_id = str_trim(as.character(.data[[PARENT_VAR]])),
    metro_fe_id  = str_trim(as.character(.data[[METRO_VAR]])),
    mp_fe_id = case_when(
      !is.na(parent_fe_id) & parent_fe_id != "" & !is.na(metro_fe_id) & metro_fe_id != "" ~ paste0(parent_fe_id, "__", metro_fe_id),
      TRUE ~ NA_character_
    ),
    pmy_fe_id = case_when(
      !is.na(mp_fe_id) & mp_fe_id != "" & !is.na(year) ~ paste0(mp_fe_id, "__", year),
      TRUE ~ NA_character_
    )
  ) %>%
  filter(!is.na(last_university_country), last_university_country == US_COUNTRY)

rm(ds)
gc()
ts_msg("Loaded rows in df_full:", nrow(df_full))
ts_msg("Loaded users in df_full:", dplyr::n_distinct(df_full$user_id))

# =========================
# 5) Inventor-level objects
# =========================
first_nonmissing_chr <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  as.character(x[1])
}

last_nonmissing_chr <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_character_)
  as.character(x[length(x)])
}

first_nonmissing_num <- function(x) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(NA_real_)
  as.numeric(x[1])
}

inventor_ref <- df_full %>%
  arrange(user_id, year) %>%
  group_by(user_id) %>%
  summarise(
    gender_std      = first_nonmissing_chr(gender_std),
    ref_year        = first_nonmissing_num(ref_year),
    first_metro     = first_nonmissing_chr(metro_fe_id),
    last_metro      = last_nonmissing_chr(metro_fe_id),
    n_metros_obs    = n_distinct(metro_fe_id[!is.na(metro_fe_id) & metro_fe_id != ""]),
    avg_pat_career  = mean(n_patents[is.finite(tenure)], na.rm = TRUE),
    career_obs_all  = sum(is.finite(tenure) & !is.na(n_patents)),
    .groups = "drop"
  ) %>%
  mutate(
    avg_pat_career = ifelse(is.nan(avg_pat_career), NA_real_, avg_pat_career),
    same_start_end_metro = !is.na(first_metro) & !is.na(last_metro) & first_metro == last_metro,
    never_moved_metro = is.finite(n_metros_obs) & n_metros_obs == 1L,
    career_topcomp = case_when(
      is.na(career_obs_all) | career_obs_all < 3 ~ NA_character_,
      TRUE ~ make_top_complement_split(avg_pat_career, p = TOP_P)
    ),
    grad_cohort = case_when(
      is.finite(ref_year) & ref_year >= 1980 & ref_year <= 1985 ~ "grad_1980_1985",
      is.finite(ref_year) & ref_year > 1985 & ref_year <= 1990 ~ "grad_1985_1990",
      is.finite(ref_year) & ref_year > 1990 & ref_year <= 1995 ~ "grad_1990_1995",
      TRUE ~ NA_character_
    )
  )

metro_stats_2000 <- df_full %>%
  filter(year == CITY_BASE_YEAR, !is.na(metro_fe_id), metro_fe_id != "") %>%
  group_by(metro_fe_id) %>%
  summarise(
    metro_size_2000 = n_distinct(user_id),
    metro_prod_2000 = mean(n_patents, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  mutate(
    metro_prod_2000 = ifelse(is.nan(metro_prod_2000), NA_real_, metro_prod_2000),
    metro_size_topcomp = make_top_complement_split(metro_size_2000, p = TOP_P),
    metro_prod_topcomp = make_top_complement_split(metro_prod_2000, p = TOP_P)
  )

inventor_ref <- inventor_ref %>%
  left_join(metro_stats_2000 %>% rename(first_metro = metro_fe_id), by = "first_metro")

write_csv(inventor_ref, file.path(dirs$diagnostics, "inventor_level_variables.csv"))
ts_msg("Saved inventor-level variables.")

career_cutoffs <- tibble(
  metric = c("career_q25", "career_q50", "career_q75", "career_p80", "career_p90"),
  value = c(
    as.numeric(stats::quantile(inventor_ref$avg_pat_career, probs = 0.25, na.rm = TRUE, type = 7)),
    as.numeric(stats::quantile(inventor_ref$avg_pat_career, probs = 0.50, na.rm = TRUE, type = 7)),
    as.numeric(stats::quantile(inventor_ref$avg_pat_career, probs = 0.75, na.rm = TRUE, type = 7)),
    as.numeric(stats::quantile(inventor_ref$avg_pat_career, probs = 0.80, na.rm = TRUE, type = 7)),
    as.numeric(stats::quantile(inventor_ref$avg_pat_career, probs = 0.90, na.rm = TRUE, type = 7))
  )
)
write_csv(career_cutoffs, file.path(dirs$diagnostics, "career_histogram_cutoffs.csv"))
ts_msg("Saved career patent cutoffs.")

hist_df <- inventor_ref %>%
  filter(is.finite(avg_pat_career), avg_pat_career >= 0, avg_pat_career <= 6)

if (nrow(hist_df) > 0) {
  p_hist <- ggplot(hist_df, aes(x = avg_pat_career)) +
    geom_histogram(bins = 150, fill = "grey80", color = "grey35", linewidth = 0.4) +
    geom_vline(data = career_cutoffs, aes(xintercept = value), linewidth = 0.7, linetype = "dashed", inherit.aes = FALSE) +
    coord_cartesian(xlim = c(0, 6)) +
    labs(
      title = "Distribution of average career patents per inventor",
      subtitle = "Cutoffs computed on full distribution; plot shown only for 0 to 6 patents",
      x = "Average patents per inventor-year over observed career",
      y = "Count"
    ) +
    theme_minimal(base_size = 12)
  save_plot(p_hist, file.path(dirs$plots, "career_avg_patents_histogram_with_cutoffs_zoom_0_6.png"))
}

# =========================
# 6) FE specs and decomposition
# =========================
run_fe_decomposition <- function(df_window, analysis_name, window_tag, sample_tag = "full_sample", cohort_tag = "all_cohorts") {
  ts_msg("Running FE decomposition:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| sample=", sample_tag,
         "| cohort=", cohort_tag)

  dd0 <- prepare_analysis_sample(df_window)
  ts_msg("FE-decomp prepared rows:", nrow(dd0))
  if (nrow(dd0) == 0) return(list(mp_fixef = tibble(), fe_decomp = tibble()))

  m_fe_only <- run_ppml(
    rhs = "1",
    fe_part = "user_id + year_fe + mp_fe",
    data = dd0,
    wform = NULL,
    cluster_var = "vcov_cluster"
  )
  if (is.null(m_fe_only)) return(list(mp_fixef = tibble(), fe_decomp = tibble()))

  fe_mp <- tryCatch(fixest::fixef(m_fe_only)[["mp_fe"]], error = function(e) NULL)
  if (is.null(fe_mp)) return(list(mp_fixef = tibble(), fe_decomp = tibble()))

  mp_fixef <- tibble(
    mp_fe_id = names(fe_mp),
    mp_fe_value = as.numeric(fe_mp),
    analysis = analysis_name,
    window = window_tag,
    sample_tag = sample_tag,
    cohort_tag = cohort_tag
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

  fe_decomp <- extract_fixest_terms(
    m_fe_decomp,
    paste0(window_tag, "__", analysis_name, "__", sample_tag, "__", cohort_tag, "__mp_fe_decomp_ols")
  ) %>%
    mutate(
      analysis = analysis_name,
      window = window_tag,
      sample_tag = sample_tag,
      cohort_tag = cohort_tag,
      spec_name = "mp_fe_decomp_ols",
      n_obs = nrow(dd_fe),
      n_users = dplyr::n_distinct(dd_fe$user_id)
    )

  list(mp_fixef = mp_fixef, fe_decomp = fe_decomp)
}

plot_mp_fe_decomp <- function(fe_decomp_table, window_tag, analysis_name, sample_tag = "full_sample", cohort_tag = "all_cohorts") {
  if (nrow(fe_decomp_table) == 0 || !("window" %in% names(fe_decomp_table))) return(NULL)
  tabp <- fe_decomp_table %>%
    filter(.data$window == .env$window_tag, .data$analysis == .env$analysis_name, .data$sample_tag == .env$sample_tag, .data$cohort_tag == .env$cohort_tag) %>%
    parse_tenure_terms()
  if (nrow(tabp) == 0) return(NULL)

  levs <- ordered_tenure_levels()
  zero_row <- tabp %>%
    distinct(window, analysis, sample_tag, cohort_tag, spec_name) %>%
    mutate(
      tenure_bin = factor(paste0("0-", TENURE_BIN_STEP), levels = levs, ordered = TRUE),
      estimate = 0,
      se = 0,
      conf_low = 0,
      conf_high = 0
    )
  tabp <- bind_rows(tabp, zero_row) %>% arrange(tenure_bin)

  ggplot(tabp, aes(x = tenure_bin, y = estimate, group = 1)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray50") +
    geom_line(linewidth = 0.65, color = "#2C3E50") +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, alpha = 0.8, color = "#2C3E50") +
    geom_point(size = 2.7, shape = 21, fill = "white", stroke = 0.8, color = "#2C3E50") +
    labs(
      title = paste0("OLS on estimated metro-parent FE — ", analysis_name, " — ", window_tag),
      subtitle = paste0("sample=", sample_tag, " | cohort=", cohort_tag),
      x = "Tenure bin",
      y = "OLS coefficient on estimated metro-parent FE",
      caption = "First stage: PPML with user + year + metro-parent FE, no tenure controls. Second stage: OLS of estimated metro-parent FE on tenure bins with user + year FE."
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1))
}

ALLOWED_SPECS_WITH_USER_FE <- c(
  "baseline_user_year",
  "plus_parent_fe",
  "plus_metro_fe",
  "plus_mp_fe",
  "plus_pmy_fe"
)

ALLOWED_SPECS_NO_USER_FE <- c(
  "no_user_year",
  "no_user_parent_fe",
  "no_user_metro_fe",
  "no_user_mp_fe",
  "no_user_pmy_fe"
)

fe_ladder_user <- list(
  list(spec_name = "baseline_user_year", fe = "user_id + year_fe"),
  list(spec_name = "plus_parent_fe",     fe = "user_id + year_fe + parent_fe"),
  list(spec_name = "plus_metro_fe",      fe = "user_id + year_fe + metro_fe"),
  list(spec_name = "plus_mp_fe",         fe = "user_id + year_fe + mp_fe"),
  list(spec_name = "plus_pmy_fe",        fe = "user_id + pmy_fe")
)

fe_ladder_no_user <- list(
  list(spec_name = "no_user_year",       fe = "year_fe"),
  list(spec_name = "no_user_parent_fe",  fe = "year_fe + parent_fe"),
  list(spec_name = "no_user_metro_fe",   fe = "year_fe + metro_fe"),
  list(spec_name = "no_user_mp_fe",      fe = "year_fe + mp_fe"),
  list(spec_name = "no_user_pmy_fe",     fe = "pmy_fe")
)

spec_names_from_ladder <- function(fe_ladder) {
  vapply(fe_ladder, function(x) x$spec_name, character(1))
}

get_allowed_specs <- function(user_fe_tag) {
  if (identical(user_fe_tag, "with_user_fe")) return(ALLOWED_SPECS_WITH_USER_FE)
  if (identical(user_fe_tag, "no_user_fe")) return(ALLOWED_SPECS_NO_USER_FE)
  stop(paste0("[ERROR] Unknown user_fe_tag: ", user_fe_tag))
}

validate_fe_ladder <- function(fe_ladder, user_fe_tag) {
  ladder_specs <- spec_names_from_ladder(fe_ladder)
  allowed_specs <- get_allowed_specs(user_fe_tag)
  ts_msg("Validating FE ladder:", user_fe_tag,
         "| ladder specs=", paste(ladder_specs, collapse = ", "),
         "| allowed specs=", paste(allowed_specs, collapse = ", "))
  if (!setequal(ladder_specs, allowed_specs)) {
    stop(paste0(
      "[ERROR] FE ladder/spec mismatch for ", user_fe_tag,
      ". Ladder has: ", paste(ladder_specs, collapse = ", "),
      ". Allowed set is: ", paste(allowed_specs, collapse = ", ")
    ))
  }
  invisible(TRUE)
}

validate_profile_results_integrity <- function(profile_table) {
  ts_msg("Validating profile result integrity.")

  if (nrow(profile_table) == 0) {
    ts_msg("Integrity check skipped: profile_table has 0 rows.")
    return(tibble())
  }

  diag <- profile_table %>%
    distinct(user_fe_tag, spec_name) %>%
    mutate(
      expected_family = case_when(
        spec_name %in% ALLOWED_SPECS_WITH_USER_FE ~ "with_user_fe",
        spec_name %in% ALLOWED_SPECS_NO_USER_FE   ~ "no_user_fe",
        TRUE                                      ~ "unknown"
      ),
      is_valid_pair = (user_fe_tag == expected_family)
    ) %>%
    arrange(user_fe_tag, spec_name)

  print(diag)

  bad <- diag %>% filter(!is_valid_pair)
  if (nrow(bad) > 0) {
    stop("[ERROR] profile_all contains contaminated user_fe_tag/spec_name pairs. See printed table above.")
  }

  ts_msg("Integrity check passed: no cross-family contamination found in profile results.")
  diag
}

check_zero_bin <- function(profile_table) {
  if (nrow(profile_table) == 0) return(tibble())
  profile_table %>%
    group_by(window, analysis, sample_tag, cohort_tag, user_fe_tag, spec_name, profile) %>%
    summarise(
      n_rows = n(),
      has_zero_bin = any(as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)),
      n_zero_bin = sum(as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)),
      zero_estimate_ok = all(abs(estimate[as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)]) < 1e-12, na.rm = TRUE),
      .groups = "drop"
    )
}

# =========================
# 7) Separate profiles and old-style interaction differences
# =========================
run_profile_ppml_one_group <- function(dd0, split_var, group_value, profile_name, analysis_name, window_tag,
                                       sample_tag, cohort_tag, fe_ladder, user_fe_tag) {
  out_list <- list()
  k <- 0L

  dd_g <- dd0 %>% filter(.data[[split_var]] == group_value)
  ts_msg("Prepared separate-group sample:",
         "analysis=", analysis_name,
         "| profile=", profile_name,
         "| group_value=", group_value,
         "| user_fe_tag=", user_fe_tag,
         "| rows=", nrow(dd_g),
         "| users=", dplyr::n_distinct(dd_g$user_id),
         "| clusters=", dplyr::n_distinct(dd_g$vcov_cluster),
         "| positives=", sum(dd_g$n_patents > 0, na.rm = TRUE))

  if (nrow(dd_g) == 0) return(tibble())

  ts_msg("Tenure-bin counts for separate-group sample:")
  print(dd_g %>% count(tenure_bin, name = "n") %>% arrange(tenure_bin))

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]

    for (sp in fe_ladder) {
      ts_msg("Running separate-group PPML:",
             "window=", window_tag,
             "| analysis=", analysis_name,
             "| sample=", sample_tag,
             "| cohort=", cohort_tag,
             "| profile=", profile_name,
             "| spec=", sp$spec_name,
             "| weight=", wtag)

      fit <- run_ppml(
        rhs = paste0("i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\")"),
        fe_part = sp$fe,
        data = dd_g,
        wform = wform,
        cluster_var = "vcov_cluster"
      )

      tmp <- extract_fixest_terms(
        fit,
        paste0(window_tag, "__", analysis_name, "__", sample_tag, "__", cohort_tag, "__", user_fe_tag, "__", profile_name, "__", sp$spec_name, "__", wtag)
      ) %>%
        parse_tenure_terms()

      if (nrow(tmp) > 0) {
        k <- k + 1L
        out_list[[k]] <- tmp %>%
          mutate(
            analysis = analysis_name,
            window = window_tag,
            sample_tag = sample_tag,
            cohort_tag = cohort_tag,
            user_fe_tag = user_fe_tag,
            profile = profile_name,
            group_value = group_value,
            spec_name = sp$spec_name,
            weight = wtag,
            n_obs = nrow(dd_g),
            n_users = dplyr::n_distinct(dd_g$user_id),
            n_clusters = dplyr::n_distinct(dd_g$vcov_cluster),
            n_positive = sum(dd_g$n_patents > 0, na.rm = TRUE),
            fe_part = sp$fe
          )
      } else {
        ts_msg("No estimable tenure-bin terms for:", profile_name, sp$spec_name)
      }
    }
  }

  bind_rows(out_list) %>% add_zero_bin_to_profile_terms()
}

run_interaction_difference_ppml <- function(dd0, split_var, analysis_name, window_tag,
                                            sample_tag, cohort_tag, fe_ladder, user_fe_tag) {
  out_list <- list()
  k <- 0L

  dd_i <- dd0 %>%
    filter(.data[[split_var]] %in% c("complement", "top20")) %>%
    mutate(
      group2 = factor(.data[[split_var]], levels = c("complement", "top20"))
    )

  ts_msg("Prepared saturated interaction/difference sample:",
         "analysis=", analysis_name,
         "| user_fe_tag=", user_fe_tag,
         "| rows=", nrow(dd_i),
         "| users=", dplyr::n_distinct(dd_i$user_id),
         "| clusters=", dplyr::n_distinct(dd_i$vcov_cluster),
         "| positives=", sum(dd_i$n_patents > 0, na.rm = TRUE))

  if (nrow(dd_i) == 0) return(tibble())

  ts_msg("Group-by-tenure counts for saturated interaction/difference sample:")
  print(dd_i %>% count(group2, tenure_bin, name = "n") %>% arrange(group2, tenure_bin))

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]

    for (sp in fe_ladder) {
      ts_msg("Running saturated interaction/difference PPML:",
             "window=", window_tag,
             "| analysis=", analysis_name,
             "| sample=", sample_tag,
             "| cohort=", cohort_tag,
             "| profile=interaction",
             "| spec=", sp$spec_name,
             "| weight=", wtag)

      # IMPORTANT:
      # This is the corrected saturated specification.
      #
      # group2:
      #   absorbs the average top20/complement level difference when no user FE are used.
      #   With user FE, group2 is time-invariant and will be absorbed/dropped.
      #
      # i(tenure_bin, ref = "0-5"):
      #   estimates the complement lifecycle ladder.
      #
      # i(tenure_bin, group2, ref = "0-5", ref2 = "complement"):
      #   estimates the additional top20-minus-complement lifecycle difference
      #   at each tenure bin, relative to the 0-5 bin.
      #
      # Therefore, the plotted interaction coefficients are the formal
      # top20-minus-complement differences, not the top20 profile itself.

      rhs <- paste0(
        "group2 + ",
        "i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\") + ",
        "i(tenure_bin, group2, ref = \"0-", TENURE_BIN_STEP,
        "\", ref2 = \"complement\")"
      )

      fml <- as.formula(paste0("n_patents ~ ", rhs, " | ", sp$fe))

      ts_msg("Running PPML formula:", deparse(fml))
      ts_msg("  Main tenure terms: complement lifecycle profile.")
      ts_msg("  Interaction terms: top20 minus complement, relative to tenure 0-", TENURE_BIN_STEP, ".")

      fit <- tryCatch({
        fixest::fepois(
          fml,
          data = dd_i,
          vcov = ~vcov_cluster,
          weights = wform,
          notes = FALSE,
          warn = FALSE
        )
      }, error = function(e) {
        ts_msg("SATURATED INTERACTION MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
        NULL
      })

      tmp <- extract_fixest_terms(
        fit,
        paste0(
          window_tag, "__", analysis_name, "__", sample_tag, "__",
          cohort_tag, "__", user_fe_tag, "__interaction__",
          sp$spec_name, "__", wtag
        )
      ) %>%
        # Keep only the interaction terms involving top20.
        # Drop the main complement tenure-bin terms.
        filter(
          grepl("tenure_bin", term),
          grepl("top20", term)
        ) %>%
        parse_tenure_terms()

      if (nrow(tmp) > 0) {
        k <- k + 1L
        out_list[[k]] <- tmp %>%
          mutate(
            analysis = analysis_name,
            window = window_tag,
            sample_tag = sample_tag,
            cohort_tag = cohort_tag,
            user_fe_tag = user_fe_tag,
            profile = "interaction",
            group_value = "top20_minus_complement",
            spec_name = sp$spec_name,
            weight = wtag,
            n_obs = nrow(dd_i),
            n_users = dplyr::n_distinct(dd_i$user_id),
            n_clusters = dplyr::n_distinct(dd_i$vcov_cluster),
            n_positive = sum(dd_i$n_patents > 0, na.rm = TRUE),
            fe_part = sp$fe
          )
      } else {
        ts_msg("No estimable top20-minus-complement interaction terms for:", sp$spec_name)
      }
    }
  }

  bind_rows(out_list) %>% add_zero_bin_to_profile_terms()
}

run_separate_topcomp_ppml <- function(df_base, split_var, analysis_name, window_tag,
                                      sample_tag = "full_sample", cohort_tag = "all_cohorts",
                                      fe_ladder, user_fe_tag) {
  validate_fe_ladder(fe_ladder, user_fe_tag)

  ts_msg("Starting separate top/complement lifecycle block:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| split_var=", split_var,
         "| sample=", sample_tag,
         "| cohort=", cohort_tag,
         "| user_fe_tag=", user_fe_tag)

  dd0 <- df_base %>%
    filter(.data[[split_var]] %in% c("top20", "complement")) %>%
    prepare_analysis_sample()

  ts_msg("Prepared analysis rows after group and PPML filters:", nrow(dd0))
  ts_msg("Prepared users:", dplyr::n_distinct(dd0$user_id))
  ts_msg("Prepared clusters:", dplyr::n_distinct(dd0$vcov_cluster))

  if (nrow(dd0) == 0) return(tibble())

  ts_msg("Group counts after PPML filters:")
  print(dd0 %>% count(.data[[split_var]], name = "n_rows") %>% arrange(.data[[split_var]]))

  ts_msg("Group-by-tenure counts after PPML filters:")
  print(dd0 %>% count(.data[[split_var]], tenure_bin, name = "n_rows") %>% arrange(.data[[split_var]], tenure_bin))

  top_tab <- run_profile_ppml_one_group(
    dd0 = dd0,
    split_var = split_var,
    group_value = "top20",
    profile_name = "top",
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    cohort_tag = cohort_tag,
    fe_ladder = fe_ladder,
    user_fe_tag = user_fe_tag
  )

  comp_tab <- run_profile_ppml_one_group(
    dd0 = dd0,
    split_var = split_var,
    group_value = "complement",
    profile_name = "complement",
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    cohort_tag = cohort_tag,
    fe_ladder = fe_ladder,
    user_fe_tag = user_fe_tag
  )

  int_tab <- run_interaction_difference_ppml(
    dd0 = dd0,
    split_var = split_var,
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    cohort_tag = cohort_tag,
    fe_ladder = fe_ladder,
    user_fe_tag = user_fe_tag
  )

  out <- bind_rows(top_tab, comp_tab, int_tab)
  ts_msg("Finished separate top/complement lifecycle block. Rows produced:", nrow(out))
  out
}

filter_plot_data <- function(profile_table, window_tag, analysis_name, sample_tag, cohort_tag, user_fe_tag, profile) {
  if (nrow(profile_table) == 0 || !("window" %in% names(profile_table))) return(tibble())
  allowed_specs <- get_allowed_specs(user_fe_tag)
  profile_table %>%
    filter(
      .data$window == .env$window_tag,
      .data$analysis == .env$analysis_name,
      .data$sample_tag == .env$sample_tag,
      .data$cohort_tag == .env$cohort_tag,
      .data$user_fe_tag == .env$user_fe_tag,
      .data$profile == .env$profile,
      .data$weight == "unweighted",
      .data$spec_name %in% .env$allowed_specs
    ) %>%
    mutate(
      spec_name = factor(spec_name, levels = allowed_specs),
      tenure_bin = factor(as.character(tenure_bin), levels = ordered_tenure_levels(), ordered = TRUE)
    ) %>%
    arrange(spec_name, tenure_bin)
}

plot_profile_core <- function(tab, window_tag, analysis_name, profile, sample_tag, cohort_tag, user_fe_tag) {
  if (nrow(tab) == 0) return(NULL)

  ylab <- switch(profile,
    top = "Top20 coefficient (relative to tenure 0-5)",
    complement = "Complement coefficient (relative to tenure 0-5)",
    interaction = "Top20 - complement difference"
  )
  title_stub <- switch(profile,
    top = "Separate PPML top20 profile",
    complement = "Separate PPML complement profile",
    interaction = "Saturated PPML top20-minus-complement difference"
  )
  caption_stub <- switch(profile,
    top = "Unweighted PPML estimated separately on the top20 subsample. Standard errors clustered by metro-parent cell. Explicit 0-5 point added to graph.",
    complement = "Unweighted PPML estimated separately on the complement subsample. Standard errors clustered by metro-parent cell. Explicit 0-5 point added to graph.",
    interaction = "Unweighted PPML with main tenure-bin effects plus top20-by-tenure interactions. Standard errors clustered by metro-parent cell. Explicit 0-5 point added to graph."
  )

  ggplot(tab, aes(x = tenure_bin, y = estimate, color = spec_name, group = spec_name)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray50") +
    geom_line(position = position_dodge(width = 0.35), linewidth = 0.7, alpha = 0.95) +
    geom_errorbar(aes(ymin = conf_low, ymax = conf_high), width = 0.12, alpha = 0.7, position = position_dodge(width = 0.35), linewidth = 0.55) +
    geom_point(
      position = position_dodge(width = 0.35),
      size = 2.6,
      shape = 21,
      fill = "white",
      stroke = 0.8
    ) +
    labs(
      title = paste0(title_stub, " — ", analysis_name, " — ", window_tag),
      subtitle = paste0("sample=", sample_tag, " | cohort=", cohort_tag, " | ", user_fe_tag),
      x = "Tenure bin",
      y = ylab,
      color = "Specification",
      caption = caption_stub
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")
}

plot_profile_with_user_fe <- function(profile_table, window_tag, analysis_name, profile = c("top", "complement", "interaction"), sample_tag = "full_sample", cohort_tag = "all_cohorts") {
  profile <- match.arg(profile)
  tab <- filter_plot_data(profile_table, window_tag, analysis_name, sample_tag, cohort_tag, user_fe_tag = "with_user_fe", profile = profile)
  if (nrow(tab) == 0) return(NULL)
  plot_profile_core(tab, window_tag, analysis_name, profile, sample_tag, cohort_tag, user_fe_tag = "with_user_fe")
}

plot_profile_no_user_fe <- function(profile_table, window_tag, analysis_name, profile = c("top", "complement", "interaction"), sample_tag = "full_sample", cohort_tag = "all_cohorts") {
  profile <- match.arg(profile)
  tab <- filter_plot_data(profile_table, window_tag, analysis_name, sample_tag, cohort_tag, user_fe_tag = "no_user_fe", profile = profile)
  if (nrow(tab) == 0) return(NULL)
  plot_profile_core(tab, window_tag, analysis_name, profile, sample_tag, cohort_tag, user_fe_tag = "no_user_fe")
}

# =========================
# 8) Build samples + run
# =========================
df_master <- df_full %>%
  left_join(
    inventor_ref %>%
      select(user_id, grad_cohort, career_topcomp,
             first_metro, metro_size_2000, metro_prod_2000,
             metro_size_topcomp, metro_prod_topcomp,
             same_start_end_metro, never_moved_metro) %>%
      distinct(user_id, .keep_all = TRUE),
    by = "user_id"
  )

profile_results <- list()
fe_decomp_results <- list()
mp_fixef_results <- list()

sample_defs <- list(
  list(sample_tag = "full_sample", expr = NULL),
  list(sample_tag = "same_start_end_metro", expr = "same_start_end_metro == TRUE"),
  list(sample_tag = "never_moved_metro", expr = "never_moved_metro == TRUE")
)

analysis_defs <- list(
  list(name = "CAREER_TOPCOMP", split_var = "career_topcomp"),
  list(name = "FIRSTMETRO_SIZE_TOPCOMP", split_var = "metro_size_topcomp"),
  list(name = "FIRSTMETRO_PROD_TOPCOMP", split_var = "metro_prod_topcomp")
)

selected_windows <- intersect(names(WINDOWS), parse_windows_env(WINDOW_TAGS_ENV))
if (length(selected_windows) == 0) stop("[ERROR] No valid windows selected.")

chunk_analysis_defs <- function(chunk_mode) {
  switch(
    chunk_mode,
    career = analysis_defs[1],
    metro_size = analysis_defs[2],
    metro_prod = analysis_defs[3],
    fe_decomp = analysis_defs,
    all = analysis_defs,
    stop(paste0("[ERROR] Unknown CHUNK_MODE: ", chunk_mode))
  )
}

active_analysis_defs <- chunk_analysis_defs(CHUNK_MODE)

# In the Slurm array, CHUNK_MODE="fe_decomp" is reserved for the
# metro-parent FE decomposition. In that chunk, the script must not rerun
# the lifecycle PPML ladders or regenerate lifecycle ladder plots.
RUN_LIFECYCLE_PROFILES <- !identical(CHUNK_MODE, "fe_decomp")

ts_msg("Selected windows:", paste(selected_windows, collapse = ", "))
ts_msg("Chunk mode:", CHUNK_MODE)
ts_msg("RUN_LIFECYCLE_PROFILES:", RUN_LIFECYCLE_PROFILES)
ts_msg("RUN_NO_USER_FE:", RUN_NO_USER_FE)
ts_msg("RUN_COHORTS:", RUN_COHORTS)
ts_msg("RUN_FE_DECOMP:", RUN_FE_DECOMP)
ts_msg("Output directory:", OUT_DIR)

for (window_tag in selected_windows) {
  yr <- WINDOWS[[window_tag]]
  ts_msg("Processing window:", window_tag, "| years:", paste(yr, collapse = "-"))
  df_window <- df_master %>% filter(year >= yr[1], year <= yr[2])
  ts_msg("Rows in window sample:", nrow(df_window))
  ts_msg("Users in window sample:", dplyr::n_distinct(df_window$user_id))

  for (sd in sample_defs) {
    ts_msg("Processing sample:", sd$sample_tag)
    df_s <- df_window
    if (!is.null(sd$expr)) df_s <- df_s %>% filter(!!rlang::parse_expr(sd$expr))
    ts_msg("Rows after sample filter:", nrow(df_s))
    ts_msg("Users after sample filter:", dplyr::n_distinct(df_s$user_id))

    for (ad in active_analysis_defs) {
      ts_msg("Processing analysis:", ad$name, "| split_var:", ad$split_var)
      ts_msg("Raw split counts before PPML filters:")
      print(df_s %>% count(.data[[ad$split_var]], name = "n_rows") %>% arrange(.data[[ad$split_var]]))

      if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
        profile_results[[length(profile_results) + 1L]] <- run_separate_topcomp_ppml(
          df_s, ad$split_var, ad$name, window_tag,
          sample_tag = sd$sample_tag,
          cohort_tag = "all_cohorts",
          fe_ladder = fe_ladder_user,
          user_fe_tag = "with_user_fe"
        )

        if (isTRUE(RUN_NO_USER_FE)) {
          profile_results[[length(profile_results) + 1L]] <- run_separate_topcomp_ppml(
            df_s, ad$split_var, ad$name, window_tag,
            sample_tag = sd$sample_tag,
            cohort_tag = "all_cohorts",
            fe_ladder = fe_ladder_no_user,
            user_fe_tag = "no_user_fe"
          )
        }
      } else {
        ts_msg(
          "Skipping lifecycle PPML profiles because CHUNK_MODE=fe_decomp:",
          "window=", window_tag,
          "| analysis=", ad$name,
          "| sample=", sd$sample_tag,
          "| cohort=all_cohorts"
        )
      }

      if (isTRUE(RUN_FE_DECOMP)) {
        fe_tmp <- run_fe_decomposition(
          df_s %>% filter(.data[[ad$split_var]] %in% c("top20", "complement")),
          ad$name, window_tag,
          sample_tag = sd$sample_tag,
          cohort_tag = "all_cohorts"
        )
        fe_decomp_results[[length(fe_decomp_results) + 1L]] <- fe_tmp$fe_decomp
        mp_fixef_results[[length(mp_fixef_results) + 1L]] <- fe_tmp$mp_fixef
      }
    }

    if (isTRUE(RUN_COHORTS)) {
      for (coh_nm in names(COHORTS)) {
        rg <- COHORTS[[coh_nm]]
        ts_msg("Processing cohort:", coh_nm, "| range:", paste(rg, collapse = "-"))
        df_c <- df_s %>% filter(is.finite(ref_year), ref_year >= rg[1], ref_year <= rg[2])
        ts_msg("Rows in cohort sample:", nrow(df_c))
        ts_msg("Users in cohort sample:", dplyr::n_distinct(df_c$user_id))

        for (ad in active_analysis_defs) {
          ts_msg("Processing cohort analysis:", ad$name, "| split_var:", ad$split_var)
          ts_msg("Raw cohort split counts before PPML filters:")
          print(df_c %>% count(.data[[ad$split_var]], name = "n_rows") %>% arrange(.data[[ad$split_var]]))

          if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
            profile_results[[length(profile_results) + 1L]] <- run_separate_topcomp_ppml(
              df_c, ad$split_var, ad$name, window_tag,
              sample_tag = sd$sample_tag,
              cohort_tag = coh_nm,
              fe_ladder = fe_ladder_user,
              user_fe_tag = "with_user_fe"
            )

            if (isTRUE(RUN_NO_USER_FE)) {
              profile_results[[length(profile_results) + 1L]] <- run_separate_topcomp_ppml(
                df_c, ad$split_var, ad$name, window_tag,
                sample_tag = sd$sample_tag,
                cohort_tag = coh_nm,
                fe_ladder = fe_ladder_no_user,
                user_fe_tag = "no_user_fe"
              )
            }
          } else {
            ts_msg(
              "Skipping lifecycle PPML profiles because CHUNK_MODE=fe_decomp:",
              "window=", window_tag,
              "| analysis=", ad$name,
              "| sample=", sd$sample_tag,
              "| cohort=", coh_nm
            )
          }

          if (isTRUE(RUN_FE_DECOMP)) {
            fe_tmp <- run_fe_decomposition(
              df_c %>% filter(.data[[ad$split_var]] %in% c("top20", "complement")),
              ad$name, window_tag,
              sample_tag = sd$sample_tag,
              cohort_tag = coh_nm
            )
            fe_decomp_results[[length(fe_decomp_results) + 1L]] <- fe_tmp$fe_decomp
            mp_fixef_results[[length(mp_fixef_results) + 1L]] <- fe_tmp$mp_fixef
          }
        }
      }
    }
  }
}

profile_all <- bind_rows(profile_results)
fe_decomp_all <- bind_rows(fe_decomp_results)
mp_fixef_all <- bind_rows(mp_fixef_results)

ts_msg("Final profile rows:", nrow(profile_all))
ts_msg("Final FE decomp rows:", nrow(fe_decomp_all))
ts_msg("Final MP fixef rows:", nrow(mp_fixef_all))

chunk_prefix <- paste0(
  "chunk__", safe_name(CHUNK_MODE),
  "__windows__", safe_name(paste(selected_windows, collapse = "__"))
)

if (nrow(profile_all) > 0) {
  integrity_check <- validate_profile_results_integrity(profile_all)

  write_csv(profile_all, file.path(dirs$chunk_tables, paste0(chunk_prefix, "__separate_topcomp_profiles_all.csv")))
  write_csv(integrity_check, file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__integrity_check_user_fe_vs_spec.csv")))

  zero_check <- check_zero_bin(profile_all)
  write_csv(zero_check, file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__zero_bin_check_by_fe_family.csv")))

  # Compatibility copy with the older filename pattern, in case downstream code
  # expects the chunk table to contain "joint_topcomp_profiles_all".
  write_csv(profile_all, file.path(dirs$chunk_tables, paste0(chunk_prefix, "__joint_topcomp_profiles_all.csv")))

  ts_msg("Saved separate profile table, compatibility table, integrity check, and zero-bin check.")
} else {
  ts_msg("No lifecycle profile rows produced in this chunk. Skipping profile table, integrity check, and zero-bin check.")
}

if (nrow(fe_decomp_all) > 0) {
  write_csv(fe_decomp_all, file.path(dirs$chunk_tables, paste0(chunk_prefix, "__fe_decomposition_all.csv")))
  ts_msg("Saved FE decomposition table.")
}

if (nrow(mp_fixef_all) > 0) {
  write_csv(mp_fixef_all, file.path(dirs$chunk_tables, paste0(chunk_prefix, "__mp_fixef_all.csv")))
  ts_msg("Saved metro-parent fixed-effect table.")
}

# =========================
# 9) Save plots
# =========================
for (window_tag in selected_windows) {
  for (ad in active_analysis_defs) {
    for (sd in sample_defs) {
      ts_msg("Saving plots:",
             "window=", window_tag,
             "| analysis=", ad$name,
             "| sample=", sd$sample_tag)

      if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
        ts_msg("Saving all-cohort lifecycle plots.")

        for (prof in c("top", "complement", "interaction")) {
          p1 <- plot_profile_with_user_fe(
            profile_all, window_tag, ad$name, prof,
            sample_tag = sd$sample_tag,
            cohort_tag = "all_cohorts"
          )
          if (!is.null(p1)) {
            save_plot(
              p1,
              file.path(
                dirs$plots,
                paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__with_user_fe__", prof, ".png")
              )
            )
          }

          if (isTRUE(RUN_NO_USER_FE)) {
            p2 <- plot_profile_no_user_fe(
              profile_all, window_tag, ad$name, prof,
              sample_tag = sd$sample_tag,
              cohort_tag = "all_cohorts"
            )
            if (!is.null(p2)) {
              save_plot(
                p2,
                file.path(
                  dirs$plots,
                  paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__no_user_fe__", prof, ".png")
                )
              )
            }
          }
        }
      } else {
        ts_msg("Skipping all-cohort lifecycle plots because CHUNK_MODE=fe_decomp.")
      }

      if (isTRUE(RUN_FE_DECOMP)) {
        ts_msg("Saving all-cohort FE-decomposition plot.")
        p_fe_all <- plot_mp_fe_decomp(
          fe_decomp_all, window_tag, ad$name,
          sample_tag = sd$sample_tag,
          cohort_tag = "all_cohorts"
        )
        if (!is.null(p_fe_all)) {
          save_plot(
            p_fe_all,
            file.path(
              dirs$plots,
              paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__mp_fe_decomp_ols.png")
            )
          )
        }
      }

      if (isTRUE(RUN_COHORTS)) {
        for (coh_nm in names(COHORTS)) {
          ts_msg("Saving cohort plots:",
                 "window=", window_tag,
                 "| analysis=", ad$name,
                 "| sample=", sd$sample_tag,
                 "| cohort=", coh_nm)

          if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
            ts_msg("Saving cohort lifecycle plots.")

            for (prof in c("top", "complement", "interaction")) {
              p3 <- plot_profile_with_user_fe(
                profile_all, window_tag, ad$name, prof,
                sample_tag = sd$sample_tag,
                cohort_tag = coh_nm
              )
              if (!is.null(p3)) {
                save_plot(
                  p3,
                  file.path(
                    dirs$plots,
                    paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__", safe_name(coh_nm), "__with_user_fe__", prof, ".png")
                  )
                )
              }

              if (isTRUE(RUN_NO_USER_FE)) {
                p4 <- plot_profile_no_user_fe(
                  profile_all, window_tag, ad$name, prof,
                  sample_tag = sd$sample_tag,
                  cohort_tag = coh_nm
                )
                if (!is.null(p4)) {
                  save_plot(
                    p4,
                    file.path(
                      dirs$plots,
                      paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__", safe_name(coh_nm), "__no_user_fe__", prof, ".png")
                    )
                  )
                }
              }
            }
          } else {
            ts_msg("Skipping cohort lifecycle plots because CHUNK_MODE=fe_decomp.")
          }

          if (isTRUE(RUN_FE_DECOMP)) {
            ts_msg("Saving cohort FE-decomposition plot.")
            p_fe_coh <- plot_mp_fe_decomp(
              fe_decomp_all, window_tag, ad$name,
              sample_tag = sd$sample_tag,
              cohort_tag = coh_nm
            )
            if (!is.null(p_fe_coh)) {
              save_plot(
                p_fe_coh,
                file.path(
                  dirs$plots,
                  paste0(window_tag, "__", safe_name(ad$name), "__", safe_name(sd$sample_tag), "__", safe_name(coh_nm), "__mp_fe_decomp_ols.png")
                )
              )
            }
          }
        }
      }
    }
  }
}

write_csv(
  tibble(
    chunk_mode = CHUNK_MODE,
    windows = paste(selected_windows, collapse = ","),
    run_no_user_fe = RUN_NO_USER_FE,
    run_cohorts = RUN_COHORTS,
    run_fe_decomp = RUN_FE_DECOMP,
    run_lifecycle_profiles = RUN_LIFECYCLE_PROFILES,
    profile_design = "top_and_complement_estimated_separately; interaction_estimated_with_saturated_main_tenure_plus_top20_by_tenure_design",
    n_profile_rows = nrow(profile_all),
    n_fe_rows = nrow(fe_decomp_all),
    n_mp_fixef_rows = nrow(mp_fixef_all),
    output_dir = OUT_DIR
  ),
  file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__manifest.csv"))
)

ts_msg("DONE. Outputs in:", OUT_DIR)
