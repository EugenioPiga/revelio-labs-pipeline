#!/usr/bin/env Rscript

##############################################################################
#
# This script estimates PPML lifecycle patenting profiles by tenure bin using
# inventor-year panel data from:
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2
#
# The script:
#   1. Loads inventor-year data for 2000--2024 and restricts to U.S. inventor-years.
#   2. Constructs tenure, tenure bins, reference cohort year, parent-firm,
#      metro, metro-parent, and metro-parent-year identifiers.
#   3. Constructs four top/complement splits:
#        - CAREER_TOPCOMP
#        - FIRSTMETRO_SIZE_TOPCOMP
#        - FIRSTMETRO_PROD_TOPCOMP
#        - FIRSTMETRO_DENSITY_TOPCOMP
#   4. Constructs inventor-level mobility samples:
#        - full_sample
#        - same_start_end_metro
#        - never_moved_metro
#        - same_start_end_mp
#        - never_moved_mp
#   5. Estimates PPML lifecycle ladders for:
#        - the full unsplit sample;
#        - separate top20 and complement profiles;
#        - pooled saturated top/complement profiles with tenure-bin-by-group
#          interactions.
#   6. Uses only with-user fixed-effect specifications:
#        - baseline_user_year: user_id + year_fe
#        - plus_parent_fe:     user_id + year_fe + parent_fe
#        - plus_metro_fe:      user_id + year_fe + metro_fe
#        - plus_mp_fe:         user_id + year_fe + mp_fe
#        - plus_pmy_fe:        user_id + pmy_fe
#   7. Clusters standard errors at the metro-parent level.
#   8. Saves raw coefficients, standard errors, confidence intervals, reference
#      bin means, implied level-scale plotting variables, support diagnostics,
#      lifecycle-growth decomposition tables, FE-decomposition tables, plots,
#      and manifests.
#   9. Does not run cohort-specific analyses. If RUN_COHORTS is exported by
#      sbatch, it is ignored harmlessly.
#
# Output directory:
#   /home/epiga/revelio_labs/output/ppml_tenure_ladder_main_revised_nocohorts
#
# Plotting convention:
#   PPML estimates are log-link coefficients. Lifecycle plots are converted to
#   implied levels using:
#       plot_estimate_b = mean_patents_0_5_group * exp(beta_b)
#   with beta_0-5 = 0.
##############################################################################

Sys.setenv(
  OMP_NUM_THREADS = "1",
  OPENBLAS_NUM_THREADS = "1",
  MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1",
  NUMEXPR_NUM_THREADS = "1"
)

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

INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR <- "/home/epiga/revelio_labs/output/ppml_tenure_ladder_main_revised_nocohorts"

DENSITY_CROSSWALK <- "/home/epiga/revelio_labs/output/metro_density_match_check/diagnostics/final_revelio_metro_density_crosswalk.csv"

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

METRO_CANDIDATES <- c(
  "first_metro", "metro", "metro_name",
  "first_metro_area", "msa", "msa_name",
  "first_cbsa", "cbsa", "cbsa_name"
)

GENDER_CANDIDATES <- c("au_sex_predicted", "sex_predicted")

WEIGHTS <- list(unweighted = NULL)

CHUNK_MODE <- Sys.getenv("CHUNK_MODE", unset = "all")
WINDOW_TAGS_ENV <- Sys.getenv("WINDOW_TAGS", unset = paste(names(WINDOWS), collapse = ","))
RUN_FE_DECOMP <- identical(Sys.getenv("RUN_FE_DECOMP", unset = "1"), "1")
RUN_NO_USER_FE_EXPORTED_BUT_IGNORED <- Sys.getenv("RUN_NO_USER_FE", unset = NA_character_)
RUN_COHORTS_EXPORTED_BUT_IGNORED <- Sys.getenv("RUN_COHORTS", unset = NA_character_)

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
  x <- gsub("[^A-Za-z0-9]+", "_", as.character(x))
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
  lowers <- seq(0, max_tenure - step, by = step) +
    c(0, rep(1, length(seq(step, max_tenure, by = step)) - 1))
  uppers <- seq(step, max_tenure, by = step)
  labels <- ifelse(lowers == 0, paste0("0-", step), paste0(lowers, "-", uppers))
  cut(x, breaks = breaks, labels = labels, include.lowest = TRUE, right = TRUE)
}

ordered_tenure_levels <- function() {
  lowers <- seq(0, TENURE_MAX - TENURE_BIN_STEP, by = TENURE_BIN_STEP) +
    c(0, rep(1, length(seq(TENURE_BIN_STEP, TENURE_MAX, by = TENURE_BIN_STEP)) - 1))
  uppers <- seq(TENURE_BIN_STEP, TENURE_MAX, by = TENURE_BIN_STEP)
  ifelse(lowers == 0, paste0("0-", TENURE_BIN_STEP), paste0(lowers, "-", uppers))
}

make_top_complement_split <- function(x, p = TOP_P) {
  q_hi <- suppressWarnings(as.numeric(stats::quantile(x, probs = 1 - p, na.rm = TRUE, type = 7)))
  dplyr::case_when(
    !is.finite(x) ~ NA_character_,
    x >= q_hi     ~ "top20",
    TRUE          ~ "complement"
  )
}

read_density_crosswalk <- function(path) {
  if (!file.exists(path)) {
    stop(paste0("[ERROR] Density crosswalk not found: ", path))
  }

  ts_msg("Reading final Revelio-to-CBSA density crosswalk:", path)

  cw <- readr::read_csv(path, show_col_types = FALSE)

  need_cols(
    names(cw),
    c(
      "revelio_metro_raw",
      "cbsa_geoid",
      "cbsa_name",
      "population_2023_acs5",
      "land_area_sqmi",
      "land_area_sqkm",
      "density_per_sqmi",
      "density_per_sqkm",
      "has_valid_density",
      "match_source",
      "match_confidence",
      "match_note"
    )
  )

  cw <- cw %>%
    mutate(
      revelio_metro_raw = str_trim(as.character(revelio_metro_raw)),
      cbsa_geoid = as.character(cbsa_geoid),
      cbsa_name = as.character(cbsa_name),
      population_2023_acs5 = suppressWarnings(as.numeric(population_2023_acs5)),
      land_area_sqmi = suppressWarnings(as.numeric(land_area_sqmi)),
      land_area_sqkm = suppressWarnings(as.numeric(land_area_sqkm)),
      density_per_sqmi = suppressWarnings(as.numeric(density_per_sqmi)),
      density_per_sqkm = suppressWarnings(as.numeric(density_per_sqkm)),
      has_valid_density = case_when(
        has_valid_density %in% c(TRUE, "TRUE", "True", "true", 1, "1") ~ TRUE,
        TRUE ~ FALSE
      ),
      log_density_per_sqmi = ifelse(
        has_valid_density & is.finite(density_per_sqmi) & density_per_sqmi > 0,
        log(density_per_sqmi),
        NA_real_
      ),
      log_density_per_sqkm = ifelse(
        has_valid_density & is.finite(density_per_sqkm) & density_per_sqkm > 0,
        log(density_per_sqkm),
        NA_real_
      )
    ) %>%
    distinct(revelio_metro_raw, .keep_all = TRUE)

  split_vec <- ifelse(cw$has_valid_density, cw$log_density_per_sqmi, NA_real_)

  cw <- cw %>%
    mutate(
      metro_density_topcomp = make_top_complement_split(split_vec, p = TOP_P),
      metro_density_topcomp = ifelse(has_valid_density, metro_density_topcomp, NA_character_)
    )

  diag <- cw %>%
    summarise(
      n_revelio_metro_labels = n(),
      n_valid_density = sum(has_valid_density, na.rm = TRUE),
      n_missing_density = sum(!has_valid_density, na.rm = TRUE),
      n_top20_density = sum(metro_density_topcomp == "top20", na.rm = TRUE),
      n_complement_density = sum(metro_density_topcomp == "complement", na.rm = TRUE),
      min_density = min(density_per_sqmi[has_valid_density], na.rm = TRUE),
      p25_density = as.numeric(quantile(density_per_sqmi[has_valid_density], 0.25, na.rm = TRUE)),
      p50_density = as.numeric(quantile(density_per_sqmi[has_valid_density], 0.50, na.rm = TRUE)),
      p75_density = as.numeric(quantile(density_per_sqmi[has_valid_density], 0.75, na.rm = TRUE)),
      max_density = max(density_per_sqmi[has_valid_density], na.rm = TRUE)
    )

  ts_msg("Density crosswalk diagnostics:")
  print(diag)

  write_csv(
    diag,
    file.path(dirs$diagnostics, "density_crosswalk_split_diagnostics.csv")
  )

  write_csv(
    cw %>%
      filter(has_valid_density) %>%
      arrange(desc(density_per_sqmi)) %>%
      select(
        revelio_metro_raw,
        cbsa_geoid,
        cbsa_name,
        density_per_sqmi,
        log_density_per_sqmi,
        metro_density_topcomp,
        n_inventor_years,
        n_users,
        total_patents,
        match_source,
        match_confidence,
        match_note
      ),
    file.path(dirs$diagnostics, "density_crosswalk_valid_metros_ranked.csv")
  )

  cw %>%
    transmute(
      first_metro = revelio_metro_raw,
      first_metro_cbsa_geoid = cbsa_geoid,
      first_metro_cbsa_name = cbsa_name,
      first_metro_population_2023_acs5 = population_2023_acs5,
      first_metro_land_area_sqmi = land_area_sqmi,
      first_metro_land_area_sqkm = land_area_sqkm,
      first_metro_density_per_sqmi = density_per_sqmi,
      first_metro_density_per_sqkm = density_per_sqkm,
      first_metro_log_density_per_sqmi = log_density_per_sqmi,
      first_metro_log_density_per_sqkm = log_density_per_sqkm,
      first_metro_has_valid_density = has_valid_density,
      first_metro_density_match_source = match_source,
      first_metro_density_match_confidence = match_confidence,
      first_metro_density_match_note = match_note,
      metro_density_topcomp = metro_density_topcomp
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

save_plot <- function(p, path, width = 11, height = 7) {
  ts_msg("Saving plot:", path)
  ggplot2::ggsave(path, p, width = width, height = height, dpi = 300)
}

print_count_by <- function(df, var, label) {
  ts_msg(label)
  if (is.null(var) || !(var %in% names(df))) {
    print(tibble(note = "No variable supplied or variable missing."))
    return(invisible(NULL))
  }
  print(
    df %>%
      group_by(group = .data[[var]]) %>%
      summarise(n_rows = n(), n_users = n_distinct(user_id), .groups = "drop") %>%
      arrange(group)
  )
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
      tenure_bin = factor(as.character(tenure_bin), levels = ordered_tenure_levels(), ordered = TRUE),
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

get_fit_data <- function(fit, data) {
  if (is.null(fit)) return(data[0, , drop = FALSE])
  idx <- tryCatch(fixest::obs(fit), error = function(e) NULL)
  if (is.null(idx)) {
    ts_msg("Could not recover exact fixest estimation rows with fixest::obs(); using pre-estimation filtered data.")
    return(data)
  }
  if (is.logical(idx) && length(idx) == nrow(data)) return(data[idx, , drop = FALSE])
  if (is.numeric(idx)) return(data[idx, , drop = FALSE])
  data
}

mean_ref_patents <- function(data, group_var = NULL, group_value = NULL) {
  dd <- data
  if (!is.null(group_var) && !is.null(group_value)) {
    dd <- dd %>% filter(.data[[group_var]] == group_value)
  }
  ref_lab <- paste0("0-", TENURE_BIN_STEP)
  out <- dd %>%
    filter(as.character(tenure_bin) == ref_lab) %>%
    summarise(
      mean_patents_0_5 = mean(n_patents, na.rm = TRUE),
      n_ref_bin = n(),
      n_ref_positive = sum(n_patents > 0, na.rm = TRUE),
      .groups = "drop"
    )
  if (nrow(out) == 0 || !is.finite(out$mean_patents_0_5[1])) {
    return(tibble(mean_patents_0_5 = NA_real_, n_ref_bin = 0L, n_ref_positive = 0L))
  }
  out
}

add_plot_scale <- function(tab) {
  if (nrow(tab) == 0) return(tab)
  tab %>%
    mutate(
      plot_estimate = mean_patents_0_5 * exp(estimate),
      plot_low      = mean_patents_0_5 * exp(conf_low),
      plot_high     = mean_patents_0_5 * exp(conf_high)
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

detect_tenure_level <- function(term) {
  levs <- ordered_tenure_levels()
  hit <- levs[vapply(levs, function(z) grepl(z, term, fixed = TRUE), logical(1))]
  if (length(hit) == 0) return(NA_character_)
  hit[1]
}

parse_tenure_terms <- function(tab) {
  levs <- ordered_tenure_levels()
  tab %>%
    filter(grepl("tenure_bin", term)) %>%
    mutate(
      raw_term = term,
      tenure_bin_chr = vapply(term, detect_tenure_level, character(1)),
      tenure_bin = factor(tenure_bin_chr, levels = levs, ordered = TRUE),
      conf_low  = estimate - 1.96 * se,
      conf_high = estimate + 1.96 * se,
      p_label   = ifelse(is.finite(p), sprintf("p=%.3f", p), NA_character_)
    ) %>%
    filter(!is.na(tenure_bin)) %>%
    select(-tenure_bin_chr)
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
    fit <- fixest::fepois(fml, data = data, vcov = vcov_fml, weights = wform, notes = FALSE, warn = FALSE)
    if (!is.null(fit$collin.var) && length(fit$collin.var) > 0) {
      ts_msg("Collinear variables dropped by fixest:", paste(fit$collin.var, collapse = "; "))
    }
    fit
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
    fit <- fixest::feols(fml, data = data, vcov = vcov_fml, weights = wform, notes = FALSE, warn = FALSE)
    if (!is.null(fit$collin.var) && length(fit$collin.var) > 0) {
      ts_msg("Collinear variables dropped by fixest:", paste(fit$collin.var, collapse = "; "))
    }
    fit
  }, error = function(e) {
    ts_msg("OLS MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

ALLOWED_SPECS_WITH_USER_FE <- c(
  "baseline_user_year",
  "plus_parent_fe",
  "plus_metro_fe",
  "plus_mp_fe",
  "plus_pmy_fe"
)

fe_ladder_user <- list(
  list(spec_name = "baseline_user_year", fe = "user_id + year_fe"),
  list(spec_name = "plus_parent_fe",     fe = "user_id + year_fe + parent_fe"),
  list(spec_name = "plus_metro_fe",      fe = "user_id + year_fe + metro_fe"),
  list(spec_name = "plus_mp_fe",         fe = "user_id + year_fe + mp_fe"),
  list(spec_name = "plus_pmy_fe",        fe = "user_id + pmy_fe")
)

validate_fe_ladder <- function(fe_ladder) {
  ladder_specs <- vapply(fe_ladder, function(x) x$spec_name, character(1))
  ts_msg("Validating FE ladder:", paste(ladder_specs, collapse = ", "))
  if (!setequal(ladder_specs, ALLOWED_SPECS_WITH_USER_FE)) {
    stop("[ERROR] FE ladder does not match allowed with-user-FE specifications.")
  }
  invisible(TRUE)
}

make_support_diagnostics <- function(dd, analysis_name, split_var, window_tag, sample_tag) {
  ref_lab <- paste0("0-", TENURE_BIN_STEP)

  if (!is.null(split_var) && split_var %in% names(dd)) {
    out <- dd %>%
      mutate(diag_group = as.character(.data[[split_var]])) %>%
      filter(diag_group %in% c("top20", "complement"))
  } else {
    out <- dd %>% mutate(diag_group = "full")
  }

  diag <- out %>%
    group_by(diag_group, tenure_bin) %>%
    summarise(
      n_obs = n(),
      n_users = n_distinct(user_id),
      n_clusters = n_distinct(vcov_cluster),
      n_positive = sum(n_patents > 0, na.rm = TRUE),
      mean_patents = mean(n_patents, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      analysis = analysis_name,
      window = window_tag,
      sample_tag = sample_tag,
      cohort_tag = "all_cohorts",
      is_ref_bin = as.character(tenure_bin) == ref_lab,
      low_ref_support = is_ref_bin & n_obs < 100,
      sparse_cell = n_obs < 50 | n_positive < 5
    ) %>%
    select(analysis, window, sample_tag, cohort_tag, diag_group, tenure_bin,
           n_obs, n_users, n_clusters, n_positive, mean_patents,
           is_ref_bin, low_ref_support, sparse_cell)

  ts_msg("Support diagnostics:",
         "analysis=", analysis_name,
         "| window=", window_tag,
         "| sample=", sample_tag)
  print(diag)

  warn_ref <- diag %>% filter(is_ref_bin, low_ref_support)
  if (nrow(warn_ref) > 0) {
    ts_msg("WARNING: Low support in 0-5 reference bin.")
    print(warn_ref)
  }

  sparse <- diag %>% filter(sparse_cell)
  if (nrow(sparse) > 0) {
    ts_msg("WARNING: Sparse group x tenure cells detected.")
    print(sparse)
  }

  diag
}

make_profile_from_simple_fit <- function(fit, dd_pre, analysis_name, window_tag,
                                         sample_tag, profile_source,
                                         profile, group_value, spec_name,
                                         weight, fe_part, model_tag) {
  if (is.null(fit)) return(tibble())

  dd_fit <- get_fit_data(fit, dd_pre)
  ref <- mean_ref_patents(dd_fit)
  term_tab <- extract_fixest_terms(fit, model_tag) %>% parse_tenure_terms()

  levs <- ordered_tenure_levels()
  ref_lab <- paste0("0-", TENURE_BIN_STEP)

  zero_row <- tibble(
    model = model_tag,
    term = paste0("__REF_", ref_lab, "__"),
    estimate = 0,
    se = 0,
    stat = NA_real_,
    p = NA_real_,
    stars = "",
    collinear_dropped = if (!is.null(fit$collin.var) && length(fit$collin.var) > 0) paste(fit$collin.var, collapse = ";") else "",
    raw_term = paste0("__REF_", ref_lab, "__"),
    tenure_bin = factor(ref_lab, levels = levs, ordered = TRUE),
    conf_low = 0,
    conf_high = 0,
    p_label = NA_character_
  )

  bind_rows(term_tab, zero_row) %>%
    mutate(
      analysis = analysis_name,
      window = window_tag,
      sample_tag = sample_tag,
      cohort_tag = "all_cohorts",
      user_fe_tag = "with_user_fe",
      profile_source = profile_source,
      profile = profile,
      group_value = group_value,
      spec_name = spec_name,
      weight = weight,
      n_obs = nrow(dd_fit),
      n_users = dplyr::n_distinct(dd_fit$user_id),
      n_clusters = dplyr::n_distinct(dd_fit$vcov_cluster),
      n_positive = sum(dd_fit$n_patents > 0, na.rm = TRUE),
      fe_part = fe_part,
      mean_patents_0_5 = ref$mean_patents_0_5[1],
      n_ref_bin = ref$n_ref_bin[1],
      n_ref_positive = ref$n_ref_positive[1]
    ) %>%
    add_plot_scale() %>%
    arrange(spec_name, profile_source, profile, tenure_bin)
}

run_full_unsplit_ppml <- function(df_base, window_tag, sample_tag, fe_ladder) {
  validate_fe_ladder(fe_ladder)

  ts_msg("Starting full unsplit lifecycle block:",
         "window=", window_tag,
         "| sample=", sample_tag)

  dd0 <- prepare_analysis_sample(df_base)

  ts_msg("Prepared full unsplit rows:", nrow(dd0))
  ts_msg("Prepared full unsplit users:", dplyr::n_distinct(dd0$user_id))
  ts_msg("Prepared full unsplit clusters:", dplyr::n_distinct(dd0$vcov_cluster))
  ts_msg("Full unsplit tenure-bin counts:")
  print(dd0 %>% count(tenure_bin, name = "n_rows") %>% arrange(tenure_bin))

  if (nrow(dd0) == 0) return(tibble())

  out_list <- list()
  k <- 0L

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]
    for (sp in fe_ladder) {
      fit <- run_ppml(
        rhs = paste0("i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\")"),
        fe_part = sp$fe,
        data = dd0,
        wform = wform,
        cluster_var = "vcov_cluster"
      )

      k <- k + 1L
      out_list[[k]] <- make_profile_from_simple_fit(
        fit = fit,
        dd_pre = dd0,
        analysis_name = "FULL_SAMPLE",
        window_tag = window_tag,
        sample_tag = sample_tag,
        profile_source = "separate",
        profile = "full",
        group_value = "full",
        spec_name = sp$spec_name,
        weight = wtag,
        fe_part = sp$fe,
        model_tag = paste0(window_tag, "__FULL_SAMPLE__", sample_tag,
                           "__all_cohorts__full__", sp$spec_name, "__", wtag)
      )
    }
  }

  bind_rows(out_list)
}

run_separate_one_group_ppml <- function(dd0, split_var, group_value, profile_name,
                                        analysis_name, window_tag, sample_tag,
                                        fe_ladder) {
  out_list <- list()
  k <- 0L

  dd_g <- dd0 %>% filter(.data[[split_var]] == group_value)

  ts_msg("Prepared separate-group sample:",
         "analysis=", analysis_name,
         "| profile=", profile_name,
         "| group_value=", group_value,
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
      fit <- run_ppml(
        rhs = paste0("i(tenure_bin, ref = \"0-", TENURE_BIN_STEP, "\")"),
        fe_part = sp$fe,
        data = dd_g,
        wform = wform,
        cluster_var = "vcov_cluster"
      )

      k <- k + 1L
      out_list[[k]] <- make_profile_from_simple_fit(
        fit = fit,
        dd_pre = dd_g,
        analysis_name = analysis_name,
        window_tag = window_tag,
        sample_tag = sample_tag,
        profile_source = "separate",
        profile = profile_name,
        group_value = group_value,
        spec_name = sp$spec_name,
        weight = wtag,
        fe_part = sp$fe,
        model_tag = paste0(window_tag, "__", analysis_name, "__", sample_tag,
                           "__all_cohorts__separate__", profile_name, "__",
                           sp$spec_name, "__", wtag)
      )
    }
  }

  bind_rows(out_list)
}

find_coef_term <- function(term_names, tenure_level, interaction = FALSE) {
  cand <- term_names[
    grepl("tenure_bin", term_names) &
      grepl(tenure_level, term_names, fixed = TRUE)
  ]

  if (interaction) {
    cand <- cand[grepl("top20", cand, fixed = TRUE)]
  } else {
    cand <- cand[!grepl("top20", cand, fixed = TRUE)]
  }

  if (length(cand) == 0) return(NA_character_)
  cand[1]
}

lincomb <- function(beta, V, terms, weights) {
  valid <- !is.na(terms) & terms %in% names(beta)
  terms_ok <- terms[valid]
  weights_ok <- weights[valid]
  if (length(terms_ok) == 0) return(list(est = NA_real_, se = NA_real_))

  est <- sum(weights_ok * beta[terms_ok])
  Vsub <- V[terms_ok, terms_ok, drop = FALSE]
  se <- sqrt(as.numeric(t(weights_ok) %*% Vsub %*% weights_ok))
  list(est = as.numeric(est), se = as.numeric(se))
}

make_pooled_row <- function(model, term, raw_term, tenure_bin, estimate, se,
                            analysis_name, window_tag, sample_tag,
                            profile, group_value, spec_name, weight, n_obs,
                            n_users, n_clusters, n_positive, fe_part,
                            mean_patents_0_5, n_ref_bin, n_ref_positive,
                            collinear_dropped) {
  pval <- ifelse(is.finite(se) && se > 0, 2 * pnorm(abs(estimate / se), lower.tail = FALSE), NA_real_)
  conf_low <- estimate - 1.96 * se
  conf_high <- estimate + 1.96 * se

  tibble(
    model = model,
    term = term,
    raw_term = raw_term,
    tenure_bin = tenure_bin,
    estimate = estimate,
    se = se,
    stat = ifelse(is.finite(se) && se > 0, estimate / se, NA_real_),
    p = pval,
    stars = sig_stars(pval),
    collinear_dropped = collinear_dropped,
    conf_low = conf_low,
    conf_high = conf_high,
    p_label = ifelse(is.finite(pval), sprintf("p=%.3f", pval), NA_character_),
    analysis = analysis_name,
    window = window_tag,
    sample_tag = sample_tag,
    cohort_tag = "all_cohorts",
    user_fe_tag = "with_user_fe",
    profile_source = "pooled",
    profile = profile,
    group_value = group_value,
    spec_name = spec_name,
    weight = weight,
    n_obs = n_obs,
    n_users = n_users,
    n_clusters = n_clusters,
    n_positive = n_positive,
    fe_part = fe_part,
    mean_patents_0_5 = mean_patents_0_5,
    n_ref_bin = n_ref_bin,
    n_ref_positive = n_ref_positive
  )
}

run_pooled_topcomp_ppml <- function(dd0, split_var, analysis_name, window_tag,
                                    sample_tag, fe_ladder) {
  validate_fe_ladder(fe_ladder)

  dd_i <- dd0 %>%
    filter(.data[[split_var]] %in% c("complement", "top20")) %>%
    mutate(group2 = factor(.data[[split_var]], levels = c("complement", "top20")))

  ts_msg("Prepared pooled saturated sample:",
         "analysis=", analysis_name,
         "| rows=", nrow(dd_i),
         "| users=", dplyr::n_distinct(dd_i$user_id),
         "| clusters=", dplyr::n_distinct(dd_i$vcov_cluster),
         "| positives=", sum(dd_i$n_patents > 0, na.rm = TRUE))

  if (nrow(dd_i) == 0) return(tibble())

  ts_msg("Group-by-tenure counts for pooled sample:")
  print(dd_i %>% count(group2, tenure_bin, name = "n") %>% arrange(group2, tenure_bin))

  out_list <- list()
  k <- 0L
  levs <- ordered_tenure_levels()
  ref_lab <- paste0("0-", TENURE_BIN_STEP)

  rhs <- paste0(
    "group2 + ",
    "i(tenure_bin, ref = \"", ref_lab, "\") + ",
    "i(tenure_bin, group2, ref = \"", ref_lab, "\", ref2 = \"complement\")"
  )

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]

    for (sp in fe_ladder) {
      fit <- run_ppml(
        rhs = rhs,
        fe_part = sp$fe,
        data = dd_i,
        wform = wform,
        cluster_var = "vcov_cluster"
      )

      if (is.null(fit)) next

      dd_fit <- get_fit_data(fit, dd_i)
      ref_comp <- mean_ref_patents(dd_fit, "group2", "complement")
      ref_top  <- mean_ref_patents(dd_fit, "group2", "top20")
      mean_comp <- ref_comp$mean_patents_0_5[1]
      mean_top  <- ref_top$mean_patents_0_5[1]

      ct <- tryCatch(fixest::coeftable(fit), error = function(e) NULL)
      V  <- tryCatch(stats::vcov(fit), error = function(e) NULL)

      if (is.null(ct) || is.null(V)) {
        ts_msg("Could not extract coeftable/vcov for pooled model; skipping profile construction.")
        next
      }

      beta <- ct[, 1]
      names(beta) <- rownames(ct)
      term_names <- names(beta)

      ts_msg("Pooled coefficient names for debugging:")
      print(tibble(term = term_names))

      dropped <- if (!is.null(fit$collin.var) && length(fit$collin.var) > 0) {
        paste(fit$collin.var, collapse = ";")
      } else {
        ""
      }

      rows <- list()

      for (lv in levs) {
        if (lv == ref_lab) {
          main_est <- 0
          main_se <- 0
          int_est <- 0
          int_se <- 0
          top_est <- 0
          top_se <- 0
          main_term <- paste0("__REF_", ref_lab, "__")
          int_term <- paste0("__REF_INTERACTION_", ref_lab, "__")
        } else {
          main_term <- find_coef_term(term_names, lv, interaction = FALSE)
          int_term  <- find_coef_term(term_names, lv, interaction = TRUE)

          main_lc <- lincomb(beta, V, main_term, 1)
          int_lc  <- lincomb(beta, V, int_term, 1)
          top_lc  <- lincomb(beta, V, c(main_term, int_term), c(1, 1))

          main_est <- main_lc$est
          main_se  <- main_lc$se
          int_est  <- int_lc$est
          int_se   <- int_lc$se
          top_est  <- top_lc$est
          top_se   <- top_lc$se
        }

        rows[[length(rows) + 1L]] <- make_pooled_row(
          model = paste0(window_tag, "__", analysis_name, "__", sample_tag,
                         "__all_cohorts__pooled_complement__", sp$spec_name, "__", wtag),
          term = main_term,
          raw_term = main_term,
          tenure_bin = factor(lv, levels = levs, ordered = TRUE),
          estimate = main_est,
          se = main_se,
          analysis_name = analysis_name,
          window_tag = window_tag,
          sample_tag = sample_tag,
          profile = "complement",
          group_value = "complement",
          spec_name = sp$spec_name,
          weight = wtag,
          n_obs = nrow(dd_fit),
          n_users = n_distinct(dd_fit$user_id),
          n_clusters = n_distinct(dd_fit$vcov_cluster),
          n_positive = sum(dd_fit$n_patents > 0, na.rm = TRUE),
          fe_part = sp$fe,
          mean_patents_0_5 = mean_comp,
          n_ref_bin = ref_comp$n_ref_bin[1],
          n_ref_positive = ref_comp$n_ref_positive[1],
          collinear_dropped = dropped
        )

        rows[[length(rows) + 1L]] <- make_pooled_row(
          model = paste0(window_tag, "__", analysis_name, "__", sample_tag,
                         "__all_cohorts__pooled_top__", sp$spec_name, "__", wtag),
          term = paste0(main_term, " + ", int_term),
          raw_term = paste0(main_term, " + ", int_term),
          tenure_bin = factor(lv, levels = levs, ordered = TRUE),
          estimate = top_est,
          se = top_se,
          analysis_name = analysis_name,
          window_tag = window_tag,
          sample_tag = sample_tag,
          profile = "top",
          group_value = "top20",
          spec_name = sp$spec_name,
          weight = wtag,
          n_obs = nrow(dd_fit),
          n_users = n_distinct(dd_fit$user_id),
          n_clusters = n_distinct(dd_fit$vcov_cluster),
          n_positive = sum(dd_fit$n_patents > 0, na.rm = TRUE),
          fe_part = sp$fe,
          mean_patents_0_5 = mean_top,
          n_ref_bin = ref_top$n_ref_bin[1],
          n_ref_positive = ref_top$n_ref_positive[1],
          collinear_dropped = dropped
        )

        rows[[length(rows) + 1L]] <- make_pooled_row(
          model = paste0(window_tag, "__", analysis_name, "__", sample_tag,
                         "__all_cohorts__pooled_interaction__", sp$spec_name, "__", wtag),
          term = int_term,
          raw_term = int_term,
          tenure_bin = factor(lv, levels = levs, ordered = TRUE),
          estimate = int_est,
          se = int_se,
          analysis_name = analysis_name,
          window_tag = window_tag,
          sample_tag = sample_tag,
          profile = "interaction",
          group_value = "top20_minus_complement",
          spec_name = sp$spec_name,
          weight = wtag,
          n_obs = nrow(dd_fit),
          n_users = n_distinct(dd_fit$user_id),
          n_clusters = n_distinct(dd_fit$vcov_cluster),
          n_positive = sum(dd_fit$n_patents > 0, na.rm = TRUE),
          fe_part = sp$fe,
          mean_patents_0_5 = mean_top - mean_comp,
          n_ref_bin = min(ref_top$n_ref_bin[1], ref_comp$n_ref_bin[1]),
          n_ref_positive = min(ref_top$n_ref_positive[1], ref_comp$n_ref_positive[1]),
          collinear_dropped = dropped
        )
      }

      pooled_tab <- bind_rows(rows) %>%
        mutate(
          plot_estimate = case_when(
            profile == "interaction" ~ NA_real_,
            TRUE ~ mean_patents_0_5 * exp(estimate)
          ),
          plot_low = case_when(
            profile == "interaction" ~ NA_real_,
            TRUE ~ mean_patents_0_5 * exp(conf_low)
          ),
          plot_high = case_when(
            profile == "interaction" ~ NA_real_,
            TRUE ~ mean_patents_0_5 * exp(conf_high)
          )
        )

      for (rr in seq_len(nrow(pooled_tab))) {
        if (pooled_tab$profile[rr] != "interaction") next
        lv <- as.character(pooled_tab$tenure_bin[rr])

        if (lv == ref_lab) {
          pooled_tab$plot_estimate[rr] <- mean_top - mean_comp
          pooled_tab$plot_low[rr] <- mean_top - mean_comp
          pooled_tab$plot_high[rr] <- mean_top - mean_comp
          next
        }

        main_term <- find_coef_term(term_names, lv, interaction = FALSE)
        int_term  <- find_coef_term(term_names, lv, interaction = TRUE)

        if (is.na(main_term) || is.na(int_term) || !(main_term %in% rownames(V)) || !(int_term %in% rownames(V))) next

        main_est <- beta[main_term]
        int_est <- beta[int_term]
        fval <- mean_top * exp(main_est + int_est) - mean_comp * exp(main_est)

        grad <- c(
          mean_top * exp(main_est + int_est) - mean_comp * exp(main_est),
          mean_top * exp(main_est + int_est)
        )
        names(grad) <- c(main_term, int_term)

        Vsub <- V[c(main_term, int_term), c(main_term, int_term), drop = FALSE]
        se_f <- sqrt(as.numeric(t(grad) %*% Vsub %*% grad))

        pooled_tab$plot_estimate[rr] <- fval
        pooled_tab$plot_low[rr] <- fval - 1.96 * se_f
        pooled_tab$plot_high[rr] <- fval + 1.96 * se_f
      }

      k <- k + 1L
      out_list[[k]] <- pooled_tab
    }
  }

  bind_rows(out_list)
}

run_topcomp_block <- function(df_base, split_var, analysis_name, window_tag,
                              sample_tag, fe_ladder) {
  validate_fe_ladder(fe_ladder)

  ts_msg("Starting top/complement lifecycle block:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| split_var=", split_var,
         "| sample=", sample_tag)

  dd0 <- df_base %>%
    filter(.data[[split_var]] %in% c("top20", "complement")) %>%
    prepare_analysis_sample()

  ts_msg("Prepared analysis rows:", nrow(dd0))
  ts_msg("Prepared users:", dplyr::n_distinct(dd0$user_id))
  ts_msg("Prepared clusters:", dplyr::n_distinct(dd0$vcov_cluster))

  if (nrow(dd0) == 0) return(tibble())

  print_count_by(dd0, split_var, "Group counts after PPML filters:")

  ts_msg("Group-by-tenure counts:")
  print(
    dd0 %>%
      group_by(group = .data[[split_var]], tenure_bin) %>%
      summarise(n_rows = n(), n_users = n_distinct(user_id),
                n_positive = sum(n_patents > 0, na.rm = TRUE), .groups = "drop") %>%
      arrange(group, tenure_bin)
  )

  top_tab <- run_separate_one_group_ppml(
    dd0 = dd0,
    split_var = split_var,
    group_value = "top20",
    profile_name = "top",
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    fe_ladder = fe_ladder
  )

  comp_tab <- run_separate_one_group_ppml(
    dd0 = dd0,
    split_var = split_var,
    group_value = "complement",
    profile_name = "complement",
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    fe_ladder = fe_ladder
  )

  pooled_tab <- run_pooled_topcomp_ppml(
    dd0 = dd0,
    split_var = split_var,
    analysis_name = analysis_name,
    window_tag = window_tag,
    sample_tag = sample_tag,
    fe_ladder = fe_ladder
  )

  bind_rows(top_tab, comp_tab, pooled_tab)
}

run_fe_decomposition <- function(df_window, analysis_name, window_tag, sample_tag) {
  ts_msg("Running FE decomposition:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| sample=", sample_tag)

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
    cohort_tag = "all_cohorts"
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
    paste0(window_tag, "__", analysis_name, "__", sample_tag, "__mp_fe_decomp_ols")
  ) %>%
    mutate(
      analysis = analysis_name,
      window = window_tag,
      sample_tag = sample_tag,
      cohort_tag = "all_cohorts",
      spec_name = "mp_fe_decomp_ols",
      n_obs = nrow(dd_fe),
      n_users = dplyr::n_distinct(dd_fe$user_id)
    )

  list(mp_fixef = mp_fixef, fe_decomp = fe_decomp)
}

filter_plot_data <- function(profile_table, window_tag, analysis_name, sample_tag,
                             profile_source, profile) {
  if (nrow(profile_table) == 0 || !("window" %in% names(profile_table))) return(tibble())

  profile_table %>%
    filter(
      .data$window == .env$window_tag,
      .data$analysis == .env$analysis_name,
      .data$sample_tag == .env$sample_tag,
      .data$cohort_tag == "all_cohorts",
      .data$profile_source == .env$profile_source,
      .data$profile == .env$profile,
      .data$weight == "unweighted",
      .data$spec_name %in% .env$ALLOWED_SPECS_WITH_USER_FE
    ) %>%
    mutate(
      spec_name = factor(spec_name, levels = ALLOWED_SPECS_WITH_USER_FE),
      tenure_bin = factor(as.character(tenure_bin), levels = ordered_tenure_levels(), ordered = TRUE)
    ) %>%
    arrange(spec_name, tenure_bin)
}

shared_ylim_for_family <- function(profile_table, window_tag, analysis_name, sample_tag,
                                   profile_source, profiles) {
  tab <- profile_table %>%
    filter(
      .data$window == .env$window_tag,
      .data$analysis == .env$analysis_name,
      .data$sample_tag == .env$sample_tag,
      .data$cohort_tag == "all_cohorts",
      .data$profile_source == .env$profile_source,
      .data$profile %in% .env$profiles,
      .data$weight == "unweighted"
    )

  vals <- c(tab$plot_estimate, tab$plot_low, tab$plot_high)
  vals <- vals[is.finite(vals)]
  if (length(vals) == 0) return(NULL)

  lo <- min(vals, na.rm = TRUE)
  hi <- max(vals, na.rm = TRUE)
  pad <- 0.05 * max(1e-8, hi - lo)
  ylim <- c(lo - pad, hi + pad)

  ts_msg("Shared y-axis:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| sample=", sample_tag,
         "| profile_source=", profile_source,
         "| profiles=", paste(profiles, collapse = ","),
         "| ymin=", round(ylim[1], 4),
         "| ymax=", round(ylim[2], 4))
  ylim
}

plot_profile_core <- function(tab, window_tag, analysis_name, profile_source,
                              profile, sample_tag, ylim = NULL) {
  if (nrow(tab) == 0) return(NULL)

  title_stub <- switch(profile_source, separate = "Separate PPML", pooled = "Pooled saturated PPML", profile_source)

  ylab <- switch(profile,
    full = "Mean patents, anchored at observed 0-5 mean",
    top = "Top20 mean patents, anchored at observed 0-5 mean",
    complement = "Complement mean patents, anchored at observed 0-5 mean",
    interaction = "Top20 minus complement level-profile difference",
    "Mean patents"
  )

  p <- ggplot(tab, aes(x = tenure_bin, y = plot_estimate, color = spec_name, group = spec_name)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray60") +
    geom_line(position = position_dodge(width = 0.35), linewidth = 0.7, alpha = 0.95) +
    geom_errorbar(aes(ymin = plot_low, ymax = plot_high),
                  width = 0.12, alpha = 0.7,
                  position = position_dodge(width = 0.35), linewidth = 0.55) +
    geom_point(position = position_dodge(width = 0.35),
               size = 2.6, shape = 21, fill = "white", stroke = 0.8) +
    labs(
      title = paste0(title_stub, " ", profile, " lifecycle profile — ", analysis_name, " — ", window_tag),
      subtitle = paste0("sample=", sample_tag, " | all cohorts | with_user_fe"),
      x = "Tenure bin",
      y = ylab,
      color = "Specification",
      caption = "PPML log-link estimates plotted as mean(0-5) * exp(beta). SEs clustered by metro-parent cell."
    ) +
    theme_minimal(base_size = 12) +
    theme(axis.text.x = element_text(angle = 45, hjust = 1), legend.position = "bottom")

  if (!is.null(ylim)) p <- p + coord_cartesian(ylim = ylim)
  p
}

save_lifecycle_plots_one_family <- function(profile_all, window_tag, analysis_name, sample_tag) {
  if (nrow(profile_all) == 0) return(invisible(NULL))

  if (analysis_name == "FULL_SAMPLE") {
    tab_full <- filter_plot_data(profile_all, window_tag, "FULL_SAMPLE", sample_tag, "separate", "full")
    if (nrow(tab_full) > 0) {
      ylim_full <- shared_ylim_for_family(profile_all, window_tag, "FULL_SAMPLE", sample_tag, "separate", c("full"))
      p_full <- plot_profile_core(tab_full, window_tag, "FULL_SAMPLE", "separate", "full", sample_tag, ylim_full)
      if (!is.null(p_full)) {
        save_plot(
          p_full,
          file.path(
            dirs$plots,
            paste0(window_tag, "__FULL_SAMPLE__", safe_name(sample_tag), "__all_cohorts__full.png")
          )
        )
      }
    }
    return(invisible(NULL))
  }

  ylim_sep <- shared_ylim_for_family(profile_all, window_tag, analysis_name, sample_tag, "separate", c("top", "complement"))
  for (prof in c("top", "complement")) {
    tab <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "separate", prof)
    if (nrow(tab) > 0) {
      p <- plot_profile_core(tab, window_tag, analysis_name, "separate", prof, sample_tag, ylim_sep)
      if (!is.null(p)) {
        save_plot(
          p,
          file.path(
            dirs$plots,
            paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                   "__all_cohorts__separate__", prof, ".png")
          )
        )
      }
    }
  }

  ylim_pool <- shared_ylim_for_family(profile_all, window_tag, analysis_name, sample_tag, "pooled", c("top", "complement"))
  for (prof in c("top", "complement")) {
    tab <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "pooled", prof)
    if (nrow(tab) > 0) {
      p <- plot_profile_core(tab, window_tag, analysis_name, "pooled", prof, sample_tag, ylim_pool)
      if (!is.null(p)) {
        save_plot(
          p,
          file.path(
            dirs$plots,
            paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                   "__all_cohorts__pooled__", prof, ".png")
          )
        )
      }
    }
  }

  ylim_int <- shared_ylim_for_family(profile_all, window_tag, analysis_name, sample_tag, "pooled", c("interaction"))
  tab_int <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "pooled", "interaction")
  if (nrow(tab_int) > 0) {
    p_int <- plot_profile_core(tab_int, window_tag, analysis_name, "pooled", "interaction", sample_tag, ylim_int)
    if (!is.null(p_int)) {
      save_plot(
        p_int,
        file.path(
          dirs$plots,
          paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                 "__all_cohorts__pooled__interaction.png")
        )
      )
    }
  }
}


shared_log_ylim_for_family <- function(profile_table, window_tag, analysis_name, sample_tag,
                                       profile_source, profiles) {
  tab <- profile_table %>%
    filter(
      .data$window == .env$window_tag,
      .data$analysis == .env$analysis_name,
      .data$sample_tag == .env$sample_tag,
      .data$cohort_tag == "all_cohorts",
      .data$profile_source == .env$profile_source,
      .data$profile %in% .env$profiles,
      .data$weight == "unweighted"
    )

  vals <- c(tab$estimate, tab$conf_low, tab$conf_high)
  vals <- vals[is.finite(vals)]
  if (length(vals) == 0) return(NULL)

  lo <- min(vals, na.rm = TRUE)
  hi <- max(vals, na.rm = TRUE)
  pad <- 0.05 * max(1e-8, hi - lo)
  ylim <- c(lo - pad, hi + pad)

  ts_msg("Shared log-coefficient y-axis:",
         "window=", window_tag,
         "| analysis=", analysis_name,
         "| sample=", sample_tag,
         "| profile_source=", profile_source,
         "| profiles=", paste(profiles, collapse = ","),
         "| ymin=", round(ylim[1], 4),
         "| ymax=", round(ylim[2], 4))

  ylim
}

plot_log_profile_core <- function(tab, window_tag, analysis_name, profile_source,
                                  profile, sample_tag, ylim = NULL) {
  if (nrow(tab) == 0) return(NULL)

  title_stub <- switch(
    profile_source,
    separate = "Separate PPML",
    pooled = "Pooled saturated PPML",
    profile_source
  )

  ylab <- switch(
    profile,
    full = "PPML coefficient relative to tenure 0-5",
    top = "Top20 PPML coefficient relative to tenure 0-5",
    complement = "Complement PPML coefficient relative to tenure 0-5",
    interaction = "Top20 minus complement log-coefficient difference",
    "PPML coefficient relative to tenure 0-5"
  )

  p <- ggplot(tab, aes(x = tenure_bin, y = estimate, color = spec_name, group = spec_name)) +
    geom_hline(yintercept = 0, linewidth = 0.3, color = "gray60") +
    geom_line(position = position_dodge(width = 0.35), linewidth = 0.7, alpha = 0.95) +
    geom_errorbar(
      aes(ymin = conf_low, ymax = conf_high),
      width = 0.12,
      alpha = 0.7,
      position = position_dodge(width = 0.35),
      linewidth = 0.55
    ) +
    geom_point(
      position = position_dodge(width = 0.35),
      size = 2.6,
      shape = 21,
      fill = "white",
      stroke = 0.8
    ) +
    labs(
      title = paste0(title_stub, " ", profile, " lifecycle profile, log coefficients — ", analysis_name, " — ", window_tag),
      subtitle = paste0("sample=", sample_tag, " | all cohorts | omitted bin: tenure 0-5"),
      x = "Tenure bin",
      y = ylab,
      color = "Specification",
      caption = "PPML log-link coefficients. Standard errors clustered by metro-parent cell."
    ) +
    theme_minimal(base_size = 12) +
    theme(
      axis.text.x = element_text(angle = 45, hjust = 1),
      legend.position = "bottom"
    )

  if (!is.null(ylim)) p <- p + coord_cartesian(ylim = ylim)
  p
}

save_log_lifecycle_plots_one_family <- function(profile_all, window_tag, analysis_name, sample_tag) {
  if (nrow(profile_all) == 0) return(invisible(NULL))

  log_plot_dir <- file.path(dirs$plots, "log_coefficients")
  dir.create(log_plot_dir, recursive = TRUE, showWarnings = FALSE)

  if (analysis_name == "FULL_SAMPLE") {
    tab_full <- filter_plot_data(profile_all, window_tag, "FULL_SAMPLE", sample_tag, "separate", "full")

    if (nrow(tab_full) > 0) {
      ylim_full <- shared_log_ylim_for_family(
        profile_all, window_tag, "FULL_SAMPLE", sample_tag, "separate", c("full")
      )

      p_full <- plot_log_profile_core(
        tab_full, window_tag, "FULL_SAMPLE", "separate", "full", sample_tag, ylim_full
      )

      if (!is.null(p_full)) {
        save_plot(
          p_full,
          file.path(
            log_plot_dir,
            paste0(window_tag, "__FULL_SAMPLE__", safe_name(sample_tag),
                   "__all_cohorts__separate__full__logcoef.png")
          )
        )
      }
    }

    return(invisible(NULL))
  }

  ylim_sep <- shared_log_ylim_for_family(
    profile_all, window_tag, analysis_name, sample_tag, "separate", c("top", "complement")
  )

  for (prof in c("top", "complement")) {
    tab <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "separate", prof)

    if (nrow(tab) > 0) {
      p <- plot_log_profile_core(
        tab, window_tag, analysis_name, "separate", prof, sample_tag, ylim_sep
      )

      if (!is.null(p)) {
        save_plot(
          p,
          file.path(
            log_plot_dir,
            paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                   "__all_cohorts__separate__", prof, "__logcoef.png")
          )
        )
      }
    }
  }

  ylim_pool <- shared_log_ylim_for_family(
    profile_all, window_tag, analysis_name, sample_tag, "pooled", c("top", "complement")
  )

  for (prof in c("top", "complement")) {
    tab <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "pooled", prof)

    if (nrow(tab) > 0) {
      p <- plot_log_profile_core(
        tab, window_tag, analysis_name, "pooled", prof, sample_tag, ylim_pool
      )

      if (!is.null(p)) {
        save_plot(
          p,
          file.path(
            log_plot_dir,
            paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                   "__all_cohorts__pooled__", prof, "__logcoef.png")
          )
        )
      }
    }
  }

  ylim_int <- shared_log_ylim_for_family(
    profile_all, window_tag, analysis_name, sample_tag, "pooled", c("interaction")
  )

  tab_int <- filter_plot_data(profile_all, window_tag, analysis_name, sample_tag, "pooled", "interaction")

  if (nrow(tab_int) > 0) {
    p_int <- plot_log_profile_core(
      tab_int, window_tag, analysis_name, "pooled", "interaction", sample_tag, ylim_int
    )

    if (!is.null(p_int)) {
      save_plot(
        p_int,
        file.path(
          log_plot_dir,
          paste0(window_tag, "__", safe_name(analysis_name), "__", safe_name(sample_tag),
                 "__all_cohorts__pooled__interaction__logcoef.png")
        )
      )
    }
  }

  invisible(NULL)
}

save_growth_decomposition <- function(profile_all, tag = "chunk") {
  if (nrow(profile_all) == 0) return(invisible(NULL))

  growth <- profile_all %>%
    filter(
      analysis == "FULL_SAMPLE",
      sample_tag == "full_sample",
      cohort_tag == "all_cohorts",
      profile_source == "separate",
      profile == "full",
      weight == "unweighted",
      spec_name %in% ALLOWED_SPECS_WITH_USER_FE
    ) %>%
    mutate(
      tenure_bin = as.character(tenure_bin),
      spec_name = factor(spec_name, levels = ALLOWED_SPECS_WITH_USER_FE)
    ) %>%
    group_by(window, spec_name) %>%
    summarise(
      ref_level = plot_estimate[tenure_bin == paste0("0-", TENURE_BIN_STEP)][1],
      peak_level = max(plot_estimate, na.rm = TRUE),
      peak_bin = tenure_bin[which.max(plot_estimate)],
      lifecycle_growth = peak_level - ref_level,
      ref_mean = first(mean_patents_0_5),
      n_ref_bin = first(n_ref_bin),
      n_obs = first(n_obs),
      n_users = first(n_users),
      n_clusters = first(n_clusters),
      .groups = "drop"
    ) %>%
    group_by(window) %>%
    mutate(
      baseline_growth = lifecycle_growth[spec_name == "baseline_user_year"][1],
      share_explained_vs_baseline = 1 - lifecycle_growth / baseline_growth
    ) %>%
    ungroup()

  out <- file.path(dirs$tables, paste0(tag, "__full_sample_lifecycle_growth_decomposition.csv"))
  write_csv(growth, out)
  ts_msg("Saved lifecycle-growth decomposition:", out)
}

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
    !!PARENT_VAR := str_trim(as.character(.data[[PARENT_VAR]])),
    !!METRO_VAR  := str_trim(as.character(.data[[METRO_VAR]])),
    !!GENDER_VAR := str_trim(as.character(.data[[GENDER_VAR]]))
  ) %>%
  mutate(
    country_year = dplyr::coalesce(na_if(last_country, ""), na_if(first_country, ""))
  ) %>%
  filter(!is.na(country_year), country_year == US_COUNTRY) %>%
  compute_tenure() %>%
  make_reference_year() %>%
  mutate(
    gender_std = case_when(.data[[GENDER_VAR]] %in% c("Male", "Female") ~ .data[[GENDER_VAR]], TRUE ~ NA_character_),
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
  ) %>%
  filter(!is.na(last_university_country), last_university_country == US_COUNTRY)

rm(ds)
gc()

ts_msg("Loaded rows in df_full:", nrow(df_full))
ts_msg("Loaded users in df_full:", dplyr::n_distinct(df_full$user_id))

inventor_ref <- df_full %>%
  arrange(user_id, year) %>%
  group_by(user_id) %>%
  summarise(
    gender_std      = first_nonmissing_chr(gender_std),
    ref_year        = first_nonmissing_num(ref_year),
    first_metro     = first_nonmissing_chr(metro_fe_id),
    last_metro      = last_nonmissing_chr(metro_fe_id),
    n_metros_obs    = n_distinct(metro_fe_id[!is.na(metro_fe_id) & metro_fe_id != ""]),
    first_mp        = first_nonmissing_chr(mp_fe_id),
    last_mp         = last_nonmissing_chr(mp_fe_id),
    n_mps_obs       = n_distinct(mp_fe_id[!is.na(mp_fe_id) & mp_fe_id != ""]),
    avg_pat_career  = mean(n_patents[is.finite(tenure)], na.rm = TRUE),
    career_obs_all  = sum(is.finite(tenure) & !is.na(n_patents)),
    .groups = "drop"
  ) %>%
  mutate(
    avg_pat_career = ifelse(is.nan(avg_pat_career), NA_real_, avg_pat_career),
    same_start_end_metro = !is.na(first_metro) & !is.na(last_metro) & first_metro == last_metro,
    never_moved_metro = is.finite(n_metros_obs) & n_metros_obs == 1L,
    same_start_end_mp = !is.na(first_mp) & !is.na(last_mp) & first_mp == last_mp,
    never_moved_mp = is.finite(n_mps_obs) & n_mps_obs == 1L,
    career_topcomp = case_when(
      is.na(career_obs_all) | career_obs_all < 3 ~ NA_character_,
      TRUE ~ make_top_complement_split(avg_pat_career, p = TOP_P)
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

density_lookup <- read_density_crosswalk(DENSITY_CROSSWALK)

inventor_ref <- inventor_ref %>%
  left_join(metro_stats_2000 %>% rename(first_metro = metro_fe_id), by = "first_metro") %>%
  left_join(density_lookup, by = "first_metro")

density_inventor_diag <- inventor_ref %>%
  summarise(
    n_users = n(),
    n_users_with_valid_first_metro_density = sum(first_metro_has_valid_density == TRUE, na.rm = TRUE),
    share_users_with_valid_first_metro_density =
      n_users_with_valid_first_metro_density / n_users,
    n_users_top20_density = sum(metro_density_topcomp == "top20", na.rm = TRUE),
    n_users_complement_density = sum(metro_density_topcomp == "complement", na.rm = TRUE),
    n_users_missing_density_split = sum(is.na(metro_density_topcomp))
  )

ts_msg("Inventor-level first-metro density diagnostics:")
print(density_inventor_diag)

write_csv(
  density_inventor_diag,
  file.path(dirs$diagnostics, "inventor_level_first_metro_density_diagnostics.csv")
)

write_csv(
  inventor_ref %>%
    count(metro_density_topcomp, name = "n_users") %>%
    arrange(metro_density_topcomp),
  file.path(dirs$diagnostics, "inventor_level_first_metro_density_split_counts.csv")
)

write_csv(inventor_ref, file.path(dirs$diagnostics, "inventor_level_variables.csv"))

df_master <- df_full %>%
  left_join(
    inventor_ref %>%
select(user_id, career_topcomp,
       first_metro, first_mp,
       metro_size_2000, metro_prod_2000,
       metro_size_topcomp, metro_prod_topcomp,
       first_metro_cbsa_geoid,
       first_metro_cbsa_name,
       first_metro_population_2023_acs5,
       first_metro_land_area_sqmi,
       first_metro_land_area_sqkm,
       first_metro_density_per_sqmi,
       first_metro_density_per_sqkm,
       first_metro_log_density_per_sqmi,
       first_metro_log_density_per_sqkm,
       first_metro_has_valid_density,
       first_metro_density_match_source,
       first_metro_density_match_confidence,
       first_metro_density_match_note,
       metro_density_topcomp,
       same_start_end_metro, never_moved_metro,
       same_start_end_mp, never_moved_mp) %>%
      distinct(user_id, .keep_all = TRUE),
    by = "user_id"
  )

sample_defs <- list(
  list(sample_tag = "full_sample", expr = NULL),
  list(sample_tag = "same_start_end_metro", expr = "same_start_end_metro == TRUE"),
  list(sample_tag = "never_moved_metro", expr = "never_moved_metro == TRUE"),
  list(sample_tag = "same_start_end_mp", expr = "same_start_end_mp == TRUE"),
  list(sample_tag = "never_moved_mp", expr = "never_moved_mp == TRUE")
)

analysis_defs <- list(
  list(name = "CAREER_TOPCOMP", split_var = "career_topcomp"),
  list(name = "FIRSTMETRO_SIZE_TOPCOMP", split_var = "metro_size_topcomp"),
  list(name = "FIRSTMETRO_PROD_TOPCOMP", split_var = "metro_prod_topcomp"),
  list(name = "FIRSTMETRO_DENSITY_TOPCOMP", split_var = "metro_density_topcomp")
)

selected_windows <- intersect(names(WINDOWS), parse_windows_env(WINDOW_TAGS_ENV))
if (length(selected_windows) == 0) stop("[ERROR] No valid windows selected.")

chunk_analysis_defs <- function(chunk_mode) {
  switch(
    chunk_mode,
    career = analysis_defs[1],
    metro_size = analysis_defs[2],
    metro_prod = analysis_defs[3],
    density = analysis_defs[4],
    fe_decomp = analysis_defs,
    all = analysis_defs,
    stop(paste0("[ERROR] Unknown CHUNK_MODE: ", chunk_mode))
  )
}

active_analysis_defs <- chunk_analysis_defs(CHUNK_MODE)
RUN_LIFECYCLE_PROFILES <- !identical(CHUNK_MODE, "fe_decomp")
RUN_FULL_UNSPLIT <- CHUNK_MODE %in% c("all", "career")

ts_msg("Selected windows:", paste(selected_windows, collapse = ", "))
ts_msg("Chunk mode:", CHUNK_MODE)
ts_msg("RUN_LIFECYCLE_PROFILES:", RUN_LIFECYCLE_PROFILES)
ts_msg("RUN_FULL_UNSPLIT:", RUN_FULL_UNSPLIT)
ts_msg("RUN_FE_DECOMP:", RUN_FE_DECOMP)
ts_msg("RUN_NO_USER_FE exported but ignored:", RUN_NO_USER_FE_EXPORTED_BUT_IGNORED)
ts_msg("RUN_COHORTS exported but ignored:", RUN_COHORTS_EXPORTED_BUT_IGNORED)
ts_msg("Output directory:", OUT_DIR)

profile_results <- list()
fe_decomp_results <- list()
mp_fixef_results <- list()
support_diag_results <- list()

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

    if (isTRUE(RUN_FULL_UNSPLIT)) {
      dd_diag_full <- prepare_analysis_sample(df_s)
      support_diag_results[[length(support_diag_results) + 1L]] <- make_support_diagnostics(
        dd_diag_full, "FULL_SAMPLE", NULL, window_tag, sd$sample_tag
      )

      if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
        profile_results[[length(profile_results) + 1L]] <- run_full_unsplit_ppml(
          df_s,
          window_tag = window_tag,
          sample_tag = sd$sample_tag,
          fe_ladder = fe_ladder_user
        )
      }
    }

    for (ad in active_analysis_defs) {
      ts_msg("Processing analysis:", ad$name, "| split_var:", ad$split_var)
      print_count_by(df_s, ad$split_var, "Raw split counts before PPML filters:")

      dd_diag <- df_s %>%
        filter(.data[[ad$split_var]] %in% c("top20", "complement")) %>%
        prepare_analysis_sample()

      support_diag_results[[length(support_diag_results) + 1L]] <- make_support_diagnostics(
        dd_diag, ad$name, ad$split_var, window_tag, sd$sample_tag
      )

      if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
        profile_results[[length(profile_results) + 1L]] <- run_topcomp_block(
          df_s, ad$split_var, ad$name, window_tag,
          sample_tag = sd$sample_tag,
          fe_ladder = fe_ladder_user
        )
      } else {
        ts_msg("Skipping lifecycle PPML profiles because CHUNK_MODE=fe_decomp.")
      }

      if (isTRUE(RUN_FE_DECOMP)) {
        fe_tmp <- run_fe_decomposition(
          df_s %>% filter(.data[[ad$split_var]] %in% c("top20", "complement")),
          ad$name, window_tag,
          sample_tag = sd$sample_tag
        )
        fe_decomp_results[[length(fe_decomp_results) + 1L]] <- fe_tmp$fe_decomp
        mp_fixef_results[[length(mp_fixef_results) + 1L]] <- fe_tmp$mp_fixef
      }
    }
  }
}

profile_all <- bind_rows(profile_results)
fe_decomp_all <- bind_rows(fe_decomp_results)
mp_fixef_all <- bind_rows(mp_fixef_results)
support_diag_all <- bind_rows(support_diag_results)

ts_msg("Final profile rows:", nrow(profile_all))
ts_msg("Final FE decomp rows:", nrow(fe_decomp_all))
ts_msg("Final MP fixef rows:", nrow(mp_fixef_all))
ts_msg("Final support diagnostic rows:", nrow(support_diag_all))

chunk_prefix <- paste0(
  "chunk__", safe_name(CHUNK_MODE),
  "__windows__", safe_name(paste(selected_windows, collapse = "__"))
)

if (nrow(profile_all) > 0) {
  out_profile <- file.path(dirs$chunk_tables, paste0(chunk_prefix, "__lifecycle_profiles_all.csv"))
  out_compat <- file.path(dirs$chunk_tables, paste0(chunk_prefix, "__joint_topcomp_profiles_all.csv"))

  write_csv(profile_all, out_profile)
  write_csv(profile_all, out_compat)

  ts_msg("Saved lifecycle profile table:", out_profile)
  ts_msg("Saved compatibility lifecycle profile table:", out_compat)

  ref_check <- profile_all %>%
    group_by(window, analysis, sample_tag, cohort_tag, profile_source, profile, spec_name, group_value) %>%
    summarise(
      n_rows = n(),
      has_zero_bin = any(as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)),
      n_zero_bin = sum(as.character(tenure_bin) == paste0("0-", TENURE_BIN_STEP)),
      mean_patents_0_5 = first(mean_patents_0_5),
      n_ref_bin = first(n_ref_bin),
      n_ref_positive = first(n_ref_positive),
      low_ref_support = first(n_ref_bin) < 100,
      .groups = "drop"
    )

  out_ref <- file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__reference_bin_check.csv"))
  write_csv(ref_check, out_ref)
  ts_msg("Saved reference-bin check:", out_ref)

  save_growth_decomposition(profile_all, tag = chunk_prefix)
}

if (nrow(support_diag_all) > 0) {
  out_supp <- file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__support_diagnostics.csv"))
  write_csv(support_diag_all, out_supp)
  ts_msg("Saved support diagnostics:", out_supp)
}

if (nrow(fe_decomp_all) > 0) {
  out_fe <- file.path(dirs$chunk_tables, paste0(chunk_prefix, "__fe_decomposition_all.csv"))
  write_csv(fe_decomp_all, out_fe)
  ts_msg("Saved FE decomposition table:", out_fe)
}

if (nrow(mp_fixef_all) > 0) {
  out_mp <- file.path(dirs$chunk_tables, paste0(chunk_prefix, "__mp_fixef_all.csv"))
  write_csv(mp_fixef_all, out_mp)
  ts_msg("Saved metro-parent fixed-effect table:", out_mp)
}

if (isTRUE(RUN_LIFECYCLE_PROFILES)) {
  for (window_tag in selected_windows) {
    for (sd in sample_defs) {
      if (isTRUE(RUN_FULL_UNSPLIT)) {
        save_lifecycle_plots_one_family(
          profile_all, window_tag, "FULL_SAMPLE",
          sample_tag = sd$sample_tag
        )

        save_log_lifecycle_plots_one_family(
          profile_all, window_tag, "FULL_SAMPLE",
          sample_tag = sd$sample_tag
        )
      }

      for (ad in active_analysis_defs) {
        save_lifecycle_plots_one_family(
          profile_all, window_tag, ad$name,
          sample_tag = sd$sample_tag
        )

        save_log_lifecycle_plots_one_family(
          profile_all, window_tag, ad$name,
          sample_tag = sd$sample_tag
        )
      }
    }
  }
}

manifest_path <- file.path(dirs$chunk_diagnostics, paste0(chunk_prefix, "__manifest.csv"))
write_csv(
  tibble(
    script = "ppml_tenure_ladder_main_revised_nocohorts.R",
    chunk_mode = CHUNK_MODE,
    windows = paste(selected_windows, collapse = ","),
    run_no_user_fe_exported_but_ignored = RUN_NO_USER_FE_EXPORTED_BUT_IGNORED,
    run_cohorts_exported_but_ignored = RUN_COHORTS_EXPORTED_BUT_IGNORED,
    run_fe_decomp = RUN_FE_DECOMP,
    run_lifecycle_profiles = RUN_LIFECYCLE_PROFILES,
    run_full_unsplit = RUN_FULL_UNSPLIT,
    profile_design = "full_unsplit_once_in_career_chunk; separate_top20; separate_complement; pooled_saturated_top_complement_interaction",
    ppml_plot_scale = "level_scale_mean_ref_times_exp_beta",
    n_profile_rows = nrow(profile_all),
    n_fe_rows = nrow(fe_decomp_all),
    n_mp_fixef_rows = nrow(mp_fixef_all),
    n_support_diag_rows = nrow(support_diag_all),
    output_dir = OUT_DIR
  ),
  manifest_path
)

ts_msg("Saved manifest:", manifest_path)
ts_msg("DONE. Outputs in:", OUT_DIR)
