#!/usr/bin/env Rscript

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

pkgs <- c("arrow", "data.table", "ggplot2", "dplyr", "readr", "stringr", "scales", "fixest")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(ggplot2)
  library(dplyr)
  library(readr)
  library(stringr)
  library(scales)
  library(fixest)
})

options(arrow.skip_nul = TRUE)
options(bitmapType = "cairo")
set.seed(123)
data.table::setDTthreads(1)
if ("setFixest_nthreads" %in% getNamespaceExports("fixest")) {
  fixest::setFixest_nthreads(1)
}

# =========================================================
# Configuration
# =========================================================
CONFIG <- list(
  input_dir = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2",
  out_dir   = "/home/epiga/revelio_labs/output/lifecycle_extended_ppml_fe_v1",
  year_min  = 2010L,
  year_max  = 2019L,
  us_only   = TRUE,
  us_country = "United States",
  tenure_max = 50L,
  cohort_start = 1981L,
  cohort_last_start = 2011L,
  cohort_last_end = 2019L,
  cohort_last_label = "2011-2019",
  cohort_last_max_tenure = 15L,
  min_people_cohort_tenure = 50L,
  cohort_width = 5L,
  keep_sections = LETTERS[1:8],
  min_obs_per_cohort_field = 50L,
  min_patents_cohort_tenure = 20L,
  run_leave_one_out = TRUE,
  run_combined_pdf = TRUE,
  run_ppml_fe = TRUE,
  ppml_min_cluster_year_size = 5L
)

# =========================================================
# Utilities
# =========================================================
log_msg <- function(...) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))
}

ensure_dir <- function(path) dir.create(path, recursive = TRUE, showWarnings = FALSE)

null_if_empty <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NULL", "N/A")] <- NA_character_
  x
}

extract_year <- function(x) suppressWarnings(as.integer(substr(as.character(x), 1, 4)))

safe_uniqueN <- function(x) uniqueN(x[!is.na(x)])

valid_tenure <- function(x, max_tenure) {
  out <- suppressWarnings(as.numeric(x))
  out[is.na(out) | out < 0 | out > max_tenure] <- NA_real_
  out
}

cum_distinct_count <- function(x) {
  x <- as.character(x)
  x[trimws(x) == ""] <- NA_character_
  out <- integer(length(x))
  seen <- new.env(hash = TRUE, parent = emptyenv())
  k <- 0L
  for (i in seq_along(x)) {
    xi <- x[[i]]
    if (!is.na(xi) && !exists(xi, envir = seen, inherits = FALSE)) {
      assign(xi, TRUE, envir = seen)
      k <- k + 1L
    }
    out[[i]] <- k
  }
  out
}

write_dt <- function(dt, path) fwrite(as.data.table(dt), path)

standard_theme <- function() {
  theme_minimal(base_size = 12) +
    theme(
      panel.grid.minor = element_blank(),
      plot.title = element_text(face = "bold"),
      legend.position = "bottom"
    )
}

make_grad_cohort <- function(y,
                             start = 1981L,
                             width = 5L,
                             last_start = 2011L,
                             last_end = 2026L) {
  y <- suppressWarnings(as.integer(y))
  out <- rep(NA_character_, length(y))

  ok <- !is.na(y) & y >= start
  if (!any(ok)) return(out)

  # Pool everyone from last_start onward into one final cohort
  pooled <- ok & y >= last_start
  out[pooled] <- sprintf("%d-%d", last_start, last_end)

  # Usual 5-year bins before last_start
  regular <- ok & y < last_start
  if (any(regular)) {
    cohort_start <- start + ((y[regular] - start) %/% width) * width
    cohort_end   <- cohort_start + width - 1L
    out[regular] <- sprintf("%d-%d", cohort_start, cohort_end)
  }

  out
}

cohort_sort_key <- function(x) suppressWarnings(as.integer(substr(as.character(x), 1, 4)))

trim_cohort_tail <- function(dt, min_people = 30L) {
  z <- copy(dt)

  # Be explicit about valid cells
  z[, keep_cell := !is.na(n_people) & !is.na(tenure) & n_people >= min_people]

  setorder(z, sample, tenure)

  z[, last_good_tenure := if (any(keep_cell, na.rm = TRUE)) {
    max(tenure[keep_cell], na.rm = TRUE)
  } else {
    NA_real_
  }, by = sample]

  z[keep_cell == TRUE & !is.na(last_good_tenure) & tenure <= last_good_tenure]
}

plot_line_by_group <- function(dt, xvar, yvar, groupvar, title, subtitle, xlab, ylab, path) {
  if (!nrow(dt) || !(yvar %in% names(dt))) return(invisible(NULL))
  p <- ggplot(
    dt,
    aes(x = .data[[xvar]], y = .data[[yvar]], color = .data[[groupvar]], group = .data[[groupvar]])
  ) +
    geom_line(linewidth = 0.9, na.rm = TRUE) +
    labs(title = title, subtitle = subtitle, x = xlab, y = ylab, color = NULL) +
    standard_theme()
  ggsave(path, p, width = 9, height = 5.5, dpi = 300)
  invisible(p)
}

plot_two_series <- function(dt, xvar, yvar1, yvar2, labels, title, subtitle, xlab, ylab, path) {
  tmp <- copy(dt)[, .(
    x = get(xvar),
    series_1 = get(yvar1),
    series_2 = get(yvar2)
  )]
  setnames(tmp, c("series_1", "series_2"), labels)
  tmp <- melt(tmp, id.vars = "x", variable.name = "series", value.name = "value")
  p <- ggplot(tmp, aes(x = x, y = value, color = series, linetype = series)) +
    geom_line(linewidth = 0.9, na.rm = TRUE) +
    labs(title = title, subtitle = subtitle, x = xlab, y = ylab, color = NULL, linetype = NULL) +
    standard_theme()
  ggsave(path, p, width = 9, height = 5.5, dpi = 300)
  invisible(p)
}

plot_single_series <- function(dt, xvar, yvar, title, subtitle, xlab, ylab, path) {
  if (!nrow(dt) || !(yvar %in% names(dt))) return(invisible(NULL))
  p <- ggplot(dt, aes(x = .data[[xvar]], y = .data[[yvar]])) +
    geom_line(linewidth = 0.9, na.rm = TRUE) +
    labs(title = title, subtitle = subtitle, x = xlab, y = ylab) +
    standard_theme()
  ggsave(path, p, width = 9, height = 5.5, dpi = 300)
  invisible(p)
}

safe_mean_present <- function(dt, var) {
  if (!(var %in% names(dt))) return(NA_real_)
  x <- dt[[var]]
  if (!length(x) || all(is.na(x))) return(NA_real_)
  mean(x, na.rm = TRUE)
}

# =========================================================
# Output folders
# =========================================================
OUT <- list(
  root        = CONFIG$out_dir,
  audit       = file.path(CONFIG$out_dir, "audit"),
  tables      = file.path(CONFIG$out_dir, "tables"),
  figures     = file.path(CONFIG$out_dir, "figures"),
  places      = file.path(CONFIG$out_dir, "places"),
  ppml        = file.path(CONFIG$out_dir, "ppml_fe"),
  diagnostics = file.path(CONFIG$out_dir, "diagnostics"),
  cohorts     = file.path(CONFIG$out_dir, "cohorts"),
  fields      = file.path(CONFIG$out_dir, "fields")
)
invisible(lapply(OUT, ensure_dir))

# =========================================================
# Inspect schema / select vars
# =========================================================
log_msg("Opening parquet dataset:", CONFIG$input_dir)
ds <- open_dataset(CONFIG$input_dir, format = "parquet")
available <- names(ds)

need <- c(
  "user_id", "year",
  "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
  "avg_salary", "avg_total_comp",
  "first_city", "first_state", "first_country", "first_region", "first_metro_area",
  "first_rcid", "first_parent_rcid",
  "n_positions", "n_unique_companies",
  "career_modal_section", "career_modal_class",
  "immig_first_deg_or_job_nonUS", "immig1_post", "immig1_ever", "immig2_post", "immig2_ever",
  "first_startdate_pos", "first_startdate_edu",
  "first_university_country", "edu_first_university_country",
  "first_field", "last_field"
)
selected_cols <- intersect(need, available)

schema_types <- vapply(selected_cols, function(v) {
  tryCatch(as.character(ds$schema$GetFieldByName(v)$type), error = function(e) NA_character_)
}, character(1))
write_dt(data.table(variable = selected_cols, type = schema_types),
         file.path(OUT$audit, "schema_columns.csv"))

log_msg("Collecting analysis columns")
raw <- ds %>%
  select(all_of(selected_cols)) %>%
  filter(year >= CONFIG$year_min, year <= CONFIG$year_max) %>%
  collect() %>%
  as.data.table()

for (cc in names(raw)) {
  if (is.character(raw[[cc]]) || is.factor(raw[[cc]])) {
    set(raw, j = cc, value = null_if_empty(raw[[cc]]))
  }
}

num_vars <- intersect(c(
  "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
  "avg_salary", "avg_total_comp", "n_positions", "n_unique_companies"
), names(raw))
for (cc in num_vars) set(raw, j = cc, value = suppressWarnings(as.numeric(raw[[cc]])))

raw[, user_id := as.character(user_id)]
raw[, year := as.integer(year)]

for (v in intersect(c("n_patents", "n_applications", "n_first_inventor", "n_us_citations"), names(raw))) {
  raw[is.na(get(v)), (v) := 0]
}

if (CONFIG$us_only && "first_country" %in% names(raw)) {
  raw <- raw[first_country == CONFIG$us_country]
}

setorder(raw, user_id, year)

# =========================================================
# Derived variables
# =========================================================
raw[, edu_year := extract_year(first_startdate_edu)]
raw[, pos_year := extract_year(first_startdate_pos)]
raw[, first_obs_year := min(year, na.rm = TRUE), by = user_id]
raw[, first_pat_year := {
  yy <- year[n_patents > 0]
  if (length(yy) == 0L) NA_integer_ else min(yy, na.rm = TRUE)
}, by = user_id]

raw[, tenure_edu3      := valid_tenure(year - edu_year + 3L, CONFIG$tenure_max)]
raw[, tenure_pos       := valid_tenure(year - pos_year, CONFIG$tenure_max)]
raw[, tenure_first_obs := valid_tenure(year - first_obs_year, CONFIG$tenure_max)]
raw[, tenure_first_pat := valid_tenure(year - first_pat_year, CONFIG$tenure_max)]

raw[, tenure_main := tenure_edu3]
raw[is.na(tenure_main), tenure_main := tenure_pos]
raw[is.na(tenure_main), tenure_main := tenure_first_obs]

raw[, patenting := as.integer(n_patents > 0)]
raw[, first_inventor_share := fifelse(n_patents > 0, n_first_inventor / pmax(n_patents, 1), 0)]
raw[, citations_per_patent := fifelse(n_patents > 0, n_us_citations / pmax(n_patents, 1), NA_real_)]

raw[, grad_cohort_5y := make_grad_cohort(
  edu_year,
  start = CONFIG$cohort_start,
  width = CONFIG$cohort_width,
  last_start = CONFIG$cohort_last_start,
  last_end = CONFIG$cohort_last_end
)]

imm_var <- intersect(c(
  "immig_first_deg_or_job_nonUS",
  "immig1_post", "immig1_ever", "immig2_post", "immig2_ever"
), names(raw))[1]
if (!is.na(imm_var)) {
  raw[, immigrant := as.integer(get(imm_var))]
} else {
  raw[, immigrant := NA_integer_]
}

# =========================================================
# Audit
# =========================================================
audit_summary <- rbindlist(lapply(names(raw), function(v) {
  x <- raw[[v]]
  data.table(
    variable = v,
    class = paste(class(x), collapse = ";"),
    missing_rate = mean(is.na(x)),
    n_distinct = safe_uniqueN(x),
    min = if (is.numeric(x)) suppressWarnings(min(x, na.rm = TRUE)) else NA_real_,
    max = if (is.numeric(x)) suppressWarnings(max(x, na.rm = TRUE)) else NA_real_
  )
}), fill = TRUE)
write_dt(audit_summary, file.path(OUT$audit, "analysis_schema_audit.csv"))

# =========================================================
# Mobility variables
# =========================================================
dt <- copy(raw)
setorder(dt, user_id, year)

dt[, prev_year := shift(year), by = user_id]
dt[, gap_years := year - prev_year]
dt[, is_consecutive := !is.na(prev_year) & gap_years == 1L]

for (nm in c("first_parent_rcid", "first_rcid", "first_city", "first_metro_area")) {
  dt[, paste0(nm, "_lag") := shift(get(nm)), by = user_id]
}

dt[, change_parent_lastobs := as.integer(
  !is.na(first_parent_rcid) & !is.na(first_parent_rcid_lag) & first_parent_rcid != first_parent_rcid_lag
)]
dt[, change_rcid_lastobs := as.integer(
  !is.na(first_rcid) & !is.na(first_rcid_lag) & first_rcid != first_rcid_lag
)]
dt[, change_city_lastobs := as.integer(
  !is.na(first_city) & !is.na(first_city_lag) & first_city != first_city_lag
)]
dt[, change_metro_lastobs := as.integer(
  !is.na(first_metro_area) & !is.na(first_metro_area_lag) & first_metro_area != first_metro_area_lag
)]

dt[, change_parent_consec := as.integer(is_consecutive & change_parent_lastobs == 1L)]
dt[, change_city_consec   := as.integer(is_consecutive & change_city_lastobs == 1L)]
dt[, change_metro_consec  := as.integer(is_consecutive & change_metro_lastobs == 1L)]

dt[, within_parent_rcid_move_consec := as.integer(is_consecutive & change_rcid_lastobs == 1L & change_parent_lastobs == 0L)]
dt[, across_parent_move_consec      := as.integer(change_parent_consec == 1L)]
dt[, firm_only_move_consec          := as.integer(change_parent_consec == 1L & change_city_consec == 0L)]
dt[, joint_firm_city_move_consec    := as.integer(change_parent_consec == 1L & change_city_consec == 1L)]

dt[, cum_n_parents := cum_distinct_count(first_parent_rcid), by = user_id]
dt[, cum_n_cities  := cum_distinct_count(first_city), by = user_id]

# =========================================================
# Leave-one-out environment measures
# =========================================================
group_env <- function(x, by_cols, prefix) {
  keep <- complete.cases(x[, ..by_cols])
  agg <- x[keep, .(
    sum_patents    = sum(n_patents, na.rm = TRUE),
    sum_citations  = sum(n_us_citations, na.rm = TRUE),
    n_inventors    = .N,
    mean_patents   = mean(n_patents, na.rm = TRUE),
    mean_citations = mean(n_us_citations, na.rm = TRUE)
  ), by = by_cols]

  setnames(
    agg,
    c("sum_patents", "sum_citations", "n_inventors", "mean_patents", "mean_citations"),
    c(
      paste0(prefix, "_sum_patents"),
      paste0(prefix, "_sum_citations"),
      paste0(prefix, "_n_inventors"),
      paste0(prefix, "_mean_patents"),
      paste0(prefix, "_mean_citations")
    )
  )

  x <- agg[x, on = by_cols]

  if (CONFIG$run_leave_one_out) {
    x[, (paste0(prefix, "_loo_patents")) := fifelse(
      get(paste0(prefix, "_n_inventors")) > 1,
      (get(paste0(prefix, "_sum_patents")) - n_patents) / (get(paste0(prefix, "_n_inventors")) - 1),
      NA_real_
    )]
    x[, (paste0(prefix, "_loo_citations")) := fifelse(
      get(paste0(prefix, "_n_inventors")) > 1,
      (get(paste0(prefix, "_sum_citations")) - n_us_citations) / (get(paste0(prefix, "_n_inventors")) - 1),
      NA_real_
    )]
  }
  x
}

dt[, parent_metro := fifelse(
  !is.na(first_parent_rcid) & !is.na(first_metro_area),
  paste0(first_parent_rcid, "||", first_metro_area),
  NA_character_
)]

dt <- group_env(dt, c("first_parent_rcid", "year"), "parent")
dt <- group_env(dt, c("first_city", "year"), "city")
dt <- group_env(dt, c("first_metro_area", "year"), "metro")
dt <- group_env(dt, c("parent_metro", "year"), "parent_metro")

# =========================================================
# Baseline PPML FE extraction
# =========================================================
add_cluster_year_size <- function(x, cluster_var, out_var) {
  out <- rep(NA_real_, nrow(x))
  valid <- !is.na(x[[cluster_var]]) & x[[cluster_var]] != "" & !is.na(x$year)
  if (!any(valid)) {
    x[, (out_var) := out]
    return(x)
  }

  tmp <- x[valid, .(cluster_value = as.character(get(cluster_var)), year, user_id)]
  size_dt <- tmp[, .(cluster_year_size = uniqueN(user_id)), by = .(cluster_value, year)]
  x[, cluster_value_tmp__ := as.character(get(cluster_var))]
  x[size_dt, (out_var) := i.cluster_year_size, on = .(cluster_value_tmp__ = cluster_value, year)]
  x[, cluster_value_tmp__ := NULL]
  x[]
}

run_baseline_ppml_fe <- function(x, cluster_var, size_var, label) {
  d <- copy(x)[
    !is.na(get(cluster_var)) & get(cluster_var) != "" &
      is.finite(get(size_var)) & get(size_var) >= CONFIG$ppml_min_cluster_year_size &
      is.finite(tenure_main) & !is.na(user_id) & !is.na(year) &
      !is.na(n_patents) & n_patents >= 0,
    .(
      user_id,
      year,
      n_patents,
      tenure_main,
      cluster_value = as.character(get(cluster_var)),
      cluster_size_current = as.numeric(get(size_var))
    )
  ]

  if (!nrow(d)) {
    return(list(fit = NULL, coef = data.table(), fe = data.table(), sample = data.table()))
  }

  d[, tenure_sq := tenure_main^2]
  d[, log_cluster_size_current := log(pmax(cluster_size_current, 1))]
  d[, user_fe := as.factor(user_id)]
  d[, year_fe := as.factor(year)]
  d[, cluster_fe := as.factor(cluster_value)]
  d[, cluster_id := as.factor(cluster_value)]

  fit <- tryCatch(
    fixest::fepois(
      n_patents ~ log_cluster_size_current + tenure_main + tenure_sq | user_fe + cluster_fe + year_fe,
      data = d,
      vcov = ~cluster_id,
      notes = FALSE,
      warn = FALSE
    ),
    error = function(e) {
      log_msg("PPML failed for", label, ":", conditionMessage(e))
      NULL
    }
  )

  coef_dt <- data.table()
  fe_dt <- data.table()

  if (!is.null(fit)) {
    ct <- tryCatch(fixest::coeftable(fit), error = function(e) NULL)
    if (!is.null(ct) && nrow(ct) > 0) {
      coef_dt <- data.table(
        cluster_level = label,
        term = rownames(ct),
        estimate = as.numeric(ct[, 1]),
        se = as.numeric(ct[, 2]),
        stat = as.numeric(ct[, 3]),
        p_value = as.numeric(ct[, 4]),
        n_obs = nobs(fit)
      )
    }

     fe_obj <- tryCatch(fixest::fixef(fit), error = function(e) NULL)
     if (!is.null(fe_obj) && "cluster_fe" %in% names(fe_obj)) {
       fe_vals <- fe_obj$cluster_fe
       fe_dt <- data.table(
         cluster_id = as.character(names(fe_vals)),
         fe_raw = as.numeric(fe_vals)
       )

       obs_w <- d[, .N, by = .(cluster_id)]
       obs_w[, cluster_id := as.character(cluster_id)]
       fe_dt[, cluster_id := as.character(cluster_id)]

       fe_dt <- merge(fe_dt, obs_w, by = "cluster_id", all.x = TRUE)

       mu_fe <- fe_dt[is.finite(fe_raw) & is.finite(N) & N > 0,
                      weighted.mean(fe_raw, w = N, na.rm = TRUE)]

       if (!is.finite(mu_fe)) {
         warning(sprintf("Could not compute weighted mean FE for %s; using unweighted mean instead.", label))
         mu_fe <- fe_dt[is.finite(fe_raw), mean(fe_raw, na.rm = TRUE)]
       }

       fe_dt[, fe_centered := fe_raw - mu_fe]
       fe_dt[, cluster_level := label]
       setcolorder(fe_dt, c("cluster_level", "cluster_id", "N", "fe_raw", "fe_centered"))
    }
  }

  write_dt(coef_dt, file.path(OUT$ppml, paste0("baseline_ppml_", tolower(label), "_coefficients.csv")))
  write_dt(fe_dt, file.path(OUT$ppml, paste0("baseline_ppml_", tolower(label), "_fixed_effects.csv")))

  list(fit = fit, coef = coef_dt, fe = fe_dt, sample = d)
}

attach_ppml_fe <- function(x, fe_dt, cluster_var, prefix) {
  raw_nm <- paste0(prefix, "_ppml_fe_raw")
  ctr_nm <- paste0(prefix, "_ppml_fe_centered")
  x[, (raw_nm) := NA_real_]
  x[, (ctr_nm) := NA_real_]

  if (is.null(fe_dt) || !nrow(fe_dt)) return(x)

  map <- copy(fe_dt)[, cluster_value_tmp__ := as.character(cluster_id)]
  x[, cluster_value_tmp__ := as.character(get(cluster_var))]
  x[map, c(raw_nm, ctr_nm) := .(i.fe_raw, i.fe_centered), on = "cluster_value_tmp__"]
  x[, cluster_value_tmp__ := NULL]
  x[]
}

log_msg("Building current cluster-year size controls for baseline PPML")
dt <- add_cluster_year_size(dt, "first_parent_rcid", "parent_size_current")
dt <- add_cluster_year_size(dt, "first_metro_area", "metro_size_current")
dt <- add_cluster_year_size(dt, "parent_metro", "metro_parent_size_current")

if (isTRUE(CONFIG$run_ppml_fe)) {
  log_msg("Running baseline PPML with parent FE")
  parent_ppml <- run_baseline_ppml_fe(dt, "first_parent_rcid", "parent_size_current", "PARENT")
  dt <- attach_ppml_fe(dt, parent_ppml$fe, "first_parent_rcid", "parent")

  log_msg("Running baseline PPML with metro FE")
  metro_ppml <- run_baseline_ppml_fe(dt, "first_metro_area", "metro_size_current", "METRO")
  dt <- attach_ppml_fe(dt, metro_ppml$fe, "first_metro_area", "metro")

  log_msg("Running baseline PPML with metro-parent FE")
  metro_parent_ppml <- run_baseline_ppml_fe(dt, "parent_metro", "metro_parent_size_current", "METRO_PARENT")
  dt <- attach_ppml_fe(dt, metro_parent_ppml$fe, "parent_metro", "metro_parent")
}

# =========================================================
# Inventor groups
# =========================================================
inv_tot <- dt[, .(
  total_patents    = sum(n_patents, na.rm = TRUE),
  total_citations  = sum(n_us_citations, na.rm = TRUE),
  total_firstinv   = sum(n_first_inventor, na.rm = TRUE),
  obs_n            = .N,
  n_parent_moves   = sum(change_parent_lastobs, na.rm = TRUE),
  n_city_moves     = sum(change_city_lastobs, na.rm = TRUE)
), by = user_id]

top_decile_cut <- quantile(inv_tot$total_patents, 0.90, na.rm = TRUE, names = FALSE)
inv_tot[, top_decile := total_patents >= top_decile_cut]
inv_tot[, repeat_churner := as.integer(n_parent_moves >= 3)]
inv_tot[, stable_career  := as.integer(n_parent_moves == 0)]
inv_tot[, leadership_share_lifetime := fifelse(total_patents > 0, total_firstinv / pmax(total_patents, 1), NA_real_)]

dt <- inv_tot[dt, on = "user_id"]

# =========================================================
# Lifecycle summarizer
# IMPORTANT: move probabilities are now EXPLICITLY unconditional
# on moving, because the denominator is n_people at each tenure.
# =========================================================
make_lifecycle_table <- function(x, tenure_var, sample_label = "FULL_SAMPLE") {
  z <- copy(x)[is.finite(get(tenure_var))]
  if (!nrow(z)) return(data.table())

  min_patents_threshold <- if (!is.null(CONFIG$min_patents_cohort_tenure)) {
    as.numeric(CONFIG$min_patents_cohort_tenure)
  } else {
    20
  }

  out <- z[, {
    n_people <- uniqueN(user_id)

    n_patenting_people <- sum(patenting, na.rm = TRUE)
    n_parent_moves     <- sum(change_parent_consec == 1L, na.rm = TRUE)
    n_city_moves       <- sum(change_city_consec == 1L, na.rm = TRUE)
    n_metro_moves      <- sum(change_metro_consec == 1L, na.rm = TRUE)
    n_within_parent    <- sum(within_parent_rcid_move_consec == 1L, na.rm = TRUE)
    n_across_parent    <- sum(across_parent_move_consec == 1L, na.rm = TRUE)
    n_firm_only        <- sum(firm_only_move_consec == 1L, na.rm = TRUE)
    n_joint_firm_city  <- sum(joint_firm_city_move_consec == 1L, na.rm = TRUE)

    total_patents_cell   <- sum(n_patents, na.rm = TRUE)
    total_citations_cell <- sum(n_us_citations, na.rm = TRUE)

    avg_citations_per_patent_stable <- if (!is.na(total_patents_cell) &&
                                           total_patents_cell >= min_patents_threshold) {
      total_citations_cell / total_patents_cell
    } else {
      NA_real_
    }

    list(
      n_obs = .N,
      n_people = n_people,
      avg_patents = mean(n_patents, na.rm = TRUE),
      avg_applications = mean(n_applications, na.rm = TRUE),
      avg_citations = mean(n_us_citations, na.rm = TRUE),

      total_patents_cell = total_patents_cell,
      total_citations_cell = total_citations_cell,
      avg_citations_per_patent = avg_citations_per_patent_stable,
      avg_citations_per_patent_raw = mean(citations_per_patent, na.rm = TRUE),

      avg_first_inventor = mean(n_first_inventor, na.rm = TRUE),
      avg_first_inventor_share = mean(first_inventor_share, na.rm = TRUE),
      avg_parent_loo_patents = mean(parent_loo_patents, na.rm = TRUE),
      avg_city_loo_patents = mean(city_loo_patents, na.rm = TRUE),
      avg_metro_loo_patents = mean(metro_loo_patents, na.rm = TRUE),
      avg_parent_loo_citations = mean(parent_loo_citations, na.rm = TRUE),
      avg_city_loo_citations = mean(city_loo_citations, na.rm = TRUE),
      avg_metro_loo_citations = mean(metro_loo_citations, na.rm = TRUE),
      avg_parent_ppml_fe = safe_mean_present(.SD, "parent_ppml_fe_centered"),
      avg_metro_ppml_fe = safe_mean_present(.SD, "metro_ppml_fe_centered"),
      avg_metro_parent_ppml_fe = safe_mean_present(.SD, "metro_parent_ppml_fe_centered"),
      avg_cum_n_parents = mean(cum_n_parents, na.rm = TRUE),
      avg_cum_n_cities = mean(cum_n_cities, na.rm = TRUE),

      n_patenting_people = n_patenting_people,
      p_patenting = if (n_people > 0) n_patenting_people / n_people else NA_real_,

      n_parent_moves = n_parent_moves,
      n_city_moves = n_city_moves,
      n_metro_moves = n_metro_moves,
      n_within_parent_moves = n_within_parent,
      n_across_parent_moves = n_across_parent,
      n_firm_only_moves = n_firm_only,
      n_joint_firm_city_moves = n_joint_firm_city,

      p_change_parent_consec = if (n_people > 0) n_parent_moves / n_people else NA_real_,
      p_change_city_consec = if (n_people > 0) n_city_moves / n_people else NA_real_,
      p_change_metro_consec = if (n_people > 0) n_metro_moves / n_people else NA_real_,
      p_within_parent_rcid_consec = if (n_people > 0) n_within_parent / n_people else NA_real_,
      p_across_parent_consec = if (n_people > 0) n_across_parent / n_people else NA_real_,
      p_firm_only_move_consec = if (n_people > 0) n_firm_only / n_people else NA_real_,
      p_joint_firm_city_consec = if (n_people > 0) n_joint_firm_city / n_people else NA_real_
    )
  }, by = tenure_var]

  setnames(out, tenure_var, "tenure")
  out[, sample := sample_label]
  out[]
}

make_lifecycle_by_group <- function(x, tenure_var, group_var, min_people_group = 1L) {
  z <- copy(x)
  z <- z[!is.na(get(group_var))]
  if (!nrow(z)) return(data.table())

  counts <- z[, .(n_people_group = uniqueN(user_id)), by = group_var]
  keep_levels <- counts[n_people_group >= min_people_group][[group_var]]
  if (!length(keep_levels)) return(data.table())

  keep_levels <- keep_levels[order(cohort_sort_key(keep_levels))]
  out <- rbindlist(
    lapply(keep_levels, function(g) {
      make_lifecycle_table(z[get(group_var) == g], tenure_var, as.character(g))
    }),
    fill = TRUE
  )
  out[]
}

# =========================================================
# Baseline lifecycle tables
# =========================================================
log_msg("Building baseline lifecycle tables")

main_life <- rbindlist(list(
  make_lifecycle_table(dt, "tenure_main", "FULL_SAMPLE"),
  make_lifecycle_table(dt[top_decile == TRUE], "tenure_main", "TOP_DECILE"),
  make_lifecycle_table(dt[top_decile == FALSE], "tenure_main", "COMPLEMENT"),
  make_lifecycle_table(dt[repeat_churner == 1L], "tenure_main", "REPEAT_CHURNER"),
  make_lifecycle_table(dt[stable_career == 1L], "tenure_main", "STABLE_CAREER")
), fill = TRUE)
write_dt(main_life, file.path(OUT$tables, "lifecycle_by_tenure_main.csv"))

# Explicit movement table with counts + unconditional rates
move_unconditional <- main_life[sample == "FULL_SAMPLE", .(
  tenure,
  n_people,
  n_parent_moves,
  n_city_moves,
  n_metro_moves,
  n_within_parent_moves,
  n_across_parent_moves,
  n_firm_only_moves,
  n_joint_firm_city_moves,
  p_change_parent_consec,
  p_change_city_consec,
  p_change_metro_consec,
  p_within_parent_rcid_consec,
  p_across_parent_consec,
  p_firm_only_move_consec,
  p_joint_firm_city_consec
)]
write_dt(move_unconditional, file.path(OUT$tables, "move_probabilities_unconditional_by_tenure.csv"))

# =========================================================
# Baseline lifecycle figures
# =========================================================
plot_metrics <- list(
  avg_patents = "Average patents",
  avg_citations = "Average citations",
  avg_citations_per_patent = "Citations per patent",
  p_patenting = "Probability of patenting",
  p_change_parent_consec = "Parent-firm move probability",
  p_change_city_consec = "City move probability",
  p_change_metro_consec = "Metro move probability",
  p_within_parent_rcid_consec = "Within-parent rcid move probability",
  p_across_parent_consec = "Across-parent move probability",
  p_firm_only_move_consec = "Firm-only move probability",
  p_joint_firm_city_consec = "Joint firm-city move probability",
  avg_metro_ppml_fe = "Average metro FE (baseline PPML)",
  avg_parent_ppml_fe = "Average parent FE (baseline PPML)",
  avg_metro_parent_ppml_fe = "Average metro-parent FE (baseline PPML)"
)

plot_store <- list()
for (m in names(plot_metrics)) {
  plot_store[[m]] <- plot_line_by_group(
    dt = main_life,
    xvar = "tenure",
    yvar = m,
    groupvar = "sample",
    title = paste("Lifecycle:", plot_metrics[[m]]),
    subtitle = "Main tenure definition",
    xlab = "Tenure",
    ylab = plot_metrics[[m]],
    path = file.path(OUT$figures, paste0("baseline_", m, ".png"))
  )
}

# =========================================================
# NEW TASK 1: city / metro productivity over time
# =========================================================
log_msg("Building city / metro productivity over time")

make_place_productivity <- function(x, place_var, place_label) {
  z <- copy(x)[!is.na(get(place_var))]

  place_year <- z[, .(
    n_people = uniqueN(user_id),
    total_patents = sum(n_patents, na.rm = TRUE),
    total_citations = sum(n_us_citations, na.rm = TRUE),
    avg_patents_per_inventor = mean(n_patents, na.rm = TRUE),
    avg_citations_per_inventor = mean(n_us_citations, na.rm = TRUE),
    avg_citations_per_patent = if (sum(n_patents, na.rm = TRUE) > 0) {
      sum(n_us_citations, na.rm = TRUE) / sum(n_patents, na.rm = TRUE)
    } else {
      NA_real_
    }
  ), by = .(year, place = get(place_var))]

  time_series <- place_year[, .(
    n_places = .N,
    avg_place_patents_unweighted = mean(avg_patents_per_inventor, na.rm = TRUE),
    avg_place_patents_weighted   = sum(total_patents, na.rm = TRUE) / sum(n_people, na.rm = TRUE),

    avg_place_citations_unweighted = mean(avg_citations_per_inventor, na.rm = TRUE),
    avg_place_citations_weighted   = sum(total_citations, na.rm = TRUE) / sum(n_people, na.rm = TRUE),

    avg_place_citations_per_patent_unweighted = mean(avg_citations_per_patent, na.rm = TRUE),
    avg_place_citations_per_patent_weighted = if (sum(total_patents, na.rm = TRUE) > 0) {
      sum(total_citations, na.rm = TRUE) / sum(total_patents, na.rm = TRUE)
    } else {
      NA_real_
    }
  ), by = year]

  time_series[, place_level := place_label]
  list(place_year = place_year, time_series = time_series)
}

city_prod  <- make_place_productivity(dt, "first_city", "city")
metro_prod <- make_place_productivity(dt, "first_metro_area", "metro")

write_dt(city_prod$place_year,  file.path(OUT$places, "city_year_productivity.csv"))
write_dt(city_prod$time_series, file.path(OUT$places, "city_productivity_over_time.csv"))
write_dt(metro_prod$place_year,  file.path(OUT$places, "metro_year_productivity.csv"))
write_dt(metro_prod$time_series, file.path(OUT$places, "metro_productivity_over_time.csv"))

plot_store[["city_patents_over_time"]] <- plot_two_series(
  city_prod$time_series, "year",
  "avg_place_patents_unweighted", "avg_place_patents_weighted",
  c("Avg across cities", "Weighted by inventors"),
  "Average city productivity over time",
  "Patents per inventor-year",
  "Year", "Patents per inventor-year",
  file.path(OUT$places, "city_productivity_patents_over_time.png")
)

plot_store[["metro_patents_over_time"]] <- plot_two_series(
  metro_prod$time_series, "year",
  "avg_place_patents_unweighted", "avg_place_patents_weighted",
  c("Avg across metros", "Weighted by inventors"),
  "Average metro productivity over time",
  "Patents per inventor-year",
  "Year", "Patents per inventor-year",
  file.path(OUT$places, "metro_productivity_patents_over_time.png")
)

plot_store[["city_citations_over_time"]] <- plot_two_series(
  city_prod$time_series, "year",
  "avg_place_citations_unweighted", "avg_place_citations_weighted",
  c("Avg across cities", "Weighted by inventors"),
  "Average city productivity over time",
  "Citations per inventor-year",
  "Year", "Citations per inventor-year",
  file.path(OUT$places, "city_productivity_citations_over_time.png")
)

plot_store[["metro_citations_over_time"]] <- plot_two_series(
  metro_prod$time_series, "year",
  "avg_place_citations_unweighted", "avg_place_citations_weighted",
  c("Avg across metros", "Weighted by inventors"),
  "Average metro productivity over time",
  "Citations per inventor-year",
  "Year", "Citations per inventor-year",
  file.path(OUT$places, "metro_productivity_citations_over_time.png")
)

# =========================================================
# Diagnostics for post-2020 metro decline
# =========================================================
log_msg("Building annual diagnostics for the post-2020 metro decline")

annual_diag <- dt[, .(
  n_inventors = uniqueN(user_id),
  total_patents = sum(n_patents, na.rm = TRUE),
  total_citations = sum(n_us_citations, na.rm = TRUE),
  n_parent_firms = uniqueN(first_parent_rcid[!is.na(first_parent_rcid)]),
  n_metros = uniqueN(first_metro_area[!is.na(first_metro_area)]),
  n_active_inventors = uniqueN(user_id[n_patents > 0]),
  share_obs_with_parent = mean(!is.na(first_parent_rcid) & first_parent_rcid != ""),
  share_obs_with_metro = mean(!is.na(first_metro_area) & first_metro_area != ""),
  share_patents_with_parent = if (sum(n_patents, na.rm = TRUE) > 0) {
    sum(n_patents[!is.na(first_parent_rcid) & first_parent_rcid != ""], na.rm = TRUE) / sum(n_patents, na.rm = TRUE)
  } else {
    NA_real_
  },
  share_patents_with_metro = if (sum(n_patents, na.rm = TRUE) > 0) {
    sum(n_patents[!is.na(first_metro_area) & first_metro_area != ""], na.rm = TRUE) / sum(n_patents, na.rm = TRUE)
  } else {
    NA_real_
  },
  patents_per_inventor = if (uniqueN(user_id) > 0) sum(n_patents, na.rm = TRUE) / uniqueN(user_id) else NA_real_,
  patents_per_active_inventor = if (uniqueN(user_id[n_patents > 0]) > 0) sum(n_patents, na.rm = TRUE) / uniqueN(user_id[n_patents > 0]) else NA_real_,
  citations_per_patent = if (sum(n_patents, na.rm = TRUE) > 0) sum(n_us_citations, na.rm = TRUE) / sum(n_patents, na.rm = TRUE) else NA_real_,
  inventors_per_parent = if (uniqueN(first_parent_rcid[!is.na(first_parent_rcid)]) > 0) uniqueN(user_id) / uniqueN(first_parent_rcid[!is.na(first_parent_rcid)]) else NA_real_,
  inventors_per_metro = if (uniqueN(first_metro_area[!is.na(first_metro_area)]) > 0) uniqueN(user_id) / uniqueN(first_metro_area[!is.na(first_metro_area)]) else NA_real_
), by = year]

metro_share_diag <- dt[!is.na(first_metro_area), .(
  metro_inventors = uniqueN(user_id),
  metro_patents = sum(n_patents, na.rm = TRUE)
), by = .(year, first_metro_area)]

metro_concentration <- metro_share_diag[, .(
  metro_hhi_inventors = {
    p <- metro_inventors / sum(metro_inventors)
    sum(p^2)
  },
  metro_hhi_patents = if (sum(metro_patents, na.rm = TRUE) > 0) {
    p <- metro_patents / sum(metro_patents, na.rm = TRUE)
    sum(p^2)
  } else {
    NA_real_
  }
), by = year]

annual_diag <- merge(annual_diag, metro_concentration, by = "year", all.x = TRUE)
write_dt(annual_diag, file.path(OUT$diagnostics, "annual_diagnostics_post2020_metro_drop.csv"))

plot_store[["diag_n_inventors_over_time"]] <- plot_single_series(
  annual_diag, "year", "n_inventors",
  "Inventors over time", "Annual inventor count in the US sample",
  "Year", "Number of inventors",
  file.path(OUT$diagnostics, "n_inventors_over_time.png")
)

plot_store[["diag_total_patents_over_time"]] <- plot_single_series(
  annual_diag, "year", "total_patents",
  "Patents over time", "Total patents in the US sample",
  "Year", "Number of patents",
  file.path(OUT$diagnostics, "total_patents_over_time.png")
)

plot_store[["diag_total_citations_over_time"]] <- plot_single_series(
  annual_diag, "year", "total_citations",
  "Citations over time", "Total citations in the US sample",
  "Year", "Number of citations",
  file.path(OUT$diagnostics, "total_citations_over_time.png")
)

plot_store[["diag_n_parent_firms_over_time"]] <- plot_single_series(
  annual_diag, "year", "n_parent_firms",
  "Existing parent firms over time", "Distinct non-missing parent firms in the US sample",
  "Year", "Number of parent firms",
  file.path(OUT$diagnostics, "n_parent_firms_over_time.png")
)

plot_store[["diag_n_metros_over_time"]] <- plot_single_series(
  annual_diag, "year", "n_metros",
  "Existing metros over time", "Distinct non-missing metros in the US sample",
  "Year", "Number of metros",
  file.path(OUT$diagnostics, "n_metros_over_time.png")
)

plot_store[["diag_patents_per_inventor_over_time"]] <- plot_single_series(
  annual_diag, "year", "patents_per_inventor",
  "Patents per inventor over time", "Useful to separate scale from intensity",
  "Year", "Patents per inventor",
  file.path(OUT$diagnostics, "patents_per_inventor_over_time.png")
)

plot_store[["diag_citations_per_patent_over_time"]] <- plot_single_series(
  annual_diag, "year", "citations_per_patent",
  "Citations per patent over time", "Useful to see whether quality also shifts after 2020",
  "Year", "Citations per patent",
  file.path(OUT$diagnostics, "citations_per_patent_over_time.png")
)

plot_store[["diag_share_obs_with_metro_over_time"]] <- plot_two_series(
  annual_diag, "year",
  "share_obs_with_metro", "share_patents_with_metro",
  c("Share of inventor-years with metro", "Share of patents with metro"),
  "Metro coverage over time", "Helpful for checking a Revelio-style measurement story",
  "Year", "Share",
  file.path(OUT$diagnostics, "metro_coverage_over_time.png")
)

plot_store[["diag_share_obs_with_parent_over_time"]] <- plot_two_series(
  annual_diag, "year",
  "share_obs_with_parent", "share_patents_with_parent",
  c("Share of inventor-years with parent", "Share of patents with parent"),
  "Parent-firm coverage over time", "Helpful for checking whether post-2020 decline reflects missing firm structure",
  "Year", "Share",
  file.path(OUT$diagnostics, "parent_coverage_over_time.png")
)

plot_store[["diag_inventors_per_metro_over_time"]] <- plot_single_series(
  annual_diag, "year", "inventors_per_metro",
  "Average inventors per metro over time", "Useful for separating fewer metros from thinner metros",
  "Year", "Inventors per metro",
  file.path(OUT$diagnostics, "inventors_per_metro_over_time.png")
)

plot_store[["diag_metro_concentration_over_time"]] <- plot_two_series(
  annual_diag, "year",
  "metro_hhi_inventors", "metro_hhi_patents",
  c("HHI of inventor shares across metros", "HHI of patent shares across metros"),
  "Metro concentration over time", "Extra diagnostic for compositional concentration",
  "Year", "HHI",
  file.path(OUT$diagnostics, "metro_concentration_over_time.png")
)

# =========================================================
# NEW TASK 2: lifecycle by graduation cohort
# =========================================================
log_msg("Building lifecycle by graduation cohort")

cohort_life <- make_lifecycle_by_group(
  x = dt[!is.na(grad_cohort_5y)],
  tenure_var = "tenure_main",
  group_var = "grad_cohort_5y",
  min_people_group = 1L
)

# Drop implausibly long tenures for the pooled final cohort
cohort_life <- cohort_life[
  !(sample == CONFIG$cohort_last_label & tenure > CONFIG$cohort_last_max_tenure)
]

write_dt(cohort_life, file.path(OUT$cohorts, "lifecycle_by_grad_cohort.csv"))

cohort_life_plot <- trim_cohort_tail(
  cohort_life,
  min_people = CONFIG$min_people_cohort_tenure
)

cohort_metric_map <- list(
  avg_patents = "Average patents",
  avg_citations = "Average citations",
  avg_citations_per_patent = "Citations per patent",
  p_patenting = "Probability of patenting",
  p_change_parent_consec = "Parent-firm move probability",
  p_change_city_consec = "City move probability"
)

for (m in names(cohort_metric_map)) {
  plot_store[[paste0("cohort_", m)]] <- plot_line_by_group(
    dt = cohort_life_plot,
    xvar = "tenure",
    yvar = m,
    groupvar = "sample",
    title = paste("Lifecycle by graduation cohort:", cohort_metric_map[[m]]),
    subtitle = sprintf(
      "Cells shown only when cohort-tenure has at least %d people",
      CONFIG$min_people_cohort_tenure
    ),
    xlab = "Tenure",
    ylab = cohort_metric_map[[m]],
    path = file.path(OUT$cohorts, paste0("cohort_overlay_", m, ".png"))
  )
}

# =========================================================
# NEW TASK 3: same by field (one graph per USPTO section)
# Here I use avg_patents as the lifecycle outcome, with cohorts as lines.
# =========================================================
log_msg("Building field-specific cohort lifecycle plots")

sections_keep <- intersect(CONFIG$keep_sections, sort(unique(dt$career_modal_section)))
field_life_all <- rbindlist(lapply(sections_keep, function(sec) {
  tmp <- dt[career_modal_section == sec & !is.na(grad_cohort_5y)]
  if (!nrow(tmp)) return(NULL)

  cohort_counts <- tmp[, .(n_people = uniqueN(user_id)), by = grad_cohort_5y]
  keep_cohorts <- cohort_counts[n_people >= CONFIG$min_obs_per_cohort_field][order(cohort_sort_key(grad_cohort_5y)), grad_cohort_5y]

  if (!length(keep_cohorts)) return(NULL)

  out <- rbindlist(lapply(keep_cohorts, function(cc) {
    make_lifecycle_table(tmp[grad_cohort_5y == cc], "tenure_main", as.character(cc))
  }), fill = TRUE)

  out[, field_section := sec]
  out[]
}), fill = TRUE)

field_life_all <- field_life_all[
  !(sample == CONFIG$cohort_last_label & tenure > CONFIG$cohort_last_max_tenure)
]

write_dt(field_life_all, file.path(OUT$fields, "field_section_cohort_lifecycle.csv"))

field_life_plot <- trim_cohort_tail(
  field_life_all,
  min_people = CONFIG$min_people_cohort_tenure
)

for (sec in sections_keep) {
  sub <- field_life_plot[field_section == sec]
  if (!nrow(sub)) next

  plot_store[[paste0("field_", sec, "_avg_patents")]] <- plot_line_by_group(
    dt = sub,
    xvar = "tenure",
    yvar = "avg_patents",
    groupvar = "sample",
    title = paste("Lifecycle of patents by graduation cohort - USPTO section", sec),
    subtitle = sprintf(
      "Cells shown only when cohort-tenure has at least %d people",
      CONFIG$min_people_cohort_tenure
    ),
    xlab = "Tenure",
    ylab = "Average patents",
    path = file.path(OUT$fields, paste0("field_section_", sec, "_avg_patents_by_cohort.png"))
  )
}

# =========================================================
# Helpful summary tables for the new outputs
# =========================================================
place_summary <- rbindlist(list(
  city_prod$time_series[, .(
    place_level,
    year,
    avg_patents_unweighted = avg_place_patents_unweighted,
    avg_patents_weighted   = avg_place_patents_weighted,
    avg_citations_unweighted = avg_place_citations_unweighted,
    avg_citations_weighted   = avg_place_citations_weighted
  )],
  metro_prod$time_series[, .(
    place_level,
    year,
    avg_patents_unweighted = avg_place_patents_unweighted,
    avg_patents_weighted   = avg_place_patents_weighted,
    avg_citations_unweighted = avg_place_citations_unweighted,
    avg_citations_weighted   = avg_place_citations_weighted
  )]
), fill = TRUE)
write_dt(place_summary, file.path(OUT$places, "place_productivity_over_time_combined.csv"))
if (exists("annual_diag")) {
  write_dt(annual_diag, file.path(OUT$places, "annual_diagnostics_post2020_metro_drop_copy.csv"))
}

# =========================================================
# Combined PDF
# =========================================================
if (CONFIG$run_combined_pdf && length(plot_store) > 0) {
  pdf(file.path(OUT$figures, "combined_figures.pdf"), width = 9, height = 5.5)
  invisible(lapply(plot_store, function(p) if (!is.null(p)) print(p)))
  dev.off()
}

# =========================================================
# Output index + memo
# =========================================================
memo_lines <- c(
  "# Lifecycle descriptives memo",
  "",
  "## Main updates in this version",
  "- Move probabilities are now computed explicitly with denominator = number of people at a given tenure, not number of movers.",
  "- Added baseline PPML runs that extract parent, metro, and metro-parent fixed effects with controls for log current size, tenure, tenure^2, inventor FE, and year FE.",
  "- Added lifecycle plots that replace raw metro patenting intensity with average extracted metro FE, and parallel versions for parent FE and metro-parent FE.",
  "- Expanded the metro-over-time diagnostics with annual counts of inventors, patents, citations, parent firms, metros, plus coverage and concentration checks after 2020.",
  "- Kept the graduation-cohort and field-cohort lifecycle outputs from the earlier script.",
  "",
  "## Notes",
  "- For place productivity, the main measure is patents (or citations) per inventor-year at the place-year level.",
  "- The time-series files report both unweighted averages across places and inventor-weighted averages.",
  "- Field-specific cohort plots are produced only when a cohort-field cell has at least the minimum number of people set in CONFIG$min_obs_per_cohort_field."
)
writeLines(memo_lines, file.path(OUT$root, "memo.md"))

all_files <- list.files(OUT$root, recursive = TRUE, full.names = FALSE)
writeLines(c("# Output index", "", paste0("- `", sort(all_files), "`")),
           file.path(OUT$root, "output_index.md"))

log_msg("Done. Outputs written to:", OUT$root)


