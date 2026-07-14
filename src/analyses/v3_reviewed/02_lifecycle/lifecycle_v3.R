#!/usr/bin/env Rscript
# Lean descriptive lifecycle module. PPML lifecycle estimates live in
# 01_tenure/tenure_suite_v3.R, so this module does not duplicate those costly
# regressions.

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "dplyr", "readr", "ggplot2", "scales"))
threads <- v3_configure_resources(FALSE)
set.seed(123)

INPUT <- v3_assert_dir(v3_env("REVELIO_V3_INPUT"), "REVELIO_V3_INPUT")
OUT <- v3_output_dir("lifecycle")
TAB <- file.path(OUT, "tables")
FIG <- file.path(OUT, "figures")
DIAG <- file.path(OUT, "diagnostics")
dir.create(TAB, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG, recursive = TRUE, showWarnings = FALSE)
dir.create(DIAG, recursive = TRUE, showWarnings = FALSE)

YEAR_MIN <- as.integer(v3_env("LIFECYCLE_YEAR_MIN", "2010"))
YEAR_MAX <- as.integer(v3_env("LIFECYCLE_YEAR_MAX", "2019"))
US <- v3_env("REVELIO_US_COUNTRY", "United States")
MIN_CELL <- as.integer(v3_env("LIFECYCLE_MIN_CELL", "50"))
MAX_TENURE <- 50L

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

ds <- arrow::open_dataset(INPUT, format = "parquet")
required <- c(
  "user_id", "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
  "first_country", "last_university_country", "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
)
v3_require_cols(names(ds), required, "core V3 panel")

optional <- intersect(c("first_field", "last_field", "career_modal_section", "avg_seniority"), names(ds))
cols <- c(required, optional)

msg("Reading V3 lifecycle panel once:", YEAR_MIN, "-", YEAR_MAX)
dt <- ds |>
  dplyr::filter(
    year >= YEAR_MIN, year <= YEAR_MAX,
    first_country == US, last_university_country == US
  ) |>
  dplyr::select(dplyr::all_of(cols)) |>
  dplyr::collect() |>
  data.table::as.data.table()

if (!nrow(dt)) stop("No lifecycle observations after sample restriction.")
for (cc in intersect(names(dt), c(
  "user_id", "first_country", "last_university_country", "first_startdate_edu",
  "first_startdate_pos", "first_parent_rcid", "first_rcid", "first_city",
  "first_metro_area", "first_field", "last_field", "career_modal_section"
))) data.table::set(dt, j = cc, value = v3_clean_chr(dt[[cc]]))

for (cc in intersect(c("year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations", "avg_seniority"), names(dt))) {
  data.table::set(dt, j = cc, value = suppressWarnings(as.numeric(dt[[cc]])))
}
dt[, year := as.integer(year)]
for (cc in intersect(c("n_patents", "n_applications", "n_first_inventor", "n_us_citations"), names(dt))) dt[is.na(get(cc)), (cc) := 0]
data.table::setorder(dt, user_id, year)
v3_assert_unique_user_year(dt, file.path(DIAG, "duplicate_user_years.csv"))
v3_compute_tenure(dt, MAX_TENURE)

# First observed spell/location variables are intentionally used as the
# within-year identifiers, matching the V3 panel's definition.
dt[, `:=`(
  parent_id = first_parent_rcid,
  firm_id = first_rcid,
  city_id = first_city,
  metro_id = first_metro_area
)]
dt[, mp_id := ifelse(!is.na(parent_id) & !is.na(metro_id), paste(parent_id, metro_id, sep = "||"), NA_character_)]
dt[, tenure_bin := v3_tenure_bin(tenure, MAX_TENURE, 5L)]
dt[, patenting := as.integer(n_patents > 0)]
dt[, first_inventor_share := ifelse(n_patents > 0, n_first_inventor / n_patents, NA_real_)]
dt[, citations_per_patent := ifelse(n_patents > 0, n_us_citations / n_patents, NA_real_)]

# Cohorts are education-completion proxies: first education start plus three.
dt[, grad_proxy_year := v3_year_from_any(first_startdate_edu) + 3L]
dt[, cohort_5y := ifelse(
  is.finite(grad_proxy_year),
  paste0(1980L + 5L * ((grad_proxy_year - 1980L) %/% 5L), "-",
         1984L + 5L * ((grad_proxy_year - 1980L) %/% 5L)),
  NA_character_
)]

# Consecutive year mobility diagnostics.
dt[, lag_year := data.table::shift(year), by = user_id]
dt[, consecutive := !is.na(lag_year) & year == lag_year + 1L]
for (vv in c("parent_id", "firm_id", "city_id", "metro_id")) {
  dt[, paste0(vv, "_lag") := data.table::shift(get(vv)), by = user_id]
}
dt[, parent_move := as.integer(consecutive & !is.na(parent_id) & !is.na(parent_id_lag) & parent_id != parent_id_lag)]
dt[, within_parent_firm_move := as.integer(consecutive & !is.na(firm_id) & !is.na(firm_id_lag) &
                                             firm_id != firm_id_lag & parent_move == 0L)]
dt[, city_move := as.integer(consecutive & !is.na(city_id) & !is.na(city_id_lag) & city_id != city_id_lag)]
dt[, metro_move := as.integer(consecutive & !is.na(metro_id) & !is.na(metro_id_lag) & metro_id != metro_id_lag)]

# Leave-one-out local environment measures. They exclude the focal inventor,
# eliminating mechanical own-outcome correlation.
add_loo <- function(x, id, prefix) {
  valid <- x[!is.na(get(id)) & get(id) != "", .(
    user_id, year, id_value = get(id), n_patents, n_us_citations
  )]
  env <- valid[, .(
    n_inventors = .N,
    sum_patents = sum(n_patents, na.rm = TRUE),
    sum_citations = sum(n_us_citations, na.rm = TRUE)
  ), by = .(id_value, year)]
  data.table::setnames(env, "id_value", id)
  data.table::setnames(
    env,
    c("n_inventors", "sum_patents", "sum_citations"),
    paste0(prefix, c("_n_inventors", "_sum_patents", "_sum_citations"))
  )
  x <- merge(x, env, by = c(id, "year"), all.x = TRUE, sort = FALSE)
  n <- x[[paste0(prefix, "_n_inventors")]]
  s_pat <- x[[paste0(prefix, "_sum_patents")]]
  s_cit <- x[[paste0(prefix, "_sum_citations")]]
  x[, paste0(prefix, "_loo_patents") := ifelse(n > 1, (s_pat - n_patents) / (n - 1), NA_real_)]
  x[, paste0(prefix, "_loo_citations") := ifelse(n > 1, (s_cit - n_us_citations) / (n - 1), NA_real_)]
  x[, c(paste0(prefix, "_sum_patents"), paste0(prefix, "_sum_citations")) := NULL]
  x
}
dt <- add_loo(dt, "parent_id", "parent")
dt <- add_loo(dt, "metro_id", "metro")

# Core lifecycle tables.
tenure_year <- dt[is.finite(tenure), .(
  n_obs = .N,
  n_users = data.table::uniqueN(user_id),
  patenting_rate = mean(patenting, na.rm = TRUE),
  mean_patents = mean(n_patents, na.rm = TRUE),
  mean_citations = mean(n_us_citations, na.rm = TRUE),
  mean_first_inventor_share = mean(first_inventor_share, na.rm = TRUE),
  mean_citations_per_patent = mean(citations_per_patent, na.rm = TRUE)
), by = tenure][order(tenure)]

tenure_bin <- dt[!is.na(tenure_bin), .(
  n_obs = .N,
  n_users = data.table::uniqueN(user_id),
  patenting_rate = mean(patenting, na.rm = TRUE),
  mean_patents = mean(n_patents, na.rm = TRUE),
  mean_citations = mean(n_us_citations, na.rm = TRUE),
  parent_move_rate = mean(parent_move, na.rm = TRUE),
  within_parent_firm_move_rate = mean(within_parent_firm_move, na.rm = TRUE),
  city_move_rate = mean(city_move, na.rm = TRUE),
  metro_move_rate = mean(metro_move, na.rm = TRUE),
  mean_parent_loo_patents = mean(parent_loo_patents, na.rm = TRUE),
  mean_metro_loo_patents = mean(metro_loo_patents, na.rm = TRUE)
), by = tenure_bin][order(tenure_bin)]

cohort_lifecycle <- dt[!is.na(tenure_bin) & !is.na(cohort_5y), .(
  n_obs = .N,
  n_users = data.table::uniqueN(user_id),
  mean_patents = mean(n_patents, na.rm = TRUE),
  patenting_rate = mean(patenting, na.rm = TRUE)
), by = .(cohort_5y, tenure_bin)][n_obs >= MIN_CELL]
cohort_lifecycle[, cohort_order := suppressWarnings(as.integer(substr(cohort_5y, 1, 4)))]
data.table::setorder(cohort_lifecycle, cohort_order, tenure_bin)

# A selection diagnostic: later tenure bins are not necessarily the same people.
user_obs <- dt[, .(n_panel_years = .N), by = user_id]
selection_diag <- merge(
  dt[!is.na(tenure_bin), .(n_obs = .N, n_users = data.table::uniqueN(user_id),
                           mean_first_observed_year = mean(first_obs_year, na.rm = TRUE)),
     by = tenure_bin],
  dt[user_obs, on = "user_id"][!is.na(tenure_bin), .(
    share_users_with_5_plus_panel_years = mean(n_panel_years >= 5L),
    share_users_with_10_plus_panel_years = mean(n_panel_years >= 10L)
  ), by = tenure_bin],
  by = "tenure_bin", all = TRUE
)

audit <- data.table::data.table(
  input = INPUT,
  year_min = YEAR_MIN,
  year_max = YEAR_MAX,
  sample = "first_country == United States AND last_university_country == United States",
  n_obs = nrow(dt),
  n_users = data.table::uniqueN(dt$user_id),
  n_parent_year_cells = data.table::uniqueN(paste(dt$parent_id, dt$year, sep = "||")),
  n_metro_year_cells = data.table::uniqueN(paste(dt$metro_id, dt$year, sep = "||")),
  r_threads = threads,
  notes = "Descriptive module only. PPML FE estimates are in 01_tenure."
)

data.table::fwrite(tenure_year, file.path(TAB, "lifecycle_by_exact_tenure.csv"))
data.table::fwrite(tenure_bin, file.path(TAB, "lifecycle_by_tenure_bin.csv"))
data.table::fwrite(cohort_lifecycle, file.path(TAB, "lifecycle_by_education_cohort.csv"))
data.table::fwrite(selection_diag, file.path(DIAG, "lifecycle_composition_by_tenure_bin.csv"))
data.table::fwrite(audit, file.path(DIAG, "manifest.csv"))

# Compact, information-dense figures.
p1 <- ggplot2::ggplot(tenure_year, ggplot2::aes(x = tenure, y = mean_patents)) +
  ggplot2::geom_line(linewidth = 0.9) +
  ggplot2::geom_point(size = 1.3) +
  ggplot2::labs(title = "Raw lifecycle patenting profile", x = "Tenure", y = "Mean patents") +
  v3_plot_theme()
ggplot2::ggsave(file.path(FIG, "raw_lifecycle_patents.png"), p1, width = 9, height = 5.5, dpi = 250)

p2dt <- data.table::melt(
  tenure_bin,
  id.vars = "tenure_bin",
  measure.vars = c("parent_move_rate", "within_parent_firm_move_rate", "city_move_rate", "metro_move_rate"),
  variable.name = "move_type", value.name = "rate"
)
p2 <- ggplot2::ggplot(p2dt, ggplot2::aes(x = tenure_bin, y = rate, group = move_type, color = move_type)) +
  ggplot2::geom_line(linewidth = 0.85) +
  ggplot2::geom_point(size = 1.3) +
  ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
  ggplot2::labs(title = "Consecutive-year mobility by tenure", x = "Tenure bin", y = "Rate", color = NULL) +
  v3_plot_theme()
ggplot2::ggsave(file.path(FIG, "mobility_by_tenure.png"), p2, width = 10, height = 6, dpi = 250)

if (nrow(cohort_lifecycle)) {
  p3 <- ggplot2::ggplot(cohort_lifecycle,
                         ggplot2::aes(x = tenure_bin, y = mean_patents, group = cohort_5y, color = cohort_5y)) +
    ggplot2::geom_line(linewidth = 0.75) +
    ggplot2::geom_point(size = 1.1) +
    ggplot2::labs(title = "Lifecycle patenting by education cohort",
                  subtitle = paste0("Cells with fewer than ", MIN_CELL, " inventor-years are omitted."),
                  x = "Tenure bin", y = "Mean patents", color = "Cohort") +
    v3_plot_theme()
  ggplot2::ggsave(file.path(FIG, "lifecycle_by_education_cohort.png"), p3, width = 11, height = 6.5, dpi = 250)
}

readr::write_lines(c(
  "# Reviewed lifecycle module",
  "",
  "- Uses the V3 core panel and the U.S.-position / U.S.-educated sample.",
  "- Treats first_* identifiers as the first observed spell/location within each inventor-year.",
  "- Produces descriptive lifecycle, cohort, mobility, and leave-one-out environment summaries.",
  "- Does not duplicate the PPML FE estimates that are maintained in the tenure suite."
), file.path(OUT, "README.md"))

msg("[PASS] Lifecycle module completed. Outputs:", OUT)
