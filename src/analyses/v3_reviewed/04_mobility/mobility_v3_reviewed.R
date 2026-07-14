#!/usr/bin/env Rscript
# Unified reviewed mobility analysis. This replaces runtime text substitution and
# duplicate component scripts with one explicit V3 implementation.

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "dplyr", "readr", "ggplot2", "scales", "fixest"))
threads <- v3_configure_resources(TRUE)
set.seed(123)

INPUT <- v3_assert_dir(v3_env("REVELIO_V3_INPUT"), "REVELIO_V3_INPUT")
OUT <- v3_output_dir("mobility")
TAB <- file.path(OUT, "tables")
FIG <- file.path(OUT, "figures")
DIAG <- file.path(OUT, "diagnostics")
dir.create(TAB, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG, recursive = TRUE, showWarnings = FALSE)
dir.create(DIAG, recursive = TRUE, showWarnings = FALSE)

YEAR_MIN <- as.integer(v3_env("MOBILITY_YEAR_MIN", "2010"))
YEAR_MAX <- as.integer(v3_env("MOBILITY_YEAR_MAX", "2019"))
US <- v3_env("REVELIO_US_COUNTRY", "United States")
MAX_TENURE <- 50L
WRITE_PANEL <- v3_bool(v3_env("MOBILITY_WRITE_PANEL", "0"))
RUN_EVENT_PROFILE <- v3_bool(v3_env("MOBILITY_RUN_EVENT_PROFILE", "0"))
RUN_FE_LPM <- v3_bool(v3_env("MOBILITY_RUN_FE_LPM", "1"))

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

ds <- arrow::open_dataset(INPUT, format = "parquet")
required <- c(
  "user_id", "year", "n_patents", "n_us_citations", "n_first_inventor",
  "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
)
v3_require_cols(names(ds), required, "core V3 panel")
optional <- intersect(c("avg_seniority", "immig_first_deg_or_job_nonUS"), names(ds))
cols <- c(required, optional)

msg("Reading mobility panel once:", YEAR_MIN, "-", YEAR_MAX)
dt <- ds |>
  dplyr::filter(
    year >= YEAR_MIN, year <= YEAR_MAX,
    first_country == US, last_university_country == US
  ) |>
  dplyr::select(dplyr::all_of(cols)) |>
  dplyr::collect() |>
  data.table::as.data.table()

if (!nrow(dt)) stop("No mobility observations after the US-position and US-educated sample restriction.")
for (cc in intersect(names(dt), c(
  "user_id", "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
))) data.table::set(dt, j = cc, value = v3_clean_chr(dt[[cc]]))
for (cc in intersect(c("year", "n_patents", "n_us_citations", "n_first_inventor", "avg_seniority",
                       "immig_first_deg_or_job_nonUS"), names(dt))) {
  data.table::set(dt, j = cc, value = suppressWarnings(as.numeric(dt[[cc]])))
}
dt[, year := as.integer(year)]
for (cc in c("n_patents", "n_us_citations", "n_first_inventor")) dt[is.na(get(cc)), (cc) := 0]
data.table::setorder(dt, user_id, year)
v3_assert_unique_user_year(dt, file.path(DIAG, "duplicate_user_years.csv"))
v3_compute_tenure(dt, MAX_TENURE)

# These are first observed values within the inventor-year, not career-start
# identifiers. The year-to-year comparison is therefore a comparison of the
# first observed spell/location in consecutive years.
dt[, `:=`(
  parent_id = first_parent_rcid,
  firm_id = first_rcid,
  city_id = first_city,
  metro_id = first_metro_area
)]
dt[, tenure_bin := v3_tenure_bin(tenure, MAX_TENURE, 5L)]
dt[, lag_year := data.table::shift(year), by = user_id]
dt[, consecutive := !is.na(lag_year) & year == lag_year + 1L]
for (id in c("parent_id", "firm_id", "city_id", "metro_id")) {
  dt[, paste0(id, "_lag") := data.table::shift(get(id)), by = user_id]
}
for (x in c("n_patents", "n_us_citations", "n_first_inventor", "tenure")) {
  dt[, paste0(x, "_lag") := data.table::shift(get(x)), by = user_id]
}

change <- function(cur, lag) as.integer(!is.na(cur) & !is.na(lag) & cur != lag)
dt[, parent_move := as.integer(consecutive & change(parent_id, parent_id_lag) == 1L)]
dt[, firm_move := as.integer(consecutive & change(firm_id, firm_id_lag) == 1L)]
dt[, city_move := as.integer(consecutive & change(city_id, city_id_lag) == 1L)]
dt[, metro_move := as.integer(consecutive & change(metro_id, metro_id_lag) == 1L)]
dt[, within_parent_firm_move := as.integer(firm_move == 1L & parent_move == 0L)]
dt[, parent_only_move := as.integer(parent_move == 1L & city_move == 0L)]
dt[, city_only_move := as.integer(parent_move == 0L & city_move == 1L)]
dt[, metro_only_move := as.integer(parent_move == 0L & metro_move == 1L)]
dt[, joint_parent_metro_move := as.integer(parent_move == 1L & metro_move == 1L)]

# Group-year environments are formed in the analytical sample. Both origin and
# destination peer-quality measures subtract the focal inventor's own outcome.
make_env <- function(x, id, prefix) {
  z <- x[!is.na(get(id)) & get(id) != "", .(
    n_inventors = as.numeric(.N),
    sum_patents = as.numeric(sum(n_patents, na.rm = TRUE)),
    sum_citations = as.numeric(sum(n_us_citations, na.rm = TRUE))
  ), by = .(id_value = get(id), year)]
  data.table::setnames(z,
    c("n_inventors", "sum_patents", "sum_citations"),
    paste0(prefix, c("_n_inventors", "_sum_patents", "_sum_citations"))
  )
  z
}

join_env <- function(base, env, key, time, side, prefix) {
  e <- data.table::copy(env)
  data.table::setnames(e, c("id_value", "year"), c("join_id", "join_year"))
  metric_cols <- setdiff(names(e), c("join_id", "join_year"))
  data.table::setnames(e, metric_cols, paste0(side, "_", metric_cols))
  merge(base, e, by.x = c(key, time), by.y = c("join_id", "join_year"), all.x = TRUE, sort = FALSE)
}

parent_env <- make_env(dt, "parent_id", "parent")
metro_env <- make_env(dt, "metro_id", "metro")
city_env <- make_env(dt, "city_id", "city")

ana <- data.table::copy(dt[consecutive == TRUE])
ana[, origin_year := lag_year]
ana <- join_env(ana, parent_env, "parent_id_lag", "origin_year", "origin", "parent")
ana <- join_env(ana, parent_env, "parent_id", "year", "dest", "parent")
ana <- join_env(ana, metro_env, "metro_id_lag", "origin_year", "origin", "metro")
ana <- join_env(ana, metro_env, "metro_id", "year", "dest", "metro")
ana <- join_env(ana, city_env, "city_id_lag", "origin_year", "origin", "city")
ana <- join_env(ana, city_env, "city_id", "year", "dest", "city")

add_peer_metrics <- function(x, prefix, side, own_pat, own_cit) {
  n <- x[[paste0(side, "_", prefix, "_n_inventors")]]
  sp <- x[[paste0(side, "_", prefix, "_sum_patents")]]
  sc <- x[[paste0(side, "_", prefix, "_sum_citations")]]
  x[, (paste0(side, "_", prefix, "_peer_count")) := ifelse(n > 1, n - 1, NA_real_)]
  x[, (paste0(side, "_", prefix, "_peer_patents")) := ifelse(n > 1, (sp - get(own_pat)) / (n - 1), NA_real_)]
  x[, (paste0(side, "_", prefix, "_peer_citations")) := ifelse(n > 1, (sc - get(own_cit)) / (n - 1), NA_real_)]
  x
}
for (prefix in c("parent", "metro", "city")) {
  ana <- add_peer_metrics(ana, prefix, "origin", "n_patents_lag", "n_us_citations_lag")
  ana <- add_peer_metrics(ana, prefix, "dest", "n_patents", "n_us_citations")
}

for (prefix in c("parent", "metro", "city")) {
  ana[, (paste0("delta_", prefix, "_size")) :=
        get(paste0("dest_", prefix, "_n_inventors")) - get(paste0("origin_", prefix, "_n_inventors"))]
  ana[, (paste0("delta_", prefix, "_peer_patents")) :=
        get(paste0("dest_", prefix, "_peer_patents")) - get(paste0("origin_", prefix, "_peer_patents"))]
  ana[, (paste0("delta_", prefix, "_peer_citations")) :=
        get(paste0("dest_", prefix, "_peer_citations")) - get(paste0("origin_", prefix, "_peer_citations"))]
}

move_rates <- data.table::rbindlist(lapply(
  c("parent_move", "firm_move", "within_parent_firm_move", "city_move", "metro_move",
    "parent_only_move", "city_only_move", "metro_only_move", "joint_parent_metro_move"),
  function(v) data.table::data.table(
    move_type = v,
    rate = mean(dt[[v]][dt$consecutive], na.rm = TRUE),
    n_consecutive_inventor_years = sum(dt$consecutive, na.rm = TRUE)
  )
))
data.table::fwrite(move_rates, file.path(TAB, "transition_rates.csv"))

event_sets <- list(
  parent_move = ana[parent_move == 1L],
  within_parent_firm_move = ana[within_parent_firm_move == 1L],
  city_move = ana[city_move == 1L],
  metro_move = ana[metro_move == 1L],
  joint_parent_metro_move = ana[joint_parent_metro_move == 1L]
)
delta_vars <- c(
  "delta_parent_size", "delta_parent_peer_patents", "delta_parent_peer_citations",
  "delta_metro_size", "delta_metro_peer_patents", "delta_metro_peer_citations",
  "delta_city_size", "delta_city_peer_patents", "delta_city_peer_citations"
)
od <- data.table::rbindlist(lapply(names(event_sets), function(event) {
  z <- event_sets[[event]]
  data.table::rbindlist(lapply(intersect(delta_vars, names(z)), function(v) {
    x <- z[[v]]
    data.table::data.table(
      move_type = event,
      measure = v,
      mean_delta = as.numeric(mean(x, na.rm = TRUE)),
      median_delta = as.numeric(stats::median(x, na.rm = TRUE)),
      share_positive = as.numeric(mean(x > 0, na.rm = TRUE)),
      n = as.integer(sum(is.finite(x)))
    )
  }))
}), fill = TRUE)
data.table::fwrite(od, file.path(TAB, "origin_destination_peer_quality_deltas.csv"))

# A concise predictor table and binned figures use lagged origin peer measures.
predictors <- c("origin_parent_peer_patents", "origin_parent_peer_citations",
                "origin_parent_peer_count", "origin_metro_peer_patents",
                "origin_metro_peer_count")
predictor_summary <- data.table::rbindlist(lapply(intersect(predictors, names(ana)), function(v) {
  x <- ana[[v]]
  x <- x[is.finite(x)]
  data.table::data.table(
    predictor = v,
    n = as.integer(length(x)),
    p25 = if (length(x)) as.numeric(stats::quantile(x, .25, na.rm = TRUE)) else NA_real_,
    p50 = if (length(x)) as.numeric(stats::quantile(x, .50, na.rm = TRUE)) else NA_real_,
    p75 = if (length(x)) as.numeric(stats::quantile(x, .75, na.rm = TRUE)) else NA_real_,
    parent_move_rate = mean(ana[is.finite(get(v)), parent_move], na.rm = TRUE)
  )
}), fill = TRUE)
data.table::fwrite(predictor_summary, file.path(TAB, "origin_peer_predictor_summary.csv"))

bin_plot <- function(x, xvar, yvar, title, file) {
  z <- x[is.finite(get(xvar)) & !is.na(get(yvar))]
  if (nrow(z) < 100L) return(invisible(NULL))
  br <- unique(stats::quantile(z[[xvar]], probs = seq(0, 1, length.out = 21), na.rm = TRUE))
  if (length(br) < 4L) return(invisible(NULL))
  z[, bin := cut(get(xvar), breaks = br, include.lowest = TRUE, labels = FALSE)]
  b <- z[!is.na(bin), .(x = mean(get(xvar), na.rm = TRUE), y = mean(get(yvar), na.rm = TRUE), n = .N), by = bin]
  p <- ggplot2::ggplot(b, ggplot2::aes(x = x, y = y)) +
    ggplot2::geom_line(linewidth = 0.9) +
    ggplot2::geom_point(ggplot2::aes(size = n), alpha = 0.85) +
    ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 0.1)) +
    ggplot2::labs(title = title, x = xvar, y = paste0("Pr(", yvar, ")")) +
    v3_plot_theme()
  ggplot2::ggsave(file.path(FIG, file), p, width = 8.5, height = 5.5, dpi = 250)
}
bin_plot(ana, "origin_parent_peer_patents", "parent_move",
         "Parent-move rate by lagged origin-parent peer patenting",
         "parent_move_vs_origin_parent_peer_patents.png")
bin_plot(ana, "origin_parent_peer_count", "parent_move",
         "Parent-move rate by lagged origin-parent peer count",
         "parent_move_vs_origin_parent_peer_count.png")
bin_plot(ana, "origin_metro_peer_patents", "metro_move",
         "Metro-move rate by lagged origin-metro peer patenting",
         "metro_move_vs_origin_metro_peer_patents.png")

# Econometric complement: a deliberately modest inventor- and year-FE linear
# probability model. It is descriptive, uses only lagged leave-one-out peer
# quality, and clusters by inventor. It is not a causal mobility design.
if (RUN_FE_LPM) {
  lpm_data <- ana[
    is.finite(origin_parent_peer_patents) & is.finite(origin_parent_peer_count) &
      !is.na(tenure_bin),
    .(parent_move, origin_parent_peer_patents, origin_parent_peer_count, tenure_bin, user_id, year)
  ]
  if (nrow(lpm_data) && data.table::uniqueN(lpm_data$user_id) > 50L) {
    lpm_data[, z_peer_patents := as.numeric(scale(origin_parent_peer_patents))]
    lpm_data[, z_peer_count := as.numeric(scale(origin_parent_peer_count))]
    fit <- tryCatch(
      fixest::feols(parent_move ~ z_peer_patents + z_peer_count + i(tenure_bin) | user_id + year,
                    data = lpm_data, vcov = ~user_id, notes = FALSE, warn = FALSE),
      error = function(e) NULL
    )
    if (!is.null(fit)) {
      ct <- fixest::coeftable(fit)
      lpm <- data.table::data.table(
        term = rownames(ct), estimate = as.numeric(ct[, 1]), se = as.numeric(ct[, 2]),
        statistic = as.numeric(ct[, 3]), p_value = as.numeric(ct[, 4]),
        n_obs = stats::nobs(fit)
      )
      data.table::fwrite(lpm, file.path(TAB, "parent_move_origin_peer_lpm.csv"))
    }
  }
}

if (RUN_EVENT_PROFILE) {
  first_event <- ana[parent_move == 1L, .(event_year = min(year)), by = user_id]
  es <- merge(dt, first_event, by = "user_id", all = FALSE)
  es[, event_time := year - event_year]
  es <- es[event_time >= -5L & event_time <= 5L]
  event_profile <- es[, .(
    n_obs = .N,
    mean_patents = mean(n_patents, na.rm = TRUE),
    mean_citations = mean(n_us_citations, na.rm = TRUE)
  ), by = event_time]
  data.table::fwrite(event_profile, file.path(TAB, "first_parent_move_event_profile.csv"))
}

if (WRITE_PANEL) {
  panel_cols <- intersect(c(
    "user_id", "year", "tenure", "tenure_bin", "parent_id", "firm_id", "city_id", "metro_id",
    "parent_move", "firm_move", "within_parent_firm_move", "city_move", "metro_move",
    "n_patents", "n_us_citations"
  ), names(dt))
  arrow::write_parquet(as.data.frame(dt[, ..panel_cols]), file.path(OUT, "mobility_panel.parquet"))
}

manifest <- data.table::data.table(
  input = INPUT,
  output = OUT,
  year_min = YEAR_MIN,
  year_max = YEAR_MAX,
  sample_definition = "first_country == United States AND last_university_country == United States",
  identifier_semantics = "first_* identifiers are first observed values within each inventor-year",
  origin_destination_measure = "group-year leave-one-out peer averages; focal inventor removed at origin and destination",
  n_obs = nrow(dt),
  n_consecutive = sum(dt$consecutive, na.rm = TRUE),
  n_users = data.table::uniqueN(dt$user_id),
  write_panel = WRITE_PANEL,
  run_event_profile = RUN_EVENT_PROFILE,
  run_fe_lpm = RUN_FE_LPM,
  r_threads = threads
)
data.table::fwrite(manifest, file.path(DIAG, "manifest.csv"))

readr::write_lines(c(
  "# Reviewed mobility analysis",
  "",
  "- Uses a single explicit V3 script; no runtime gsub/source rewriting.",
  "- Main sample is U.S. position-years and U.S.-educated inventors.",
  "- Move definitions compare first observed identifiers in consecutive inventor-years.",
  "- Origin and destination quality use leave-one-out peer averages.",
  "- All mobility and origin-destination outputs are descriptive; the optional FE LPM is not causal.",
  "- The potentially huge person-year mobility panel is not written unless MOBILITY_WRITE_PANEL=1."
), file.path(OUT, "README.md"))

msg("[PASS] Mobility analysis completed. Outputs:", OUT)
