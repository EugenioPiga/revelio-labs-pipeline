#!/usr/bin/env Rscript
# Metro productivity premium decomposition, reviewed V3 version.
#
# The second-stage metro-FE projection is a descriptive decomposition. Its
# default standard errors are conditional on first-stage estimated fixed effects;
# the output explicitly labels this limitation.

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "dplyr", "fixest", "readr", "ggplot2", "scales"))
threads <- v3_configure_resources(TRUE)
set.seed(123)

INPUT <- v3_assert_dir(v3_env("REVELIO_V3_INPUT"), "REVELIO_V3_INPUT")
OUT <- v3_output_dir("metro_parent_productivity")
TAB <- file.path(OUT, "tables")
FIG <- file.path(OUT, "figures")
FE_DIR <- file.path(OUT, "fixed_effects")
DIAG <- file.path(OUT, "diagnostics")
dir.create(TAB, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG, recursive = TRUE, showWarnings = FALSE)
dir.create(FE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(DIAG, recursive = TRUE, showWarnings = FALSE)

YEAR_MIN <- as.integer(v3_env("METRO_YEAR_MIN", "2010"))
YEAR_MAX <- as.integer(v3_env("METRO_YEAR_MAX", "2019"))
US <- v3_env("REVELIO_US_COUNTRY", "United States")
MAX_TENURE <- 50L
RUN_DYNAMIC <- v3_bool(v3_env("METRO_RUN_DYNAMIC", "0"))

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

ds <- arrow::open_dataset(INPUT, format = "parquet")
required <- c(
  "user_id", "year", "n_patents", "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_metro_area"
)
v3_require_cols(names(ds), required, "core V3 panel")

# Full-panel metro size is intentionally built before the analytical
# US-position/US-educated restriction, preserving the original economic object.
msg("Reading minimal full-panel metro-size fields.")
size_dt <- ds |>
  dplyr::filter(year >= YEAR_MIN, year <= YEAR_MAX) |>
  dplyr::select(user_id, year, first_metro_area) |>
  dplyr::collect() |>
  data.table::as.data.table()
size_dt[, user_id := v3_clean_chr(user_id)]
size_dt[, first_metro_area := v3_clean_chr(first_metro_area)]
size_dt[, year := as.integer(year)]
size_dt <- unique(size_dt[!is.na(user_id) & !is.na(first_metro_area), .(user_id, year, metro_id = first_metro_area)])
metro_year_size <- size_dt[, .(metro_size_full_panel = .N), by = .(metro_id, year)]
metro_size <- metro_year_size[, .(
  avg_metro_size_full_panel = as.numeric(mean(metro_size_full_panel)),
  median_metro_size_full_panel = as.numeric(stats::median(metro_size_full_panel)),
  n_years_size_observed = as.integer(.N)
), by = metro_id]
metro_size[, log_avg_metro_size_full_panel := log(pmax(avg_metro_size_full_panel, 1))]
rm(size_dt)

msg("Reading analytical U.S.-position / U.S.-educated panel.")
dt <- ds |>
  dplyr::filter(
    year >= YEAR_MIN, year <= YEAR_MAX,
    first_country == US, last_university_country == US
  ) |>
  dplyr::select(dplyr::all_of(required)) |>
  dplyr::collect() |>
  data.table::as.data.table()

if (!nrow(dt)) stop("No analytical observations after U.S. position / U.S. education restriction.")
for (cc in intersect(c("user_id", "first_country", "last_university_country",
                       "first_startdate_edu", "first_startdate_pos",
                       "first_parent_rcid", "first_metro_area"), names(dt))) {
  data.table::set(dt, j = cc, value = v3_clean_chr(dt[[cc]]))
}
dt[, `:=`(year = as.integer(year), n_patents = suppressWarnings(as.numeric(n_patents)))]
dt[is.na(n_patents), n_patents := 0]
data.table::setorder(dt, user_id, year)
v3_assert_unique_user_year(dt, file.path(DIAG, "duplicate_user_years.csv"))
v3_compute_tenure(dt, MAX_TENURE)

# first_parent_rcid and first_metro_area are first observed within each year.
dt[, parent_id := first_parent_rcid]
dt[, metro_id := first_metro_area]
dt[, mp_id := ifelse(!is.na(parent_id) & !is.na(metro_id), paste(parent_id, metro_id, sep = "||"), NA_character_)]
dt[, lag_year := data.table::shift(year), by = user_id]
dt[, lag_n_patents := data.table::shift(n_patents), by = user_id]
dt[, lag_parent_id := data.table::shift(parent_id), by = user_id]
dt[, lag_metro_id := data.table::shift(metro_id), by = user_id]
dt[, lag_mp_id := data.table::shift(mp_id), by = user_id]
dt[, consecutive := !is.na(lag_year) & year == lag_year + 1L]
dt[, same_metro := consecutive & !is.na(metro_id) & metro_id == lag_metro_id]
dt[, same_mp := consecutive & !is.na(mp_id) & mp_id == lag_mp_id]
dt[, tenure_sq := tenure^2]
dt <- merge(dt, metro_size, by = "metro_id", all.x = TRUE, sort = FALSE)

samples <- list(
  full_same_metro = dt[
    same_metro & is.finite(tenure) & is.finite(tenure_sq) &
      !is.na(lag_parent_id) & !is.na(lag_mp_id) &
      is.finite(log_avg_metro_size_full_panel)
  ],
  stayers_same_metro_parent = dt[
    same_mp & is.finite(tenure) & is.finite(tenure_sq) &
      !is.na(lag_parent_id) & !is.na(lag_mp_id) &
      is.finite(log_avg_metro_size_full_panel)
  ]
)

specs <- list(
  baseline = "user_id + metro_id + year",
  lagged_parent_fe = "user_id + metro_id + year + lag_parent_id",
  lagged_metro_parent_fe = "user_id + metro_id + year + lag_mp_id"
)

out_models <- list()
out_fe <- list()
out_second <- list()
k <- 0L

extract_fe <- function(fit, d, sample_name, spec_name, model_type) {
  fe <- fixest::fixef(fit)
  if (!"metro_id" %in% names(fe)) return(data.table::data.table())
  z <- data.table::data.table(metro_id = names(fe$metro_id), metro_fe = as.numeric(fe$metro_id))
  support <- d[, .(n_obs_first_stage = .N, n_users_first_stage = data.table::uniqueN(user_id)), by = metro_id]
  z <- merge(z, support, by = "metro_id", all.x = TRUE)
  z <- merge(z, metro_size, by = "metro_id", all.x = TRUE)
  z[, `:=`(sample = sample_name, spec = spec_name, model_type = model_type)]
  z
}

run_second_stage <- function(z) {
  if (nrow(z) < 10L) return(data.table::data.table())
  out <- list()
  for (wt in c("unweighted", "first_stage_observation_weighted")) {
    fit <- tryCatch(
      if (wt == "unweighted") {
        fixest::feols(metro_fe ~ log_avg_metro_size_full_panel, data = z, vcov = "hetero", notes = FALSE, warn = FALSE)
      } else {
        fixest::feols(metro_fe ~ log_avg_metro_size_full_panel, data = z,
                      weights = ~n_obs_first_stage, vcov = "hetero", notes = FALSE, warn = FALSE)
      },
      error = function(e) NULL
    )
    if (is.null(fit)) next
    ct <- fixest::coeftable(fit)
    term <- "log_avg_metro_size_full_panel"
    out[[length(out) + 1L]] <- data.table::data.table(
      sample = z$sample[1], spec = z$spec[1], model_type = z$model_type[1],
      weighting = wt,
      n_metros = nrow(z),
      alpha = as.numeric(ct[term, 1]),
      alpha_se_conditional = as.numeric(ct[term, 2]),
      alpha_p_conditional = as.numeric(ct[term, 4]),
      alpha_ci_low_conditional = as.numeric(ct[term, 1] - 1.96 * ct[term, 2]),
      alpha_ci_high_conditional = as.numeric(ct[term, 1] + 1.96 * ct[term, 2]),
      se_interpretation = "heteroskedastic robust conditional on estimated first-stage metro FE"
    )
  }
  data.table::rbindlist(out)
}

for (sample_name in names(samples)) {
  d <- data.table::copy(samples[[sample_name]])
  if (nrow(d) < 100L || data.table::uniqueN(d$metro_id) < 10L) next

  model_defs <- list(static = "tenure + tenure_sq")
  if (RUN_DYNAMIC) model_defs$lagged_persistence_sensitivity <- "lag_n_patents + tenure + tenure_sq"

  for (model_type in names(model_defs)) {
    rhs <- model_defs[[model_type]]
    for (spec_name in names(specs)) {
      d_model <- if (model_type == "lagged_persistence_sensitivity") d[is.finite(lag_n_patents)] else d
      if (!nrow(d_model)) next
      fml <- stats::as.formula(paste0("n_patents ~ ", rhs, " | ", specs[[spec_name]]))
      fit <- tryCatch(
        fixest::fepois(
          fml, data = d_model, vcov = ~user_id + metro_id,
          fixef.rm = "perfect_fit", notes = FALSE, warn = FALSE
        ),
        error = function(e) {
          msg("Model failed:", sample_name, spec_name, model_type, "|", conditionMessage(e))
          NULL
        }
      )
      if (is.null(fit)) next

      ct <- fixest::coeftable(fit)
      k <- k + 1L
      out_models[[k]] <- data.table::data.table(
        sample = sample_name, spec = spec_name, model_type = model_type,
        formula = deparse(fml), n_obs = stats::nobs(fit),
        n_users = data.table::uniqueN(d_model$user_id), n_metros = data.table::uniqueN(d_model$metro_id),
        term = rownames(ct), estimate = as.numeric(ct[, 1]), se = as.numeric(ct[, 2]),
        statistic = as.numeric(ct[, 3]), p_value = as.numeric(ct[, 4]),
        cluster = "two-way: user_id + metro_id"
      )

      fe_z <- extract_fe(fit, d_model, sample_name, spec_name, model_type)
      if (nrow(fe_z)) {
        out_fe[[length(out_fe) + 1L]] <- fe_z
        out_second[[length(out_second) + 1L]] <- run_second_stage(fe_z)
      }
    }
  }
}

models <- data.table::rbindlist(out_models, fill = TRUE)
fe_all <- data.table::rbindlist(out_fe, fill = TRUE)
second <- data.table::rbindlist(out_second, fill = TRUE)
if (!nrow(models)) stop("No metro-parent productivity model completed.")

data.table::fwrite(models, file.path(TAB, "first_stage_coefficients.csv"))
data.table::fwrite(metro_size, file.path(TAB, "metro_size_full_panel.csv"))
data.table::fwrite(fe_all, file.path(FE_DIR, "metro_fixed_effects.csv"))
data.table::fwrite(second, file.path(TAB, "second_stage_metro_size_projection.csv"))

# Binned display avoids a visually uninformative cloud of metro fixed effects.
if (nrow(fe_all)) {
  bins <- data.table::copy(
    fe_all[is.finite(metro_fe) & is.finite(log_avg_metro_size_full_panel)]
  )
  bins[, size_bin := {
    r <- data.table::frank(log_avg_metro_size_full_panel, ties.method = "average")
    pmax(1L, pmin(25L, ceiling(25L * r / .N)))
  }, by = .(sample, spec, model_type)]
  bins <- bins[, .(
    mean_log_size = mean(log_avg_metro_size_full_panel),
    mean_metro_size = mean(avg_metro_size_full_panel),
    mean_metro_fe = mean(metro_fe),
    n_metros = .N
  ), by = .(sample, spec, model_type, size_bin)]
  data.table::fwrite(bins, file.path(TAB, "metro_fe_binned_for_display.csv"))

  p <- ggplot2::ggplot(
    bins[model_type == "static"],
    ggplot2::aes(x = mean_metro_size, y = mean_metro_fe, color = spec, group = spec)
  ) +
    ggplot2::geom_line(linewidth = 0.8) +
    ggplot2::geom_point(size = 1.5) +
    ggplot2::scale_x_log10(labels = scales::label_number(big.mark = ",")) +
    ggplot2::facet_wrap(~sample) +
    ggplot2::labs(
      title = "Metro fixed effects and average metro size",
      subtitle = "Static PPML first stage; binned display. Projection inference is conditional on estimated FE.",
      x = "Average metro size in full V3 panel (log scale)",
      y = "Estimated metro fixed effect", color = "First-stage specification"
    ) + v3_plot_theme()
  ggplot2::ggsave(file.path(FIG, "metro_fe_vs_size_static.png"), p, width = 12, height = 7, dpi = 250)
}

manifest <- data.table::data.table(
  input = INPUT, output = OUT, year_min = YEAR_MIN, year_max = YEAR_MAX,
  sample_definition = "first_country == United States AND last_university_country == United States",
  metro_size_universe = "all core V3 inventor-year observations in window, before analytical restriction",
  identifier_semantics = "first_parent_rcid and first_metro_area are first observed values within inventor-year",
  dynamic_lagged_model = RUN_DYNAMIC,
  second_stage_interpretation = "descriptive FE-size projection; robust SE conditional on first-stage FE",
  r_threads = threads
)
data.table::fwrite(manifest, file.path(DIAG, "manifest.csv"))

readr::write_lines(c(
  "# Reviewed metro-parent productivity decomposition",
  "",
  "- Analytical regression sample: U.S. position-years and U.S.-educated inventors.",
  "- Metro size: full V3 core panel in the selected window, retained as in the original design.",
  "- Primary first stage is static PPML; dynamic lagged-patenting models are optional sensitivity analyses.",
  "- Second-stage standard errors are conditional on estimated metro fixed effects and should not be read as causal 'air-effect' inference."
), file.path(OUT, "README.md"))

msg("[PASS] Metro-parent productivity decomposition completed. Outputs:", OUT)
