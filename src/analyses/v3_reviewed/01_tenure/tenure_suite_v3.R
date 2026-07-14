#!/usr/bin/env Rscript
# Reviewed tenure PPML suite.
#
# Main maintained choices:
# - first_parent_rcid / first_rcid / first_city / first_metro_area are treated
#   as the first observed spell/location within the inventor-year.
# - main sample: U.S. position-years AND U.S. last-degree-country.
# - plotted implied levels remain mean(n_patents in tenure 0-5) * exp(beta).
#
# Default runtime:
#   TENURE_WINDOW=2010_2019
#   TENURE_VARIANTS=base,seniority
#   TENURE_SPLITS=career
#   TENURE_FE_SET=core
#
# Optional extended splits:
#   TENURE_SPLITS=career,metro_size,metro_prod,density
# Optional full FE ladder:
#   TENURE_FE_SET=all

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "dplyr", "fixest", "readr", "ggplot2", "scales"))
threads <- v3_configure_resources(TRUE)
set.seed(123)

INPUT <- v3_assert_dir(v3_env("REVELIO_V3_INPUT"), "REVELIO_V3_INPUT")
WINDOW_TAG <- v3_env("TENURE_WINDOW", "2010_2019")
WINDOW <- v3_year_window(WINDOW_TAG)
VARIANTS <- trimws(strsplit(v3_env("TENURE_VARIANTS", "base,seniority"), ",", fixed = TRUE)[[1]])
SPLITS <- trimws(strsplit(v3_env("TENURE_SPLITS", "career"), ",", fixed = TRUE)[[1]])
FE_SET <- tolower(v3_env("TENURE_FE_SET", "core"))
CLUSTER_MODE <- tolower(v3_env("TENURE_CLUSTER_MODE", "two_way"))
OUT <- v3_output_dir(file.path("tenure", WINDOW_TAG))
TAB <- file.path(OUT, "tables")
FIG <- file.path(OUT, "figures")
DIAG <- file.path(OUT, "diagnostics")
dir.create(TAB, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG, recursive = TRUE, showWarnings = FALSE)
dir.create(DIAG, recursive = TRUE, showWarnings = FALSE)

US <- v3_env("REVELIO_US_COUNTRY", "United States")
MAX_TENURE <- 50L
TOP_P <- 0.20
REF_BIN <- "0-5"

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

required <- c(
  "user_id", "year", "n_patents",
  "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
)

ds <- arrow::open_dataset(INPUT, format = "parquet")
v3_require_cols(names(ds), required, "core V3 panel")
has_seniority <- "avg_seniority" %in% names(ds)

pull_cols <- unique(c(required, if (has_seniority) "avg_seniority"))
msg("Reading V3 tenure panel once for", WINDOW_TAG, "using", threads, "thread(s).")

dt <- ds |>
  dplyr::filter(
    year >= WINDOW[[1]],
    year <= WINDOW[[2]],
    first_country == US,
    last_university_country == US
  ) |>
  dplyr::select(dplyr::all_of(pull_cols)) |>
  dplyr::collect() |>
  data.table::as.data.table()

if (!nrow(dt)) stop("No observations after the US-position and US-educated sample restriction.")

char_cols <- intersect(c(
  "user_id", "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
), names(dt))
for (cc in char_cols) data.table::set(dt, j = cc, value = v3_clean_chr(dt[[cc]]))
dt[, year := as.integer(year)]
dt[, n_patents := suppressWarnings(as.numeric(n_patents))]
dt[is.na(n_patents), n_patents := 0]
if (has_seniority) dt[, avg_seniority := suppressWarnings(as.numeric(avg_seniority))]

v3_assert_unique_user_year(dt, file.path(DIAG, "duplicate_user_years.csv"))
data.table::setorder(dt, user_id, year)
v3_compute_tenure(dt, MAX_TENURE)

# The variables below preserve the user's documented semantics: they are the
# first observed parent/firm/city/metro associated with each inventor-year.
dt[, parent_id := first_parent_rcid]
dt[, firm_id := first_rcid]
dt[, city_id := first_city]
dt[, metro_id := first_metro_area]
dt[, mp_id := ifelse(!is.na(parent_id) & !is.na(metro_id), paste(parent_id, metro_id, sep = "||"), NA_character_)]
dt[, mp_year_id := ifelse(!is.na(mp_id), paste(mp_id, year, sep = "||"), NA_character_)]

common <- dt[
  is.finite(tenure) & !is.na(parent_id) & !is.na(metro_id) &
    !is.na(mp_id) & !is.na(mp_year_id) & is.finite(n_patents) & n_patents >= 0
]
if (!nrow(common)) stop("No observations satisfy common support for parent, metro, and metro-parent fixed effects.")
common[, tenure_bin := v3_tenure_bin(tenure, MAX_TENURE, 5L)]

sample_audit <- data.table::data.table(
  stage = c("V3 window", "US position + US education", "common FE support"),
  n_obs = c(nrow(dt), nrow(dt), nrow(common)),
  n_users = c(data.table::uniqueN(dt$user_id), data.table::uniqueN(dt$user_id), data.table::uniqueN(common$user_id)),
  min_year = c(min(dt$year), min(dt$year), min(common$year)),
  max_year = c(max(dt$year), max(dt$year), max(common$year))
)
data.table::fwrite(sample_audit, file.path(DIAG, "sample_audit.csv"))

# ---------------------------------------------------------------------------
# Fixed-effect specification ladders
# ---------------------------------------------------------------------------
base_specs <- list(
  baseline_user_year = "user_id + year",
  plus_parent_fe = "user_id + year + parent_id",
  plus_metro_fe = "user_id + year + metro_id",
  plus_mp_fe = "user_id + year + mp_id",
  plus_pmy_fe = "user_id + mp_year_id"
)

seniority_specs <- list(
  baseline_user_year = "user_id + year",
  plus_parent_seniority_fe = "user_id + year + parent_seniority_id",
  plus_mp_parent_seniority_fe = "user_id + year + mp_id + parent_seniority_id"
)

choose_specs <- function(specs) {
  if (FE_SET == "all") return(specs)
  if (identical(specs, base_specs)) {
    return(specs[c("baseline_user_year", "plus_mp_fe", "plus_pmy_fe")])
  }
  specs[c("baseline_user_year", "plus_parent_seniority_fe", "plus_mp_parent_seniority_fe")]
}

cluster_formula <- if (CLUSTER_MODE == "two_way") ~ user_id + mp_id else ~mp_id

get_fit_rows <- function(fit, d) {
  idx <- tryCatch(fixest::obs(fit), error = function(e) NULL)
  if (is.null(idx)) return(d)
  if (is.logical(idx) && length(idx) == nrow(d)) return(d[idx])
  if (is.numeric(idx)) return(d[idx])
  d
}

find_tenure_term <- function(terms, level) {
  exact <- paste0("tenure_bin::", level)
  if (exact %in% terms) return(exact)
  hit <- terms[grepl("tenure_bin", terms, fixed = TRUE) & grepl(level, terms, fixed = TRUE)]
  if (length(hit)) hit[[1]] else NA_character_
}

extract_profile <- function(fit, d, metadata) {
  lev <- v3_tenure_levels(MAX_TENURE, 5L)
  if (is.null(fit)) {
    return(data.table::data.table(
      analysis = metadata$analysis,
      group = metadata$group,
      variant = metadata$variant,
      spec = metadata$spec,
      tenure_bin = lev,
      estimate = NA_real_,
      se = NA_real_,
      ci_low = NA_real_,
      ci_high = NA_real_,
      mean_patents_ref_0_5 = NA_real_,
      implied_level = NA_real_,
      implied_level_low = NA_real_,
      implied_level_high = NA_real_,
      n_obs = nrow(d),
      n_users = data.table::uniqueN(d$user_id),
      n_clusters_mp = data.table::uniqueN(d$mp_id),
      status = "MODEL_FAILED"
    ))
  }

  dfit <- get_fit_rows(fit, d)
  ref <- dfit[as.character(tenure_bin) == REF_BIN]
  ref_mean <- if (nrow(ref)) mean(ref$n_patents, na.rm = TRUE) else NA_real_
  ct <- fixest::coeftable(fit)
  terms <- rownames(ct)

  out <- data.table::rbindlist(lapply(lev, function(z) {
    if (identical(z, REF_BIN)) {
      est <- se <- lo <- hi <- 0
    } else {
      term <- find_tenure_term(terms, z)
      if (is.na(term)) {
        est <- se <- lo <- hi <- NA_real_
      } else {
        est <- as.numeric(ct[term, 1])
        se <- as.numeric(ct[term, 2])
        lo <- est - 1.96 * se
        hi <- est + 1.96 * se
      }
    }
    data.table::data.table(
      analysis = metadata$analysis,
      group = metadata$group,
      variant = metadata$variant,
      spec = metadata$spec,
      tenure_bin = z,
      estimate = est,
      se = se,
      ci_low = lo,
      ci_high = hi,
      mean_patents_ref_0_5 = ref_mean,
      implied_level = ref_mean * exp(est),
      implied_level_low = ref_mean * exp(lo),
      implied_level_high = ref_mean * exp(hi),
      n_obs = stats::nobs(fit),
      n_users = data.table::uniqueN(dfit$user_id),
      n_clusters_mp = data.table::uniqueN(dfit$mp_id),
      status = "OK"
    )
  }), fill = TRUE)
  out
}

run_one_model <- function(d, fe_rhs, metadata) {
  fml <- stats::as.formula(
    paste0("n_patents ~ i(tenure_bin, ref = '", REF_BIN, "') | ", fe_rhs)
  )
  fit <- tryCatch(
    fixest::fepois(
      fml, data = d, vcov = cluster_formula,
      fixef.rm = "perfect_fit", notes = FALSE, warn = FALSE
    ),
    error = function(e) {
      msg("Model failed:", metadata$analysis, metadata$group, metadata$spec, "|", conditionMessage(e))
      NULL
    }
  )

  model_row <- data.table::data.table(
    analysis = metadata$analysis,
    group = metadata$group,
    variant = metadata$variant,
    spec = metadata$spec,
    formula = deparse(fml),
    n_input_obs = nrow(d),
    n_input_users = data.table::uniqueN(d$user_id),
    n_input_clusters_mp = data.table::uniqueN(d$mp_id),
    n_estimation_obs = if (is.null(fit)) NA_integer_ else stats::nobs(fit),
    status = if (is.null(fit)) "MODEL_FAILED" else "OK",
    collinear_dropped = if (!is.null(fit) && length(fit$collin.var)) paste(fit$collin.var, collapse = " | ") else ""
  )

  list(
    profile = extract_profile(fit, d, metadata),
    model = model_row
  )
}

support_table <- function(d, metadata) {
  d[, .(
    n_obs = .N,
    n_users = data.table::uniqueN(user_id),
    n_clusters_mp = data.table::uniqueN(mp_id),
    n_positive = sum(n_patents > 0, na.rm = TRUE),
    mean_patents = mean(n_patents, na.rm = TRUE)
  ), by = tenure_bin][, `:=`(
    analysis = metadata$analysis,
    group = metadata$group,
    variant = metadata$variant
  )][]
}

run_family <- function(d, variant, specs, analysis, split_var = NULL) {
  results <- list(profiles = list(), models = list(), support = list())
  k <- 0L

  groups <- "full"
  if (!is.null(split_var)) groups <- c("complement", "top20")

  for (g in groups) {
    dg <- if (identical(g, "full")) data.table::copy(d) else data.table::copy(d[get(split_var) == g])
    if (!nrow(dg)) next

    meta_base <- list(analysis = analysis, group = g, variant = variant)
    k <- k + 1L
    results$support[[k]] <- support_table(dg, meta_base)

    for (spec_name in names(specs)) {
      meta <- c(meta_base, list(spec = spec_name))
      rr <- run_one_model(dg, specs[[spec_name]], meta)
      results$profiles[[length(results$profiles) + 1L]] <- rr$profile
      results$models[[length(results$models) + 1L]] <- rr$model
    }
  }
  results
}

make_top20 <- function(x) {
  x <- as.numeric(x)
  q <- suppressWarnings(stats::quantile(x, probs = 1 - TOP_P, na.rm = TRUE, type = 7))
  out <- ifelse(is.finite(x) & x >= q, "top20",
                ifelse(is.finite(x), "complement", NA_character_))
  out
}

# ---------------------------------------------------------------------------
# Classification variables. Career classification is within the selected
# estimation window; optional metro classifications use one additional
# year-2000 Arrow read instead of a full 2000-2024 scan.
# ---------------------------------------------------------------------------
common[, career_mean_patents := mean(n_patents), by = user_id]
common[, career_n_obs := .N, by = user_id]
common[, career_topcomp := ifelse(career_n_obs >= 3L, make_top20(career_mean_patents), NA_character_)]

first_metro <- common[!is.na(metro_id), .(baseline_metro = metro_id[which.min(year)]), by = user_id]
common <- first_metro[common, on = "user_id"]

if (any(SPLITS %in% c("metro_size", "metro_prod"))) {
  metrics_2000 <- ds |>
    dplyr::filter(year == 2000L, first_country == US, last_university_country == US) |>
    dplyr::select(user_id, first_metro_area, n_patents) |>
    dplyr::collect() |>
    data.table::as.data.table()

  if (nrow(metrics_2000)) {
    metrics_2000[, user_id := v3_clean_chr(user_id)]
    metrics_2000[, first_metro_area := v3_clean_chr(first_metro_area)]
    metrics_2000[, n_patents := suppressWarnings(as.numeric(n_patents))]
    metro_2000 <- metrics_2000[!is.na(first_metro_area), .(
      metro_size_2000 = data.table::uniqueN(user_id),
      metro_prod_2000 = mean(n_patents, na.rm = TRUE)
    ), by = .(metro_id = first_metro_area)]
    metro_2000[, metro_size_topcomp := make_top20(metro_size_2000)]
    metro_2000[, metro_prod_topcomp := make_top20(metro_prod_2000)]
    common <- merge(
      common,
      metro_2000[, .(baseline_metro = metro_id, metro_size_topcomp, metro_prod_topcomp)],
      by = "baseline_metro", all.x = TRUE, sort = FALSE
    )
  } else {
    common[, `:=`(metro_size_topcomp = NA_character_, metro_prod_topcomp = NA_character_)]
  }
}

if ("density" %in% SPLITS) {
  crosswalk <- path.expand(v3_env("REVELIO_METRO_DENSITY_CROSSWALK"))
  if (!file.exists(crosswalk)) {
    stop("Density split requested but REVELIO_METRO_DENSITY_CROSSWALK is missing: ", crosswalk)
  }
  cw <- data.table::as.data.table(readr::read_csv(crosswalk, show_col_types = FALSE))
  v3_require_cols(names(cw), c("revelio_metro_raw", "density_per_sqmi"), "metro density crosswalk")
  cw[, metro_id := v3_clean_chr(revelio_metro_raw)]
  cw[, density_per_sqmi := suppressWarnings(as.numeric(density_per_sqmi))]
  cw <- cw[!is.na(metro_id)]
  cw <- cw[!duplicated(metro_id)]
  cw[, density_topcomp := make_top20(log(density_per_sqmi))]
  cw[!is.finite(density_per_sqmi) | density_per_sqmi <= 0, density_topcomp := NA_character_]
  common <- merge(
    common,
    cw[, .(baseline_metro = metro_id, density_topcomp)],
    by = "baseline_metro", all.x = TRUE, sort = FALSE
  )
}

# ---------------------------------------------------------------------------
# Run base and seniority variants.
# ---------------------------------------------------------------------------
all_profiles <- list()
all_models <- list()
all_support <- list()

if ("base" %in% VARIANTS) {
  msg("Running base tenure PPML suite.")
  base_out <- run_family(common, "base", choose_specs(base_specs), "FULL_SAMPLE")
  all_profiles <- c(all_profiles, base_out$profiles)
  all_models <- c(all_models, base_out$models)
  all_support <- c(all_support, base_out$support)

  split_map <- list(
    career = list(name = "CAREER_TOPCOMP", var = "career_topcomp"),
    metro_size = list(name = "BASELINE_METRO_SIZE_TOPCOMP", var = "metro_size_topcomp"),
    metro_prod = list(name = "BASELINE_METRO_PRODUCTIVITY_TOPCOMP", var = "metro_prod_topcomp"),
    density = list(name = "BASELINE_METRO_DENSITY_TOPCOMP", var = "density_topcomp")
  )
  for (sp in intersect(SPLITS, names(split_map))) {
    ss <- split_map[[sp]]
    if (!ss$var %in% names(common)) next
    msg("Running base split:", ss$name)
    rr <- run_family(common, "base", choose_specs(base_specs), ss$name, ss$var)
    all_profiles <- c(all_profiles, rr$profiles)
    all_models <- c(all_models, rr$models)
    all_support <- c(all_support, rr$support)
  }
}

if ("seniority" %in% VARIANTS) {
  if (!has_seniority) {
    msg("Skipping seniority suite: avg_seniority is not available in V3 core panel.")
  } else {
    senior <- data.table::copy(common)
    senior[, seniority_raw := suppressWarnings(as.numeric(avg_seniority))]
    senior[, seniority_bin := as.integer(round(seniority_raw))]
    senior[seniority_bin < 1L | seniority_bin > 7L, seniority_bin := NA_integer_]
    senior[, parent_seniority_id := ifelse(!is.na(parent_id) & !is.na(seniority_bin),
                                           paste(parent_id, seniority_bin, sep = "||seniority="), NA_character_)]
    senior <- senior[!is.na(seniority_bin) & !is.na(parent_seniority_id)]

    senior_diag <- senior[, .(
      n_obs = .N,
      n_users = data.table::uniqueN(user_id),
      mean_seniority = mean(seniority_raw, na.rm = TRUE),
      p95_abs_rounding_error = stats::quantile(abs(seniority_raw - seniority_bin), .95, na.rm = TRUE)
    ), by = seniority_bin]
    data.table::fwrite(senior_diag, file.path(DIAG, "seniority_support.csv"))

    msg("Running seniority-conditioned tenure PPML suite.")
    senior_out <- run_family(senior, "seniority", choose_specs(seniority_specs), "FULL_SAMPLE")
    all_profiles <- c(all_profiles, senior_out$profiles)
    all_models <- c(all_models, senior_out$models)
    all_support <- c(all_support, senior_out$support)
  }
}

profiles <- data.table::rbindlist(all_profiles, fill = TRUE)
models <- data.table::rbindlist(all_models, fill = TRUE)
support <- data.table::rbindlist(all_support, fill = TRUE)

if (!nrow(profiles)) stop("No tenure models produced profile output.")

data.table::fwrite(profiles, file.path(TAB, "tenure_profiles.csv"))
data.table::fwrite(models, file.path(TAB, "tenure_model_registry.csv"))
data.table::fwrite(support, file.path(DIAG, "tenure_support.csv"))

# Fixed-effect ladder decomposition uses no extra regressions.
ladder <- profiles[
  status == "OK" & analysis == "FULL_SAMPLE" & group == "full",
  .(
    reference_implied_level = implied_level[tenure_bin == REF_BIN][1],
    peak_implied_level = max(implied_level, na.rm = TRUE),
    peak_tenure_bin = tenure_bin[which.max(implied_level)],
    peak_minus_reference = max(implied_level, na.rm = TRUE) - implied_level[tenure_bin == REF_BIN][1],
    peak_ratio_to_reference = max(implied_level, na.rm = TRUE) / implied_level[tenure_bin == REF_BIN][1]
  ),
  by = .(variant, spec)
]
data.table::fwrite(ladder, file.path(TAB, "tenure_fe_ladder_summary.csv"))

# Only core, interpretable figures by default.
plot_specs <- intersect(
  c("baseline_user_year", "plus_mp_fe", "plus_pmy_fe", "plus_mp_parent_seniority_fe"),
  unique(profiles$spec)
)
plot_families <- unique(profiles$analysis)
for (family in plot_families) {
  z <- profiles[
    analysis == family & spec %in% plot_specs & status == "OK" &
      is.finite(implied_level)
  ]
  if (!nrow(z)) next

  z[, tenure_bin := factor(tenure_bin, levels = v3_tenure_levels(MAX_TENURE, 5L), ordered = TRUE)]
  z[, label := ifelse(group == "full", paste(variant, spec, sep = " | "),
                      paste(variant, group, spec, sep = " | "))]

  p <- ggplot2::ggplot(z, ggplot2::aes(x = tenure_bin, y = implied_level, group = label, color = label)) +
    ggplot2::geom_line(linewidth = 0.85, na.rm = TRUE) +
    ggplot2::geom_point(size = 1.4, na.rm = TRUE) +
    ggplot2::geom_errorbar(ggplot2::aes(ymin = implied_level_low, ymax = implied_level_high),
                           width = 0.15, alpha = 0.45, na.rm = TRUE) +
    ggplot2::labs(
      title = paste("Tenure patenting profiles:", family, "(", WINDOW_TAG, ")"),
      subtitle = "Implied level = mean patents in the 0–5 reference bin × exp(PPML coefficient)",
      x = "Tenure bin", y = "Implied patents", color = NULL
    ) +
    v3_plot_theme()
  ggplot2::ggsave(file.path(FIG, paste0("tenure_profiles__", gsub("[^A-Za-z0-9]+", "_", family), ".png")),
                  p, width = 12, height = 7, dpi = 250)
}

manifest <- data.table::data.table(
  window = WINDOW_TAG,
  input = INPUT,
  output = OUT,
  variants = paste(VARIANTS, collapse = ","),
  splits = paste(SPLITS, collapse = ","),
  fe_set = FE_SET,
  cluster_mode = CLUSTER_MODE,
  sample_definition = "first_country == United States AND last_university_country == United States",
  within_year_id_semantics = "first_parent_rcid/first_rcid/first_city/first_metro_area are first observed values within inventor-year",
  plot_scale = "mean(n_patents | tenure 0-5, estimation sample) * exp(beta)",
  density_crosswalk = v3_env("REVELIO_METRO_DENSITY_CROSSWALK"),
  r_threads = threads
)
data.table::fwrite(manifest, file.path(DIAG, "manifest.csv"))

msg("[PASS] Reviewed tenure suite completed. Outputs:", OUT)
