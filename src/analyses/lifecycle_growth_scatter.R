#!/usr/bin/env Rscript

##############################################################################
# lifecycle_growth_scatter.R
#
# Purpose
#   This script asks whether places where inventors' patenting profiles grow
#   more over the lifecycle are also more productive on average. It produces
#   scatter plots at two levels of aggregation: metro areas and metro-parent
#   clusters.
#
# Input
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2
#
# Main workflow
#   1. Load the inventor-year panel for the configured year window.
#   2. Restrict to the U.S. sample using the same logic as the lifecycle code
#      when the relevant columns are present.
#   3. Construct tenure_main using the lifecycle-code convention:
#        - year - first_startdate_edu + 3;
#        - fallback to year - first_startdate_pos;
#        - fallback to year - first observed year.
#      Keep valid tenure values between 0 and 50.
#   4. Construct five-year tenure bins: 0-5, 6-10, ..., 46-50, and attach each
#      bin midpoint.
#   5. Construct cluster identifiers:
#        - metro_id = first_metro_area;
#        - parent_id = first_parent_rcid;
#        - mp_fe_id = paste0(first_parent_rcid, "__", first_metro_area),
#          matching the PPML ladder's metro-parent fixed-effect logic.
#   6. For each metro and metro-parent cluster, compute full-window productivity:
#        - patents per inventor-year;
#        - total patents.
#   7. For each cluster x tenure-bin cell, compute mean patenting intensity.
#   8. Estimate lifecycle growth separately for two windows:
#        - early_0_25: tenure-bin midpoints from 0 to 25;
#        - late_26_50: tenure-bin midpoints from 26 to 50.
#      Growth is a support-weighted OLS slope of
#        log(1 + patents per inventor-year in the cluster-bin cell)
#      on tenure-bin midpoint, multiplied by the observed tenure-span of that
#      window. Weights are the number of inventor-year observations in the
#      cluster-bin cell.
#   9. Save scatter plots where each point is one cluster:
#        x-axis: early or late lifecycle growth;
#        y-axis: full-window productivity.
#      The script produces both y-axis versions for both cluster levels:
#        - patents per inventor-year;
#        - total patents.
#  10. Save the underlying cluster-productivity tables, cluster-bin lifecycle
#      tables, combined growth-productivity tables, diagnostics, and manifest.
#
# Output
#   /home/epiga/revelio_labs/output/lifecycle_growth_scatter
#
# Logging
#   The script prints progress at every major stage, including selected schema,
#   row/user counts after filters, tenure support, cluster support, growth-window
#   support, table writes, scatter-plot saves/skips, and final output paths.
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

pkgs <- c("arrow", "data.table", "ggplot2", "dplyr", "readr", "stringr", "scales")
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
})

options(arrow.skip_nul = TRUE)
options(bitmapType = "cairo")
set.seed(123)
data.table::setDTthreads(1)

# =========================================================
# Configuration
# =========================================================
CONFIG <- list(
  input_dir = "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2",
  out_dir   = "/home/epiga/revelio_labs/output/lifecycle_growth_scatter",

  # Kept aligned with the attached lifecycle code. Change these if you want the
  # PPML-style 2000-2019 window instead.
  year_min = 2010L,
  year_max = 2019L,

  us_only = TRUE,
  us_country = "United States",
  tenure_max = 50L,
  tenure_bin_step = 5L,

  # Support restrictions. These keep the scatter from being dominated by tiny
  # cluster x tenure cells.
  min_cluster_obs = 50L,
  min_cluster_users = 10L,
  min_bin_obs = 10L,
  min_bins_in_growth_window = 3L,

  # The total-patent scatter is usually extremely skewed. The plotted variable
  # remains total patents, but the y-axis is log10-scaled when this is TRUE.
  log10_total_patent_axis = TRUE,

  # Number of equal-width productivity bins for the binned growth-productivity plots.
  # Bins are equal-width in log1p(productivity), which is much more meaningful
  # than raw equal-width bins because patent counts are extremely skewed.
  n_productivity_bins = 20L

)

OUT <- list(
  root = CONFIG$out_dir,
  tables = file.path(CONFIG$out_dir, "tables"),
  plots = file.path(CONFIG$out_dir, "plots"),
  diagnostics = file.path(CONFIG$out_dir, "diagnostics")
)
invisible(lapply(OUT, dir.create, recursive = TRUE, showWarnings = FALSE))

log_msg <- function(...) {
  cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))
  flush.console()
}

print_config <- function(cfg) {
  log_msg("Configuration summary:")
  for (nm in names(cfg)) {
    val <- cfg[[nm]]
    if (length(val) > 1L) val <- paste(val, collapse = ", ")
    log_msg("  ", nm, "=", as.character(val))
  }
}

null_if_empty <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "NULL", "N/A")] <- NA_character_
  x
}

extract_year <- function(x) suppressWarnings(as.integer(substr(as.character(x), 1, 4)))

valid_tenure <- function(x, max_tenure) {
  out <- suppressWarnings(as.numeric(x))
  out[is.na(out) | out < 0 | out > max_tenure] <- NA_real_
  out
}

ordered_tenure_bin_levels <- function(max_tenure = CONFIG$tenure_max, step = CONFIG$tenure_bin_step) {
  lows <- seq(0L, max_tenure - step, by = step)
  highs <- seq(step, max_tenure, by = step)
  lows_label <- ifelse(lows == 0L, 0L, lows + 1L)
  paste0(lows_label, "-", highs)
}

make_tenure_bin <- function(x, max_tenure = CONFIG$tenure_max, step = CONFIG$tenure_bin_step) {
  x <- suppressWarnings(as.numeric(x))
  breaks <- c(-Inf, seq(step, max_tenure, by = step))
  labels <- ordered_tenure_bin_levels(max_tenure, step)
  out <- cut(x, breaks = breaks, labels = labels, include.lowest = TRUE, right = TRUE)
  factor(as.character(out), levels = labels, ordered = TRUE)
}

tenure_bin_midpoint <- function(bin_chr) {
  z <- as.character(bin_chr)
  lo <- suppressWarnings(as.numeric(sub("-.*$", "", z)))
  hi <- suppressWarnings(as.numeric(sub("^.*-", "", z)))
  (lo + hi) / 2
}

safe_name <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", as.character(x))
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "X", x)
}

standard_theme <- function() {
  theme_minimal(base_size = 12) +
    theme(
      panel.grid.minor = element_blank(),
      plot.title = element_text(face = "bold"),
      legend.position = "bottom"
    )
}

write_dt <- function(dt, path) {
  z <- as.data.table(dt)
  log_msg("Writing table:", path, "| rows:", nrow(z), "| cols:", ncol(z))
  fwrite(z, path)
}

# =========================================================
# Load data
# =========================================================
log_msg("Script started: lifecycle_growth_scatter.R")
log_msg("R version:", R.version.string)
print_config(CONFIG)
log_msg("Output folders created under:", OUT$root)
log_msg("Opening parquet dataset:", CONFIG$input_dir)
ds <- open_dataset(CONFIG$input_dir, format = "parquet")
available <- names(ds)
log_msg("Dataset schema has", length(available), "columns.")

need <- c(
  "user_id", "year", "n_patents",
  "first_country", "last_country", "last_university_country",
  "first_startdate_pos", "first_startdate_edu",
  "first_metro_area", "first_parent_rcid"
)
selected_cols <- intersect(need, available)
log_msg("Selected", length(selected_cols), "columns for this analysis:", paste(selected_cols, collapse = ", "))
missing_optional <- setdiff(need, selected_cols)
if (length(missing_optional) > 0L) log_msg("Requested columns not found and skipped:", paste(missing_optional, collapse = ", "))
missing_required <- setdiff(c("user_id", "year", "n_patents", "first_metro_area", "first_parent_rcid"), selected_cols)
if (length(missing_required) > 0) {
  stop("Missing required columns: ", paste(missing_required, collapse = ", "))
}

write_dt(
  data.table(variable = selected_cols),
  file.path(OUT$diagnostics, "selected_columns.csv")
)

log_msg("Collecting rows:", CONFIG$year_min, "to", CONFIG$year_max)
raw <- ds %>%
  select(all_of(selected_cols)) %>%
  filter(year >= CONFIG$year_min, year <= CONFIG$year_max) %>%
  collect() %>%
  as.data.table()

log_msg("Collected raw rows:", nrow(raw), "| users:", uniqueN(raw$user_id))

rm(ds)
gc()

for (cc in names(raw)) {
  if (is.character(raw[[cc]]) || is.factor(raw[[cc]])) {
    set(raw, j = cc, value = null_if_empty(raw[[cc]]))
  }
}

raw[, user_id := as.character(user_id)]
raw[, year := suppressWarnings(as.integer(year))]
raw[, n_patents := suppressWarnings(as.numeric(n_patents))]
raw[is.na(n_patents), n_patents := 0]

if (CONFIG$us_only) {
  log_msg("Applying U.S. sample restriction.")
  if ("first_country" %in% names(raw) && "last_university_country" %in% names(raw)) {
    raw <- raw[
      first_country == CONFIG$us_country &
        !is.na(last_university_country) &
        last_university_country == CONFIG$us_country
    ]
  } else if ("first_country" %in% names(raw)) {
    raw <- raw[first_country == CONFIG$us_country]
  }
}

setorder(raw, user_id, year)

log_msg("Rows after filters:", nrow(raw))
log_msg("Users after filters:", uniqueN(raw$user_id))

# =========================================================
# Tenure and cluster ids
# =========================================================
raw[, edu_year := extract_year(first_startdate_edu)]
raw[, pos_year := extract_year(first_startdate_pos)]
raw[, first_obs_year := min(year, na.rm = TRUE), by = user_id]

raw[, tenure_edu3      := valid_tenure(year - edu_year + 3L, CONFIG$tenure_max)]
raw[, tenure_pos       := valid_tenure(year - pos_year, CONFIG$tenure_max)]
raw[, tenure_first_obs := valid_tenure(year - first_obs_year, CONFIG$tenure_max)]

raw[, tenure_main := tenure_edu3]
raw[is.na(tenure_main), tenure_main := tenure_pos]
raw[is.na(tenure_main), tenure_main := tenure_first_obs]
raw[, tenure_bin := make_tenure_bin(tenure_main)]
raw[, tenure_mid := tenure_bin_midpoint(tenure_bin)]
log_msg("Constructed tenure variables. Non-missing tenure rows:", sum(is.finite(raw$tenure_main)),
        "| users:", uniqueN(raw$user_id[is.finite(raw$tenure_main)]))
print(raw[!is.na(tenure_bin), .(n_rows = .N, n_users = uniqueN(user_id), total_patents = sum(n_patents, na.rm = TRUE)), by = tenure_bin][order(tenure_bin)])

raw[, metro_id := trimws(as.character(first_metro_area))]
raw[metro_id == "", metro_id := NA_character_]

raw[, parent_id := trimws(as.character(first_parent_rcid))]
raw[parent_id == "", parent_id := NA_character_]

# This mirrors the PPML script's metro-parent definition: parent__metro.
raw[, mp_fe_id := fifelse(
  !is.na(parent_id) & !is.na(metro_id),
  paste0(parent_id, "__", metro_id),
  NA_character_
)]

analysis_dt <- raw[is.finite(tenure_main) & !is.na(tenure_bin)]
log_msg("Analysis sample rows:", nrow(analysis_dt), "| users:", uniqueN(analysis_dt$user_id),
        "| metros:", uniqueN(analysis_dt$metro_id[!is.na(analysis_dt$metro_id)]),
        "| metro-parent clusters:", uniqueN(analysis_dt$mp_fe_id[!is.na(analysis_dt$mp_fe_id)]))

write_dt(
  analysis_dt[, .(
    n_rows = .N,
    n_users = uniqueN(user_id),
    total_patents = sum(n_patents, na.rm = TRUE),
    n_metros = uniqueN(metro_id[!is.na(metro_id)]),
    n_metro_parent_clusters = uniqueN(mp_fe_id[!is.na(mp_fe_id)])
  )],
  file.path(OUT$diagnostics, "analysis_sample_summary.csv")
)

write_dt(
  analysis_dt[, .(
    n_rows = .N,
    n_users = uniqueN(user_id),
    total_patents = sum(n_patents, na.rm = TRUE)
  ), by = tenure_bin][order(tenure_bin)],
  file.path(OUT$diagnostics, "tenure_bin_support.csv")
)

# =========================================================
# Cluster lifecycle-growth construction
# =========================================================
make_cluster_lifecycle <- function(x, cluster_var, cluster_label) {
  log_msg("Constructing cluster lifecycle:", cluster_label, "using", cluster_var)
  z <- copy(x)[!is.na(get(cluster_var)) & trimws(as.character(get(cluster_var))) != ""]
  if (!nrow(z)) {
    log_msg("No rows available for cluster lifecycle:", cluster_label)
    return(list(productivity = data.table(), bins = data.table()))
  }

  z[, cluster_id := as.character(get(cluster_var))]
  log_msg("Rows with valid cluster:", nrow(z), "| clusters before support filters:", uniqueN(z$cluster_id))

  productivity <- z[, .(
    cluster_label = cluster_label,
    n_obs = .N,
    n_users = uniqueN(user_id),
    total_patents = sum(n_patents, na.rm = TRUE),
    patents_per_inventor_year = sum(n_patents, na.rm = TRUE) / .N
  ), by = cluster_id]

  productivity <- productivity[
    n_obs >= CONFIG$min_cluster_obs &
      n_users >= CONFIG$min_cluster_users
  ]

  log_msg("Clusters after support filters for", cluster_label, ":", nrow(productivity))
  if (!nrow(productivity)) return(list(productivity = productivity, bins = data.table()))

  z <- z[cluster_id %in% productivity$cluster_id]

  bins <- z[, .(
    cluster_label = cluster_label,
    n_obs_bin = .N,
    n_users_bin = uniqueN(user_id),
    total_patents_bin = sum(n_patents, na.rm = TRUE),
    patents_per_inventor_year_bin = sum(n_patents, na.rm = TRUE) / .N,
    mean_patents_per_inventor_year_bin = mean(n_patents, na.rm = TRUE)
  ), by = .(cluster_id, tenure_bin, tenure_mid)]

  bins <- bins[order(cluster_id, tenure_bin)]
  log_msg("Built cluster-bin table for", cluster_label, "| rows:", nrow(bins))

  list(productivity = productivity, bins = bins)
}

estimate_growth_window <- function(bin_dt, window_tag, min_mid, max_mid) {
  log_msg("Estimating growth window:", window_tag, "| tenure-midpoint range:", min_mid, "to", max_mid)
  if (!nrow(bin_dt)) {
    log_msg("Skipping growth window", window_tag, "because cluster-bin table is empty.")
    return(data.table())
  }

  sub <- copy(bin_dt)[
    is.finite(tenure_mid) & tenure_mid >= min_mid & tenure_mid <= max_mid &
      n_obs_bin >= CONFIG$min_bin_obs &
      is.finite(patents_per_inventor_year_bin)
  ]

  log_msg("Rows in growth-window support sample:", nrow(sub), "| clusters:", uniqueN(sub$cluster_id))
  if (!nrow(sub)) return(data.table())

  out <- sub[, {
    ok <- is.finite(tenure_mid) & is.finite(patents_per_inventor_year_bin) & is.finite(n_obs_bin)
    xx <- tenure_mid[ok]
    yy <- log1p(patents_per_inventor_year_bin[ok])
    ww <- n_obs_bin[ok]

    if (length(unique(xx)) < 2L || length(xx) < CONFIG$min_bins_in_growth_window) {
      list(
        growth_window = window_tag,
        growth_log_points = NA_real_,
        growth_slope_per_year = NA_real_,
        predicted_start_log = NA_real_,
        predicted_end_log = NA_real_,
        predicted_level_change = NA_real_,
        n_bins_growth = length(unique(xx)),
        n_obs_growth = sum(ww, na.rm = TRUE)
      )
    } else {
      fit <- tryCatch(lm(yy ~ xx, weights = ww), error = function(e) NULL)
      if (is.null(fit) || any(!is.finite(coef(fit)))) {
        list(
          growth_window = window_tag,
          growth_log_points = NA_real_,
          growth_slope_per_year = NA_real_,
          predicted_start_log = NA_real_,
          predicted_end_log = NA_real_,
          predicted_level_change = NA_real_,
          n_bins_growth = length(unique(xx)),
          n_obs_growth = sum(ww, na.rm = TRUE)
        )
      } else {
        b0 <- unname(coef(fit)[1])
        b1 <- unname(coef(fit)[2])
        x0 <- min(xx, na.rm = TRUE)
        x1 <- max(xx, na.rm = TRUE)
        y0 <- b0 + b1 * x0
        y1 <- b0 + b1 * x1
        list(
          growth_window = window_tag,
          growth_log_points = b1 * (x1 - x0),
          growth_slope_per_year = b1,
          predicted_start_log = y0,
          predicted_end_log = y1,
          predicted_level_change = expm1(y1) - expm1(y0),
          n_bins_growth = length(unique(xx)),
          n_obs_growth = sum(ww, na.rm = TRUE)
        )
      }
    }
  }, by = .(cluster_label, cluster_id)]

  out <- out[is.finite(growth_log_points)]
  log_msg("Estimated growth window", window_tag, "| valid clusters:", nrow(out))
  out
}

build_growth_productivity_table <- function(x, cluster_var, cluster_label) {
  lifecycle <- make_cluster_lifecycle(x, cluster_var, cluster_label)

  write_dt(
    lifecycle$productivity,
    file.path(OUT$tables, paste0(cluster_label, "_cluster_productivity.csv"))
  )
  write_dt(
    lifecycle$bins,
    file.path(OUT$tables, paste0(cluster_label, "_cluster_lifecycle_bins.csv"))
  )

  early <- estimate_growth_window(
    lifecycle$bins,
    window_tag = "early_0_25",
    min_mid = 0,
    max_mid = 25
  )

  late <- estimate_growth_window(
    lifecycle$bins,
    window_tag = "late_26_50",
    min_mid = 26,
    max_mid = 50
  )

  growth <- rbindlist(list(early, late), fill = TRUE)
  if (!nrow(growth)) return(data.table())

  out <- merge(
    growth,
    lifecycle$productivity,
    by = c("cluster_label", "cluster_id"),
    all.x = TRUE,
    sort = FALSE
  )

  out[, log1p_total_patents := log1p(total_patents)]
  out[, log1p_patents_per_inventor_year := log1p(patents_per_inventor_year)]
  out[]
}

log_msg("Building metro growth-productivity table")
metro_gp <- build_growth_productivity_table(analysis_dt, "metro_id", "metro")
write_dt(metro_gp, file.path(OUT$tables, "metro_growth_productivity.csv"))

log_msg("Building metro-parent growth-productivity table")
mp_gp <- build_growth_productivity_table(analysis_dt, "mp_fe_id", "metro_parent")
write_dt(mp_gp, file.path(OUT$tables, "metro_parent_growth_productivity.csv"))

combined_gp <- rbindlist(list(metro_gp, mp_gp), fill = TRUE)
write_dt(combined_gp, file.path(OUT$tables, "combined_growth_productivity.csv"))

# =========================================================
# Binned productivity-growth plots
# =========================================================

weighted_mean_safe <- function(x, w) {
  ok <- is.finite(x) & is.finite(w) & w > 0
  if (!any(ok)) return(NA_real_)
  weighted.mean(x[ok], w[ok])
}

weighted_se_safe <- function(x, w) {
  ok <- is.finite(x) & is.finite(w) & w > 0
  x <- x[ok]
  w <- w[ok]

  if (length(x) < 2L) return(NA_real_)

  wm <- weighted.mean(x, w)
  v <- sum(w * (x - wm)^2) / sum(w)

  # Kish effective sample size. This is only descriptive, not causal inference.
  n_eff <- sum(w)^2 / sum(w^2)
  if (!is.finite(n_eff) || n_eff <= 1) return(NA_real_)

  sqrt(v / n_eff)
}

pretty_productivity_labels <- function(x) {
  vals <- expm1(x)
  ifelse(
    vals < 1,
    scales::number(vals, accuracy = 0.01),
    scales::comma(round(vals, 1))
  )
}

format_pvalue <- function(p) {
  if (!is.finite(p)) return("NA")
  if (p < 0.001) return("<0.001")
  scales::number(p, accuracy = 0.001)
}

estimate_raw_wls_beta <- function(z) {
  zreg <- copy(z)[
    is.finite(growth_log_points) &
      is.finite(productivity_log) &
      is.finite(weight_for_plot) &
      weight_for_plot > 0
  ]

  if (nrow(zreg) < 3L || length(unique(zreg$productivity_log)) < 2L) {
    return(data.table(
      beta = NA_real_,
      se = NA_real_,
      p_value = NA_real_,
      n_raw_clusters = nrow(zreg),
      regression_weight = "n_obs_growth",
      x_variable = "log1p(productivity)",
      y_variable = "growth_log_points"
    ))
  }

  fit <- tryCatch(
    lm(growth_log_points ~ productivity_log, data = zreg, weights = weight_for_plot),
    error = function(e) NULL
  )

  if (is.null(fit)) {
    return(data.table(
      beta = NA_real_,
      se = NA_real_,
      p_value = NA_real_,
      n_raw_clusters = nrow(zreg),
      regression_weight = "n_obs_growth",
      x_variable = "log1p(productivity)",
      y_variable = "growth_log_points"
    ))
  }

  X <- model.matrix(fit)
  e <- resid(fit)
  w <- weights(fit)

  n <- nrow(X)
  k <- ncol(X)

  bread <- tryCatch(
    solve(crossprod(X, X * w)),
    error = function(e) NULL
  )

  if (is.null(bread) || n <= k) {
    return(data.table(
      beta = unname(coef(fit)["productivity_log"]),
      se = NA_real_,
      p_value = NA_real_,
      n_raw_clusters = nrow(zreg),
      regression_weight = "n_obs_growth",
      x_variable = "log1p(productivity)",
      y_variable = "growth_log_points"
    ))
  }

  # HC1 robust variance for weighted least squares.
  meat <- crossprod(X, X * ((w * e)^2))
  vcov_hc1 <- (n / (n - k)) * bread %*% meat %*% bread

  beta <- unname(coef(fit)["productivity_log"])
  se <- sqrt(diag(vcov_hc1))[["productivity_log"]]
  t_stat <- beta / se
  p_value <- 2 * pt(abs(t_stat), df = n - k, lower.tail = FALSE)

  data.table(
    beta = beta,
    se = se,
    p_value = p_value,
    n_raw_clusters = nrow(zreg),
    regression_weight = "n_obs_growth",
    x_variable = "log1p(productivity)",
    y_variable = "growth_log_points"
  )
}

plot_productivity_binned_growth <- function(
    tab,
    cluster_label,
    window_tag,
    xvar,
    xlab,
    path,
    n_bins = CONFIG$n_productivity_bins,
    bin_method = c("equal_width_log", "equal_count_clusters")
) {
  bin_method <- match.arg(bin_method)
  bin_method_arg <- bin_method

  cl_arg <- cluster_label
  ww_arg <- window_tag

  z <- copy(tab)[
    cluster_label == cl_arg &
      growth_window == ww_arg &
      is.finite(growth_log_points) &
      is.finite(get(xvar)) &
      get(xvar) >= 0
  ]

  if (!nrow(z)) {
    log_msg("Skipping binned productivity-growth plot:", path, "| reason: no valid rows")
    return(invisible(NULL))
  }

  z[, productivity_raw := get(xvar)]
  z[, productivity_log := log1p(productivity_raw)]

  # Weight the binned mean by the support behind the lifecycle-growth estimate.
  # Fallback to cluster-level observations if needed.
  z[, weight_for_plot := fifelse(
    is.finite(n_obs_growth) & n_obs_growth > 0,
    n_obs_growth,
    fifelse(is.finite(n_obs) & n_obs > 0, n_obs, 1)
  )]

  if (length(unique(z$productivity_log[is.finite(z$productivity_log)])) < 2L) {
    log_msg("Skipping binned productivity-growth plot:", path, "| reason: productivity has no variation")
    return(invisible(NULL))
  }

  if (bin_method_arg == "equal_width_log") {
    prod_range <- range(z$productivity_log, na.rm = TRUE)
    breaks <- seq(prod_range[1], prod_range[2], length.out = n_bins + 1L)

    z[, productivity_bin := as.integer(cut(
      productivity_log,
      breaks = breaks,
      include.lowest = TRUE,
      right = TRUE,
      labels = FALSE
    ))]
  }

  if (bin_method_arg == "equal_count_clusters") {
    # Equal-sized bins by number of raw cluster observations.
    # This is usually the clearest visual diagnostic because each bin has
    # roughly the same number of clusters behind it.
    setorder(z, productivity_log, cluster_id)
    z[, productivity_rank := seq_len(.N)]
    z[, productivity_bin := pmin(
      n_bins,
      pmax(1L, ceiling(productivity_rank * n_bins / .N))
    )]
  }

  z <- z[!is.na(productivity_bin)]

  # For both binning methods, place the bin on the x-axis at the
  # support-weighted average productivity inside the bin.
  z[, productivity_bin_lo_log := min(productivity_log, na.rm = TRUE), by = productivity_bin]
  z[, productivity_bin_hi_log := max(productivity_log, na.rm = TRUE), by = productivity_bin]
  z[, productivity_bin_mid_log := weighted_mean_safe(productivity_log, weight_for_plot), by = productivity_bin]
  z[, productivity_bin_mid_raw := expm1(productivity_bin_mid_log)]

  z[, bin_method := bin_method_arg]

  z[, productivity_variable := xvar]

  bin_dt <- z[, .(
    n_clusters_bin = .N,
    support_weight_bin = sum(weight_for_plot, na.rm = TRUE),
    n_users_bin_sum = sum(n_users, na.rm = TRUE),

    productivity_bin_mid_log = mean(productivity_bin_mid_log, na.rm = TRUE),
    productivity_bin_mid_raw = mean(productivity_bin_mid_raw, na.rm = TRUE),
    productivity_raw_mean_weighted = weighted_mean_safe(productivity_raw, weight_for_plot),

    growth_mean_weighted = weighted_mean_safe(growth_log_points, weight_for_plot),
    growth_mean_unweighted = mean(growth_log_points, na.rm = TRUE),
    growth_se_weighted = weighted_se_safe(growth_log_points, weight_for_plot)
  ), by = .(
    cluster_label,
    growth_window,
    productivity_variable,
    bin_method,
    productivity_bin,
    productivity_bin_lo_log,
    productivity_bin_hi_log
  )]

  bin_dt[, growth_ci_low := growth_mean_weighted - 1.96 * growth_se_weighted]
  bin_dt[, growth_ci_high := growth_mean_weighted + 1.96 * growth_se_weighted]

  setorder(bin_dt, productivity_bin)

  bin_table_path <- file.path(
    OUT$tables,
    paste0(cluster_label, "__", window_tag, "__", xvar, "__", bin_method_arg, "__productivity_bins.csv")
  )
  write_dt(bin_dt, bin_table_path)

  pearson_raw <- suppressWarnings(cor(
    z$productivity_raw,
    z$growth_log_points,
    use = "complete.obs"
  ))

  spearman_raw <- suppressWarnings(cor(
    z$productivity_raw,
    z$growth_log_points,
    method = "spearman",
    use = "complete.obs"
  ))

  raw_reg <- estimate_raw_wls_beta(z)
  raw_reg[, `:=`(
    cluster_label = cl_arg,
    growth_window = ww_arg,
    productivity_variable = xvar
  )]

  raw_reg_path <- file.path(
    OUT$tables,
    paste0(cluster_label, "__", window_tag, "__", xvar, "__raw_wls_growth_on_productivity.csv")
  )
  write_dt(raw_reg, raw_reg_path)

  reg_label <- if (is.finite(raw_reg$beta) && is.finite(raw_reg$se) && is.finite(raw_reg$p_value)) {
    paste0(
      "Raw WLS regression\n",
      "growth = α + β log(1 + productivity)\n",
      "β = ", scales::number(raw_reg$beta, accuracy = 0.001), "\n",
      "SE = ", scales::number(raw_reg$se, accuracy = 0.001), "\n",
      "p = ", format_pvalue(raw_reg$p_value), "\n",
      "N = ", raw_reg$n_raw_clusters
    )
  } else {
    paste0(
      "Raw WLS regression unavailable\n",
      "N = ", raw_reg$n_raw_clusters
    )
  }

  log_msg(
    "Saving binned productivity-growth plot:", path,
    "| clusters:", nrow(z),
    "| bins with support:", nrow(bin_dt),
    "| productivity variable:", xvar
  )

  p <- ggplot() +
    geom_hline(yintercept = 0, linewidth = 0.3, linetype = "dashed") +

    # Main binned relationship.
    geom_errorbar(
      data = bin_dt[is.finite(growth_ci_low) & is.finite(growth_ci_high)],
      aes(
        x = productivity_bin_mid_log,
        ymin = growth_ci_low,
        ymax = growth_ci_high
      ),
      width = 0,
      linewidth = 0.45,
      alpha = 0.75,
      na.rm = TRUE
    ) +
    geom_line(
      data = bin_dt,
      aes(x = productivity_bin_mid_log, y = growth_mean_weighted, group = 1),
      linewidth = 0.9,
      na.rm = TRUE
    ) +
    geom_point(
      data = bin_dt,
      aes(
        x = productivity_bin_mid_log,
        y = growth_mean_weighted,
        size = n_clusters_bin
      ),
      alpha = 0.95,
      na.rm = TRUE
    ) +
    annotate(
      "label",
      x = Inf,
      y = Inf,
      label = reg_label,
      hjust = 1.05,
      vjust = 1.10,
      size = 3.2,
      label.size = 0.25
    ) +
    scale_x_continuous(
      labels = pretty_productivity_labels
    ) +
    scale_y_continuous(
      labels = scales::number_format(accuracy = 0.01)
    ) +

    labs(
      title = paste0("Lifecycle growth by productivity bin: ", cluster_label, " — ", window_tag),
      subtitle = paste0(
        "Binning method: ", bin_method_arg,
        ". Bin means are weighted by lifecycle-growth support."
      ),
      x = paste0(xlab, " — equal-width log1p bins"),
      y = "Lifecycle growth over window, log points",
      size = "Clusters in bin",
      caption = paste0(
        "Window: ", window_tag,
        " | N clusters: ", nrow(z),
        " | Bin method: ", bin_method_arg,
        " | Non-empty bins: ", nrow(bin_dt),
        " | Pearson corr(raw): ", round(pearson_raw, 3),
        " | Spearman corr(raw): ", round(spearman_raw, 3),
        " | Bin table: ", basename(bin_table_path)
      )
    ) +
    standard_theme()

  ggsave(path, p, width = 9, height = 6, dpi = 300)
  invisible(p)
}

plot_specs <- data.table(
  xvar = c("patents_per_inventor_year", "total_patents"),
  xlab = c("Productivity: patents per inventor-year", "Productivity: total patents"),
  suffix = c("patents_per_inventor_year", "total_patents")
)

for (cl in c("metro", "metro_parent")) {
  cl_dir <- file.path(OUT$plots, cl)
  dir.create(cl_dir, recursive = TRUE, showWarnings = FALSE)

  for (ww in c("early_0_25", "late_26_50")) {
    for (ii in seq_len(nrow(plot_specs))) {
      for (bm in c("equal_width_log", "equal_count_clusters")) {
        plot_productivity_binned_growth(
          tab = combined_gp,
          cluster_label = cl,
          window_tag = ww,
          xvar = plot_specs$xvar[ii],
          xlab = plot_specs$xlab[ii],
          bin_method = bm,
          path = file.path(
            cl_dir,
            paste0(cl, "__", ww, "__", plot_specs$suffix[ii], "__", bm, ".png")
          )
        )
      }
    }
  }
}

# =========================================================
# Diagnostics / manifest
# =========================================================
summary_tab <- combined_gp[, .(
  n_clusters = .N,
  mean_growth_log_points = mean(growth_log_points, na.rm = TRUE),
  p25_growth_log_points = as.numeric(quantile(growth_log_points, 0.25, na.rm = TRUE)),
  p50_growth_log_points = as.numeric(quantile(growth_log_points, 0.50, na.rm = TRUE)),
  p75_growth_log_points = as.numeric(quantile(growth_log_points, 0.75, na.rm = TRUE)),
  mean_patents_per_inventor_year = mean(patents_per_inventor_year, na.rm = TRUE),
  mean_total_patents = mean(total_patents, na.rm = TRUE)
), by = .(cluster_label, growth_window)]

write_dt(summary_tab, file.path(OUT$diagnostics, "growth_productivity_summary.csv"))

manifest <- data.table(
  script = "lifecycle_growth_scatter.R",
  input_dir = CONFIG$input_dir,
  output_dir = CONFIG$out_dir,
  year_min = CONFIG$year_min,
  year_max = CONFIG$year_max,
  tenure_definition = "tenure_main = edu_start_year + 3; fallback position start; fallback first observed year",
  tenure_bins = paste(ordered_tenure_bin_levels(), collapse = ", "),
  growth_definition = "weighted OLS slope of log(1 + mean patents per inventor-year in cluster x tenure-bin) on tenure-bin midpoint, multiplied by window span",
  growth_windows = "early_0_25; late_26_50",
  metro_parent_definition = "paste0(first_parent_rcid, '__', first_metro_area)",
  min_cluster_obs = CONFIG$min_cluster_obs,
  min_cluster_users = CONFIG$min_cluster_users,
  min_bin_obs = CONFIG$min_bin_obs,
  min_bins_in_growth_window = CONFIG$min_bins_in_growth_window,
  total_patent_axis_log10 = CONFIG$log10_total_patent_axis,
  n_productivity_bins = CONFIG$n_productivity_bins,
  productivity_bin_definition = "20 equal-width bins in log1p(productivity); plotted x-axis labels converted back to productivity units; bin means weighted by n_obs_growth"
)
write_dt(manifest, file.path(OUT$diagnostics, "manifest.csv"))

log_msg("Done. Outputs written to:", OUT$root)
