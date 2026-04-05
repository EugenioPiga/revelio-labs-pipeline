#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(scales)
})

options(arrow.skip_nul = TRUE)
options(bitmapType = "cairo")
set.seed(123)

# =========================================================
# Config
# =========================================================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR   <- "/home/epiga/revelio_labs/output/mobility_churn_extended_v2"
YEAR_MIN  <- 2010L
YEAR_MAX  <- 2019L
US_ONLY   <- TRUE
US_COUNTRY <- "United States"
MIN_POINTS_PLOT <- 250L
TOP_THRESHOLDS <- c(0.75, 0.90, 0.95)
EVENT_WINDOW <- 5L
COMPUTE_LOO <- TRUE
RUN_EVENT_STUDIES <- FALSE
RUN_HETEROGENEITY <- FALSE
RUN_NETWORKS <- FALSE
TENURE_SPLIT_CUTOFF <- 15L
DENSITY_ADJUST <- 1.10

PARENT_VAR  <- "first_parent_rcid"
FIRM_VAR    <- "first_rcid"
CITY_VAR    <- "first_city"
METRO_VAR   <- "first_metro_area"
STATE_VAR   <- "first_state"
REGION_VAR  <- "first_region"
COUNTRY_VAR <- "first_country"

IMMIG_CANDIDATES <- c("immig_first_deg_or_job_nonUS", "immig1_post", "immig1_ever", "immig2_post", "immig2_ever")
SENIORITY_CANDIDATES <- c("avg_seniority", "min_seniority", "max_seniority")
COMP_CANDIDATES <- c("avg_salary", "avg_total_comp", "min_salary", "max_salary", "min_total_comp", "max_total_comp")
NETWORK_CANDIDATES <- c("first_university_country", "edu_first_university_country", "first_university", "edu_first_university_name")

# =========================================================
# Helpers
# =========================================================
ensure_dir <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

audit_dir   <- file.path(OUT_DIR, "audit")
panel_dir   <- file.path(OUT_DIR, "mobility_panel")
od_dir      <- file.path(OUT_DIR, "origin_destination")
event_dir   <- file.path(OUT_DIR, "event_studies")
het_dir     <- file.path(OUT_DIR, "heterogeneity")
fig_dir     <- file.path(OUT_DIR, "figures")
summary_dir <- file.path(OUT_DIR, "summary")

for (d in c(OUT_DIR, audit_dir, panel_dir, od_dir, event_dir, het_dir, fig_dir, summary_dir)) ensure_dir(d)

ts_msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

nz_trim <- function(x) {
  x <- str_trim(as.character(x))
  x[x %in% c("", "NA", "NULL", "N/A")] <- NA_character_
  x
}

num_or_na <- function(x) suppressWarnings(as.numeric(x))
safe_share <- function(x) if (all(is.na(x))) NA_real_ else mean(x, na.rm = TRUE)

hhi_div <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (!length(x)) return(NA_real_)
  p <- as.numeric(table(x) / length(x))
  1 - sum(p^2)
}

safe_quantile <- function(x, probs) {
  x <- x[is.finite(x)]
  if (!length(x)) return(rep(NA_real_, length(probs)))
  as.numeric(quantile(x, probs = probs, na.rm = TRUE, names = FALSE))
}

combined_plot_registry <- list()

save_plot <- function(g, path, width = 8.5, height = 5.5, add_to_combined_pdf = TRUE) {
  if (is.null(g)) return(invisible(NULL))
  ggsave(path, g, width = width, height = height, dpi = 300)
  if (isTRUE(add_to_combined_pdf)) {
    combined_plot_registry[[length(combined_plot_registry) + 1L]] <<- list(
      plot = g,
      path = path,
      width = width,
      height = height
    )
  }
  invisible(g)
}

write_combined_pdf <- function(path, width = 11, height = 8.5) {
  if (!length(combined_plot_registry)) return(invisible(NULL))
  grDevices::cairo_pdf(path, width = width, height = height, onefile = TRUE)
  on.exit(grDevices::dev.off(), add = TRUE)
  for (obj in combined_plot_registry) print(obj$plot)
  invisible(path)
}

safe_uniqueN <- function(x) uniqueN(x[!is.na(x) & x != ""])

cumulative_distinct <- function(x) {
  seen <- new.env(hash = TRUE, parent = emptyenv())
  out <- integer(length(x))
  n <- 0L
  for (i in seq_along(x)) {
    val <- x[[i]]
    if (is.na(val) || identical(val, "")) {
      out[[i]] <- n
    } else {
      key <- as.character(val)
      if (!exists(key, envir = seen, inherits = FALSE)) {
        assign(key, TRUE, envir = seen)
        n <- n + 1L
      }
      out[[i]] <- n
    }
  }
  out
}

loo_from_group <- function(sum_x, n, own_x) {
  out <- rep(NA_real_, length(sum_x))
  ok <- !is.na(sum_x) & !is.na(n) & n > 1L & !is.na(own_x)
  out[ok] <- (sum_x[ok] - own_x[ok]) / (n[ok] - 1)
  out
}

first_non_missing <- function(dt, cols) {
  hit <- cols[cols %in% names(dt)]
  if (!length(hit)) return(NULL)
  hit[1L]
}

write_md <- function(path, lines) writeLines(lines, path)

calc_delta <- function(DT, dest, origin, out) {
  if (all(c(dest, origin) %in% names(DT))) DT[, (out) := get(dest) - get(origin)]
}

extract_year <- function(x) suppressWarnings(as.integer(substr(as.character(x), 1, 4)))

bin_and_plot <- function(df, xvar, yvar, ttl, xlab = NULL, nbins = 20L) {
  z <- as.data.table(df)[is.finite(get(xvar)) & !is.na(get(yvar))]
  if (nrow(z) < MIN_POINTS_PLOT) return(NULL)

  cuts <- unique(safe_quantile(z[[xvar]], seq(0, 1, length.out = nbins + 1)))
  cuts <- cuts[is.finite(cuts)]
  if (length(cuts) < 5L) return(NULL)

  z[, bin := cut(get(xvar), breaks = cuts, include.lowest = TRUE, labels = FALSE)]
  b <- z[!is.na(bin), .(
    x_mean = mean(get(xvar), na.rm = TRUE),
    y_mean = mean(get(yvar), na.rm = TRUE),
    n = .N
  ), by = bin][order(x_mean)]

  ggplot(b, aes(x = x_mean, y = y_mean)) +
    geom_point(aes(size = n), alpha = 0.85) +
    geom_line() +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(title = ttl, x = ifelse(is.null(xlab), xvar, xlab), y = paste0("Pr(", yvar, ")")) +
    theme_minimal(base_size = 12)
}

delta_test_stats <- function(x) {
  z <- x[is.finite(x)]
  if (length(z) < 2L) {
    return(list(beta = NA_real_, se = NA_real_, p_value = NA_real_, n = length(z)))
  }
  fit <- summary(lm(z ~ 1))
  cf <- coef(fit)
  list(
    beta = unname(cf[1, 1]),
    se = unname(cf[1, 2]),
    p_value = unname(cf[1, 4]),
    n = length(z)
  )
}

delta_diff_stats <- function(x, group) {
  z <- data.table(x = x, group = group)
  z <- z[is.finite(x) & !is.na(group)]
  if (nrow(z) < 4L || uniqueN(z$group) < 2L) {
    return(list(beta = NA_real_, se = NA_real_, p_value = NA_real_, n = nrow(z)))
  }
  z[, late_group := as.integer(group == "Late tenure (>15)")]
  if (z[, min(.N), by = late_group][, min(V1)] < 2L) {
    return(list(beta = NA_real_, se = NA_real_, p_value = NA_real_, n = nrow(z)))
  }
  fit <- summary(lm(x ~ late_group, data = z))
  cf <- coef(fit)
  if (!"late_group" %in% rownames(cf)) {
    return(list(beta = NA_real_, se = NA_real_, p_value = NA_real_, n = nrow(z)))
  }
  list(
    beta = unname(cf["late_group", 1]),
    se = unname(cf["late_group", 2]),
    p_value = unname(cf["late_group", 4]),
    n = nrow(z)
  )
}

format_p <- function(p) {
  if (is.na(p)) return("NA")
  if (p < 0.001) return("<0.001")
  sprintf("%.3f", p)
}

format_stat_line <- function(lbl, stats) {
  paste0(
    lbl, ": β = ", sprintf("%.4f", stats$beta),
    ", SE = ", sprintf("%.4f", stats$se),
    ", p = ", format_p(stats$p_value),
    ", N = ", scales::comma(stats$n)
  )
}

add_stats_label <- function(g, stats_lines) {
  lbl <- paste(stats_lines, collapse = "\n")
  g + annotate(
    "label",
    x = Inf, y = Inf,
    label = lbl,
    hjust = 1.05, vjust = 1.1,
    size = 3.3,
    label.size = 0.25
  )
}

has_density_support <- function(x) {
  length(unique(x[is.finite(x)])) >= 2L
}

make_density_plot <- function(sub, delta_var, title_txt, xlab_txt, stats_lines, split = FALSE) {
  if (!has_density_support(sub[[delta_var]])) return(NULL)

  g <- ggplot(sub, aes(x = .data[[delta_var]])) +
    geom_vline(xintercept = 0, linetype = 2, linewidth = 0.4) +
    theme_minimal(base_size = 12)

  if (isTRUE(split)) {
    g <- g +
      geom_density(aes(color = tenure_group, fill = tenure_group), adjust = DENSITY_ADJUST, alpha = 0.18, linewidth = 0.9) +
      labs(title = title_txt, x = xlab_txt, y = "Density", color = NULL, fill = NULL)
  } else {
    g <- g +
      geom_density(fill = "grey70", color = "grey25", adjust = DENSITY_ADJUST, alpha = 0.45, linewidth = 0.9) +
      labs(title = title_txt, x = xlab_txt, y = "Density")
  }

  add_stats_label(g, stats_lines)
}

make_delta_density_bundle <- function(DT, delta_var, move_var, cluster_label, measure_label, move_label, file_stub) {
  needed <- c(delta_var, move_var)
  if (!all(needed %in% names(DT))) {
    return(data.table(
      file_stub = file_stub,
      cluster = cluster_label,
      measure = measure_label,
      move_var = move_var,
      move_label = move_label,
      delta_var = delta_var,
      plot_variant = "overall",
      stat_type = "all",
      tenure_group = "all",
      produced = 0L,
      reason = "missing_variable",
      beta = NA_real_, se = NA_real_, p_value = NA_real_, n = NA_integer_
    ))
  }

  sub <- copy(DT[get(move_var) == 1L & is.finite(get(delta_var))])
  if (nrow(sub) < 2L) {
    return(data.table(
      file_stub = file_stub,
      cluster = cluster_label,
      measure = measure_label,
      move_var = move_var,
      move_label = move_label,
      delta_var = delta_var,
      plot_variant = "overall",
      stat_type = "all",
      tenure_group = "all",
      produced = 0L,
      reason = "too_few_observations",
      beta = NA_real_, se = NA_real_, p_value = NA_real_, n = nrow(sub)
    ))
  }

  overall_stats <- delta_test_stats(sub[[delta_var]])
  overall_plot <- make_density_plot(
    sub = sub,
    delta_var = delta_var,
    title_txt = paste0("Destination - origin ", cluster_label, " ", measure_label, " (", move_label, ")"),
    xlab_txt = paste0("Destination - origin ", cluster_label, " ", measure_label),
    stats_lines = format_stat_line("All movers", overall_stats),
    split = FALSE
  )
  save_plot(overall_plot, file.path(fig_dir, paste0(file_stub, "_density.png")))

  out <- data.table(
    file_stub = file_stub,
    cluster = cluster_label,
    measure = measure_label,
    move_var = move_var,
    move_label = move_label,
    delta_var = delta_var,
    plot_variant = "overall",
    stat_type = "all",
    tenure_group = "all",
    produced = as.integer(!is.null(overall_plot)),
    reason = fifelse(is.null(overall_plot), "insufficient_support", NA_character_),
    beta = overall_stats$beta,
    se = overall_stats$se,
    p_value = overall_stats$p_value,
    n = overall_stats$n
  )

  tenure_source <- if ("tenure_main_lag" %in% names(sub)) "tenure_main_lag" else if ("tenure_main" %in% names(sub)) "tenure_main" else NULL
  if (is.null(tenure_source)) return(out)

  sub[, tenure_group := fifelse(
    get(tenure_source) <= TENURE_SPLIT_CUTOFF, "Early tenure (<=15)",
    fifelse(get(tenure_source) > TENURE_SPLIT_CUTOFF, "Late tenure (>15)", NA_character_)
  )]
  split_sub <- sub[!is.na(tenure_group)]
  if (!nrow(split_sub)) return(out)

  early_stats <- delta_test_stats(split_sub[tenure_group == "Early tenure (<=15)", get(delta_var)])
  late_stats  <- delta_test_stats(split_sub[tenure_group == "Late tenure (>15)", get(delta_var)])
  diff_stats  <- delta_diff_stats(split_sub[[delta_var]], split_sub$tenure_group)

  split_plot <- make_density_plot(
    sub = split_sub,
    delta_var = delta_var,
    title_txt = paste0("Destination - origin ", cluster_label, " ", measure_label, " by mover tenure (", move_label, ")"),
    xlab_txt = paste0("Destination - origin ", cluster_label, " ", measure_label),
    stats_lines = c(
      format_stat_line("Early", early_stats),
      format_stat_line("Late", late_stats),
      format_stat_line("Late - Early", diff_stats)
    ),
    split = TRUE
  )
  save_plot(split_plot, file.path(fig_dir, paste0(file_stub, "_density_tenure_split.png")))

  split_rows <- rbindlist(list(
    data.table(
      file_stub = file_stub,
      cluster = cluster_label,
      measure = measure_label,
      move_var = move_var,
      move_label = move_label,
      delta_var = delta_var,
      plot_variant = "tenure_split",
      stat_type = "early",
      tenure_group = "Early tenure (<=15)",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = early_stats$beta,
      se = early_stats$se,
      p_value = early_stats$p_value,
      n = early_stats$n
    ),
    data.table(
      file_stub = file_stub,
      cluster = cluster_label,
      measure = measure_label,
      move_var = move_var,
      move_label = move_label,
      delta_var = delta_var,
      plot_variant = "tenure_split",
      stat_type = "late",
      tenure_group = "Late tenure (>15)",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = late_stats$beta,
      se = late_stats$se,
      p_value = late_stats$p_value,
      n = late_stats$n
    ),
    data.table(
      file_stub = file_stub,
      cluster = cluster_label,
      measure = measure_label,
      move_var = move_var,
      move_label = move_label,
      delta_var = delta_var,
      plot_variant = "tenure_split",
      stat_type = "late_minus_early",
      tenure_group = "difference",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = diff_stats$beta,
      se = diff_stats$se,
      p_value = diff_stats$p_value,
      n = diff_stats$n
    )
  ), fill = TRUE)

  rbindlist(list(out, split_rows), fill = TRUE)
}

# =========================================================
# Inspect schema / collect columns
# =========================================================
ts_msg("Opening dataset:", INPUT_DIR)
ds <- open_dataset(INPUT_DIR, format = "parquet")
ds_names <- names(ds)

schema_fields <- ds$schema$fields
safe_field_type <- function(f) {
  tryCatch({
    if (!is.null(f$type$ToString)) f$type$ToString() else paste(capture.output(print(f$type)), collapse = " ")
  }, error = function(e) paste(capture.output(print(f$type)), collapse = " "))
}

schema_df <- data.frame(
  variable = vapply(schema_fields, function(f) f$name, character(1)),
  type = vapply(schema_fields, safe_field_type, character(1)),
  stringsAsFactors = FALSE
)
write_csv(schema_df, file.path(audit_dir, "schema_types.csv"))

required_core <- c("user_id", "year", "n_patents", PARENT_VAR, FIRM_VAR, CITY_VAR, METRO_VAR, STATE_VAR, REGION_VAR, COUNTRY_VAR)
missing_core <- setdiff(required_core, ds_names)
if (length(missing_core) > 0) stop(paste("Missing required columns:", paste(missing_core, collapse = ", ")))

selected_cols <- unique(c(
  "user_id", "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations", "user_linkedin_patent_count",
  "career_modal_section", "career_modal_class", "modal_section_y", "modal_class_y",
  PARENT_VAR, "last_parent_rcid", FIRM_VAR, "last_rcid", "n_positions", "n_unique_companies",
  "au_prestige", "au_numconnections", COMP_CANDIDATES, SENIORITY_CANDIDATES,
  CITY_VAR, "last_city", METRO_VAR, "last_metro_area", STATE_VAR, "last_state",
  COUNTRY_VAR, "last_country", REGION_VAR, "last_region",
  IMMIG_CANDIDATES, NETWORK_CANDIDATES,
  "first_field", "last_field", "first_degree", "last_degree", "first_university_country", "last_university_country",
  "first_startdate_pos", "first_startdate_edu",
  "au_sex_predicted", "au_ethnicity_predicted"
))
selected_cols <- selected_cols[selected_cols %in% ds_names]
ts_msg("Collecting analysis columns:", paste(selected_cols, collapse = ", "))

df <- ds %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(all_of(selected_cols)) %>%
  collect() %>%
  as.data.table()

# =========================================================
# Clean / construct base variables
# =========================================================
char_cols <- names(df)[vapply(df, function(x) is.character(x) || is.factor(x), logical(1))]
for (cc in char_cols) set(df, j = cc, value = nz_trim(df[[cc]]))

num_cols <- intersect(c(
  "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations", "user_linkedin_patent_count",
  "n_positions", "n_unique_companies", "au_prestige", "au_numconnections",
  COMP_CANDIDATES, SENIORITY_CANDIDATES
), names(df))
for (cc in num_cols) set(df, j = cc, value = num_or_na(df[[cc]]))

df[, user_id := as.character(user_id)]
df[, year := as.integer(year)]
if (US_ONLY) df <- df[!is.na(get(COUNTRY_VAR)) & get(COUNTRY_VAR) == US_COUNTRY]

for (v in c("n_patents", "n_applications", "n_first_inventor", "n_us_citations")) {
  if (v %in% names(df)) df[is.na(get(v)), (v) := 0]
}

imm_var <- first_non_missing(df, IMMIG_CANDIDATES)
if (!is.null(imm_var)) df[, imm := as.integer(get(imm_var))] else df[, imm := NA_integer_]

if ("first_university_country" %in% names(df)) {
  df[, origin_country_proxy := fifelse(
    !is.na(first_university_country) & first_university_country != "",
    first_university_country,
    fifelse("edu_first_university_country" %in% names(df) & !is.na(edu_first_university_country), edu_first_university_country, NA_character_)
  )]
} else if ("edu_first_university_country" %in% names(df)) {
  df[, origin_country_proxy := edu_first_university_country]
} else {
  df[, origin_country_proxy := NA_character_]
}

if ("first_university" %in% names(df)) {
  df[, university_proxy := first_university]
} else if ("edu_first_university_name" %in% names(df)) {
  df[, university_proxy := edu_first_university_name]
} else {
  df[, university_proxy := NA_character_]
}

if (all(c("first_startdate_edu", "first_startdate_pos") %in% names(df))) {
  df[, edu_year := extract_year(first_startdate_edu)]
  df[, pos_year := extract_year(first_startdate_pos)]
} else {
  df[, edu_year := NA_real_]
  df[, pos_year := NA_real_]
}

df[, first_obs_year := min(year, na.rm = TRUE), by = user_id]
df[, first_patent_year := { yy <- year[n_patents > 0]; if (length(yy)) min(yy) else NA_real_ }, by = user_id]
df[, tenure_main := year - edu_year + 3]
df[is.na(tenure_main) | tenure_main < 0 | tenure_main > 60, tenure_main := year - pos_year]
df[is.na(tenure_main) | tenure_main < 0 | tenure_main > 60, tenure_main := year - first_obs_year]
df[tenure_main < 0 | tenure_main > 60, tenure_main := NA_real_]
df[, patenting := as.integer(n_patents > 0)]
setorder(df, user_id, year)

# =========================================================
# Schema audit
# =========================================================
audit_stats <- rbindlist(lapply(names(df), function(v) {
  x <- df[[v]]
  is_num <- is.numeric(x) || is.integer(x)
  data.table(
    variable = v,
    missing_rate = mean(is.na(x)),
    n_distinct = if (length(x) <= 5e6) safe_uniqueN(as.character(x)) else NA_integer_,
    min_value = if (is_num) suppressWarnings(min(x, na.rm = TRUE)) else NA_real_,
    max_value = if (is_num) suppressWarnings(max(x, na.rm = TRUE)) else NA_real_
  )
}), fill = TRUE)
schema_audit <- merge(as.data.table(schema_df), audit_stats, by = "variable", all.x = TRUE)
write_csv(as_tibble(schema_audit), file.path(audit_dir, "analysis_schema_audit.csv"))

# =========================================================
# Mobility panel
# =========================================================
dt <- copy(df)
rm(df)
gc()
setorder(dt, user_id, year)
dt[, lag_year := shift(year), by = user_id]
dt[, is_consecutive := as.integer(!is.na(lag_year) & year - lag_year == 1L)]

lag_map <- list(
  parent_id = PARENT_VAR,
  firm_id   = FIRM_VAR,
  city_id   = CITY_VAR,
  metro_id  = METRO_VAR,
  state_id  = STATE_VAR,
  region_id = REGION_VAR
)

for (nm in names(lag_map)) {
  vv <- lag_map[[nm]]
  dt[, (nm) := get(vv)]
  dt[, paste0(nm, "_lag") := shift(get(nm)), by = user_id]
}

personal_lag_vars <- intersect(c(
  "n_patents", "n_applications", "n_first_inventor", "n_us_citations", "user_linkedin_patent_count",
  "avg_salary", "avg_total_comp", "avg_seniority", "min_seniority", "max_seniority",
  "tenure_main", "patenting", "imm"
), names(dt))
for (vv in personal_lag_vars) dt[, paste0(vv, "_lag") := shift(get(vv)), by = user_id]

mk_change <- function(cur, lag) as.integer(!is.na(cur) & !is.na(lag) & cur != lag)

dt[, leave_parent_lastobs := mk_change(parent_id, parent_id_lag)]
dt[, leave_firm_lastobs   := mk_change(firm_id, firm_id_lag)]
dt[, leave_city_lastobs   := mk_change(city_id, city_id_lag)]
dt[, leave_metro_lastobs  := mk_change(metro_id, metro_id_lag)]
dt[, leave_state_lastobs  := mk_change(state_id, state_id_lag)]

for (v in c("leave_parent", "leave_firm", "leave_city", "leave_metro", "leave_state")) {
  dt[, paste0(v, "_consec") := as.integer(is_consecutive == 1L & get(paste0(v, "_lastobs")) == 1L)]
}

dt[, within_parent_firm_move_consec := as.integer(leave_firm_consec == 1L & leave_parent_consec == 0L)]
dt[, across_parent_move_consec      := as.integer(leave_parent_consec == 1L)]
dt[, parent_only_move_consec        := as.integer(leave_parent_consec == 1L & leave_city_consec == 0L)]
dt[, city_only_move_consec          := as.integer(leave_parent_consec == 0L & leave_city_consec == 1L)]
dt[, metro_only_move_consec         := as.integer(leave_parent_consec == 0L & leave_metro_consec == 1L)]
dt[, joint_parent_city_move_consec  := as.integer(leave_parent_consec == 1L & leave_city_consec == 1L)]
dt[, joint_parent_metro_move_consec := as.integer(leave_parent_consec == 1L & leave_metro_consec == 1L)]

dt[, cum_parent_moves := cumsum(fifelse(is.na(leave_parent_lastobs), 0L, leave_parent_lastobs)), by = user_id]
dt[, cum_city_moves   := cumsum(fifelse(is.na(leave_city_lastobs), 0L, leave_city_lastobs)), by = user_id]

for (nm in c("parent_id", "firm_id", "city_id", "metro_id")) {
  dt[, paste0("cum_unique_", sub("_id$", "", nm)) := cumulative_distinct(get(nm)), by = user_id]
}

inv_tot <- dt[, .(
  total_patents = sum(n_patents, na.rm = TRUE),
  total_citations = sum(n_us_citations, na.rm = TRUE),
  obs_n = .N,
  n_parent_moves = sum(leave_parent_lastobs, na.rm = TRUE),
  n_city_moves   = sum(leave_city_lastobs, na.rm = TRUE)
), by = user_id]

for (q in TOP_THRESHOLDS) {
  thr <- quantile(inv_tot$total_patents, probs = q, na.rm = TRUE)
  nm <- paste0("top_", gsub("\\.", "", format(q * 100, trim = TRUE)), "_patents")
  inv_tot[, (nm) := as.integer(total_patents >= thr)]
}

lead_tot <- dt[, .(
  leadership_share = fifelse(
    sum(n_patents, na.rm = TRUE) > 0,
    sum(n_first_inventor, na.rm = TRUE) / pmax(sum(n_patents, na.rm = TRUE), 1),
    NA_real_
  )
), by = user_id]

inv_tot[, repeat_churner := as.integer(n_parent_moves >= 3)]
inv_tot <- merge(inv_tot, lead_tot, by = "user_id", all.x = TRUE)
dt <- merge(dt, inv_tot, by = "user_id", all.x = TRUE)
rm(inv_tot, lead_tot)
gc()

# =========================================================
# Group environments
# =========================================================
group_env <- function(DT, idvar, prefix) {
  if (!idvar %in% names(DT)) return(NULL)
  keep <- !is.na(DT[[idvar]]) & DT[[idvar]] != ""
  if (!any(keep)) return(NULL)

  out <- DT[keep, .(
    n_inventors = .N,
    patents_sum = sum(n_patents, na.rm = TRUE),
    patents_mean = mean(n_patents, na.rm = TRUE),
    citations_sum = sum(n_us_citations, na.rm = TRUE),
    citations_mean = mean(n_us_citations, na.rm = TRUE),
    firstinv_mean = mean(n_first_inventor, na.rm = TRUE),
    immig_share = safe_share(imm),
    avg_salary_env = mean(avg_salary, na.rm = TRUE),
    avg_total_comp_env = mean(avg_total_comp, na.rm = TRUE),
    avg_seniority_env = mean(avg_seniority, na.rm = TRUE),
    origin_div = hhi_div(origin_country_proxy),
    uni_div = hhi_div(university_proxy)
  ), by = c(idvar, "year")]

  setnames(
    out,
    old = c(idvar, setdiff(names(out), c(idvar, "year"))),
    new = c(idvar, paste0(prefix, "_", setdiff(names(out), c(idvar, "year"))))
  )
  out
}

parent_env <- group_env(dt, "parent_id", "parent")
firm_env   <- group_env(dt, "firm_id",   "firm")
city_env   <- group_env(dt, "city_id",   "city")
metro_env  <- group_env(dt, "metro_id",  "metro")
state_env  <- group_env(dt, "state_id",  "state")

if (RUN_NETWORKS) {
  if (!all(is.na(dt$origin_country_proxy))) {
    parent_origin <- dt[!is.na(parent_id) & !is.na(origin_country_proxy), .N, by = .(parent_id, year, origin_country_proxy)]
    parent_origin_tot <- dt[!is.na(parent_id), .(parent_n = .N), by = .(parent_id, year)]
    parent_origin <- merge(parent_origin, parent_origin_tot, by = c("parent_id", "year"), all.x = TRUE)
    parent_origin[, parent_same_origin_share := fifelse(parent_n > 1, (N - 1) / (parent_n - 1), NA_real_)]
    dt <- merge(
      dt,
      parent_origin[, .(parent_id, year, origin_country_proxy, parent_same_origin_share)],
      by = c("parent_id", "year", "origin_country_proxy"),
      all.x = TRUE
    )
  }

  if (!all(is.na(dt$university_proxy))) {
    parent_uni <- dt[!is.na(parent_id) & !is.na(university_proxy), .N, by = .(parent_id, year, university_proxy)]
    parent_uni_tot <- dt[!is.na(parent_id), .(parent_n = .N), by = .(parent_id, year)]
    parent_uni <- merge(parent_uni, parent_uni_tot, by = c("parent_id", "year"), all.x = TRUE)
    parent_uni[, parent_same_university_share := fifelse(parent_n > 1, (N - 1) / (parent_n - 1), NA_real_)]
    dt <- merge(
      dt,
      parent_uni[, .(parent_id, year, university_proxy, parent_same_university_share)],
      by = c("parent_id", "year", "university_proxy"),
      all.x = TRUE
    )
  }
}

if (COMPUTE_LOO) {
  if (!is.null(parent_env)) {
    dt <- merge(
      dt,
      parent_env[, .(parent_id, year, parent_n_inventors, parent_patents_sum, parent_citations_sum)],
      by = c("parent_id", "year"), all.x = TRUE
    )
    dt[, parent_loo_patents := loo_from_group(parent_patents_sum, parent_n_inventors, n_patents)]
    dt[, parent_loo_citations := loo_from_group(parent_citations_sum, parent_n_inventors, n_us_citations)]
  }

  if (!is.null(firm_env)) {
    dt <- merge(
      dt,
      firm_env[, .(firm_id, year, firm_n_inventors, firm_patents_sum, firm_citations_sum)],
      by = c("firm_id", "year"), all.x = TRUE
    )
    dt[, firm_loo_patents := loo_from_group(firm_patents_sum, firm_n_inventors, n_patents)]
    dt[, firm_loo_citations := loo_from_group(firm_citations_sum, firm_n_inventors, n_us_citations)]
  }

  if (!is.null(city_env)) {
    dt <- merge(
      dt,
      city_env[, .(city_id, year, city_n_inventors, city_patents_sum, city_citations_sum)],
      by = c("city_id", "year"), all.x = TRUE
    )
    dt[, city_loo_patents := loo_from_group(city_patents_sum, city_n_inventors, n_patents)]
    dt[, city_loo_citations := loo_from_group(city_citations_sum, city_n_inventors, n_us_citations)]
  }

  if (!is.null(metro_env)) {
    dt <- merge(
      dt,
      metro_env[, .(metro_id, year, metro_n_inventors, metro_patents_sum, metro_citations_sum)],
      by = c("metro_id", "year"), all.x = TRUE
    )
    dt[, metro_loo_patents := loo_from_group(metro_patents_sum, metro_n_inventors, n_patents)]
    dt[, metro_loo_citations := loo_from_group(metro_citations_sum, metro_n_inventors, n_us_citations)]
  }
}

# =========================================================
# Origin/destination comparisons
# =========================================================
ana <- copy(dt[!is.na(parent_id_lag) | !is.na(firm_id_lag) | !is.na(city_id_lag) | !is.na(metro_id_lag)])
ana[, year_lag := year - 1L]

merge_origin_env <- function(base, env, lag_id) {
  if (is.null(env)) return(base)
  val_cols <- setdiff(names(env), c(lag_id, "year"))
  env2 <- copy(env)
  setnames(env2, lag_id, paste0(lag_id, "_lag_join"))
  setnames(env2, val_cols, paste0("origin_", val_cols))
  merge(
    base,
    env2,
    by.x = c(paste0(lag_id, "_lag"), "year_lag"),
    by.y = c(paste0(lag_id, "_lag_join"), "year"),
    all.x = TRUE
  )
}

merge_dest_env <- function(base, env, idvar) {
  if (is.null(env)) return(base)
  val_cols <- setdiff(names(env), c(idvar, "year"))
  env2 <- copy(env)
  setnames(env2, val_cols, paste0("dest_", val_cols))
  merge(base, env2, by = c(idvar, "year"), all.x = TRUE)
}

for (spec in list(
  list(env = parent_env, id = "parent_id"),
  list(env = firm_env,   id = "firm_id"),
  list(env = city_env,   id = "city_id"),
  list(env = metro_env,  id = "metro_id"),
  list(env = state_env,  id = "state_id")
)) {
  ana <- merge_origin_env(ana, spec$env, spec$id)
  ana <- merge_dest_env(ana, spec$env, spec$id)
}

if ("parent_same_origin_share" %in% names(dt)) {
  ana[, parent_same_origin_share_lag := shift(parent_same_origin_share), by = user_id]
  calc_delta(ana, "parent_same_origin_share", "parent_same_origin_share_lag", "delta_parent_same_origin_share")
}
if ("parent_same_university_share" %in% names(dt)) {
  ana[, parent_same_university_share_lag := shift(parent_same_university_share), by = user_id]
  calc_delta(ana, "parent_same_university_share", "parent_same_university_share_lag", "delta_parent_same_university_share")
}

# Personal changes
calc_delta(ana, "avg_salary",      "avg_salary_lag",      "delta_salary")
calc_delta(ana, "avg_total_comp",  "avg_total_comp_lag",  "delta_total_comp")
calc_delta(ana, "avg_seniority",   "avg_seniority_lag",   "delta_seniority")

# Cluster deltas: explicit parent / firm / city / metro naming
delta_spec_rows <- list(
  list(cluster = "parent", measure = "size",                 dest = "dest_parent_n_inventors",         origin = "origin_parent_n_inventors",         out = "delta_parent_size"),
  list(cluster = "parent", measure = "patents",              dest = "dest_parent_patents_mean",        origin = "origin_parent_patents_mean",        out = "delta_parent_patents"),
  list(cluster = "parent", measure = "citations",           dest = "dest_parent_citations_mean",      origin = "origin_parent_citations_mean",      out = "delta_parent_citations"),
  list(cluster = "parent", measure = "first_inventor",      dest = "dest_parent_firstinv_mean",       origin = "origin_parent_firstinv_mean",       out = "delta_parent_first_inventor"),
  list(cluster = "parent", measure = "immigrant_share",     dest = "dest_parent_immig_share",         origin = "origin_parent_immig_share",         out = "delta_parent_immigrant_share"),
  list(cluster = "parent", measure = "origin_diversity",    dest = "dest_parent_origin_div",          origin = "origin_parent_origin_div",          out = "delta_parent_origin_diversity"),
  list(cluster = "parent", measure = "university_diversity",dest = "dest_parent_uni_div",             origin = "origin_parent_uni_div",             out = "delta_parent_university_diversity"),
  list(cluster = "parent", measure = "avg_salary",          dest = "dest_parent_avg_salary_env",      origin = "origin_parent_avg_salary_env",      out = "delta_parent_avg_salary"),
  list(cluster = "parent", measure = "avg_total_comp",      dest = "dest_parent_avg_total_comp_env",  origin = "origin_parent_avg_total_comp_env",  out = "delta_parent_avg_total_comp"),
  list(cluster = "parent", measure = "avg_seniority",       dest = "dest_parent_avg_seniority_env",   origin = "origin_parent_avg_seniority_env",   out = "delta_parent_avg_seniority"),

  list(cluster = "firm", measure = "size",                 dest = "dest_firm_n_inventors",           origin = "origin_firm_n_inventors",           out = "delta_firm_size"),
  list(cluster = "firm", measure = "patents",              dest = "dest_firm_patents_mean",          origin = "origin_firm_patents_mean",          out = "delta_firm_patents"),
  list(cluster = "firm", measure = "citations",            dest = "dest_firm_citations_mean",        origin = "origin_firm_citations_mean",        out = "delta_firm_citations"),
  list(cluster = "firm", measure = "first_inventor",       dest = "dest_firm_firstinv_mean",         origin = "origin_firm_firstinv_mean",         out = "delta_firm_first_inventor"),
  list(cluster = "firm", measure = "immigrant_share",      dest = "dest_firm_immig_share",           origin = "origin_firm_immig_share",           out = "delta_firm_immigrant_share"),
  list(cluster = "firm", measure = "origin_diversity",     dest = "dest_firm_origin_div",            origin = "origin_firm_origin_div",            out = "delta_firm_origin_diversity"),
  list(cluster = "firm", measure = "university_diversity", dest = "dest_firm_uni_div",               origin = "origin_firm_uni_div",               out = "delta_firm_university_diversity"),
  list(cluster = "firm", measure = "avg_salary",           dest = "dest_firm_avg_salary_env",        origin = "origin_firm_avg_salary_env",        out = "delta_firm_avg_salary"),
  list(cluster = "firm", measure = "avg_total_comp",       dest = "dest_firm_avg_total_comp_env",    origin = "origin_firm_avg_total_comp_env",    out = "delta_firm_avg_total_comp"),
  list(cluster = "firm", measure = "avg_seniority",        dest = "dest_firm_avg_seniority_env",     origin = "origin_firm_avg_seniority_env",     out = "delta_firm_avg_seniority"),

  list(cluster = "city", measure = "size",                 dest = "dest_city_n_inventors",           origin = "origin_city_n_inventors",           out = "delta_city_size"),
  list(cluster = "city", measure = "patents",              dest = "dest_city_patents_mean",          origin = "origin_city_patents_mean",          out = "delta_city_patents"),
  list(cluster = "city", measure = "citations",            dest = "dest_city_citations_mean",        origin = "origin_city_citations_mean",        out = "delta_city_citations"),
  list(cluster = "city", measure = "first_inventor",       dest = "dest_city_firstinv_mean",         origin = "origin_city_firstinv_mean",         out = "delta_city_first_inventor"),
  list(cluster = "city", measure = "immigrant_share",      dest = "dest_city_immig_share",           origin = "origin_city_immig_share",           out = "delta_city_immigrant_share"),
  list(cluster = "city", measure = "origin_diversity",     dest = "dest_city_origin_div",            origin = "origin_city_origin_div",            out = "delta_city_origin_diversity"),
  list(cluster = "city", measure = "university_diversity", dest = "dest_city_uni_div",               origin = "origin_city_uni_div",               out = "delta_city_university_diversity"),
  list(cluster = "city", measure = "avg_salary",           dest = "dest_city_avg_salary_env",        origin = "origin_city_avg_salary_env",        out = "delta_city_avg_salary"),
  list(cluster = "city", measure = "avg_total_comp",       dest = "dest_city_avg_total_comp_env",    origin = "origin_city_avg_total_comp_env",    out = "delta_city_avg_total_comp"),
  list(cluster = "city", measure = "avg_seniority",        dest = "dest_city_avg_seniority_env",     origin = "origin_city_avg_seniority_env",     out = "delta_city_avg_seniority"),

  list(cluster = "metro", measure = "size",                 dest = "dest_metro_n_inventors",          origin = "origin_metro_n_inventors",          out = "delta_metro_size"),
  list(cluster = "metro", measure = "patents",              dest = "dest_metro_patents_mean",         origin = "origin_metro_patents_mean",         out = "delta_metro_patents"),
  list(cluster = "metro", measure = "citations",            dest = "dest_metro_citations_mean",       origin = "origin_metro_citations_mean",       out = "delta_metro_citations"),
  list(cluster = "metro", measure = "first_inventor",       dest = "dest_metro_firstinv_mean",        origin = "origin_metro_firstinv_mean",        out = "delta_metro_first_inventor"),
  list(cluster = "metro", measure = "immigrant_share",      dest = "dest_metro_immig_share",          origin = "origin_metro_immig_share",          out = "delta_metro_immigrant_share"),
  list(cluster = "metro", measure = "origin_diversity",     dest = "dest_metro_origin_div",           origin = "origin_metro_origin_div",           out = "delta_metro_origin_diversity"),
  list(cluster = "metro", measure = "university_diversity", dest = "dest_metro_uni_div",              origin = "origin_metro_uni_div",              out = "delta_metro_university_diversity"),
  list(cluster = "metro", measure = "avg_salary",           dest = "dest_metro_avg_salary_env",       origin = "origin_metro_avg_salary_env",       out = "delta_metro_avg_salary"),
  list(cluster = "metro", measure = "avg_total_comp",       dest = "dest_metro_avg_total_comp_env",   origin = "origin_metro_avg_total_comp_env",   out = "delta_metro_avg_total_comp"),
  list(cluster = "metro", measure = "avg_seniority",        dest = "dest_metro_avg_seniority_env",    origin = "origin_metro_avg_seniority_env",    out = "delta_metro_avg_seniority")
)

delta_specs <- rbindlist(lapply(delta_spec_rows, as.data.table), fill = TRUE)
for (i in seq_len(nrow(delta_specs))) {
  calc_delta(ana, delta_specs$dest[i], delta_specs$origin[i], delta_specs$out[i])
}

# Keep legacy names too if you had downstream code relying on them
if ("delta_parent_first_inventor" %in% names(ana)) ana[, delta_parent_firstinv := delta_parent_first_inventor]
if ("delta_firm_first_inventor"   %in% names(ana)) ana[, delta_firm_firstinv   := delta_firm_first_inventor]
if ("delta_city_first_inventor"   %in% names(ana)) ana[, delta_city_firstinv   := delta_city_first_inventor]
if ("delta_metro_first_inventor"  %in% names(ana)) ana[, delta_metro_firstinv  := delta_metro_first_inventor]

# =========================================================
# Summary tables
# =========================================================
panel_keep <- unique(c(
  "user_id", "year", "tenure_main", "imm", "avg_seniority", "avg_salary", "avg_total_comp", "n_patents", "n_us_citations",
  "n_first_inventor", "patenting", "parent_id", "parent_id_lag", "firm_id", "firm_id_lag", "city_id", "city_id_lag",
  "metro_id", "metro_id_lag",
  grep("^leave_", names(dt), value = TRUE),
  grep("_move_", names(dt), value = TRUE),
  grep("^cum_", names(dt), value = TRUE),
  grep("^top_", names(dt), value = TRUE),
  "repeat_churner"
))
write_csv(as_tibble(dt[, ..panel_keep]), file.path(panel_dir, "mobility_panel_compact.csv"))

move_type_tbl <- rbindlist(list(
  dt[, .(move_type = "within_parent_firm_consec", share = mean(within_parent_firm_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "across_parent_consec", share = mean(across_parent_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "parent_only_consec", share = mean(parent_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "city_only_consec", share = mean(city_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "metro_only_consec", share = mean(metro_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_parent_city_consec", share = mean(joint_parent_city_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_parent_metro_consec", share = mean(joint_parent_metro_move_consec, na.rm = TRUE), n = .N)]
), fill = TRUE)
write_csv(as_tibble(move_type_tbl), file.path(panel_dir, "move_type_shares.csv"))

predictor_vars <- intersect(c(
  "origin_parent_n_inventors", "origin_parent_patents_mean", "origin_parent_citations_mean", "origin_parent_firstinv_mean",
  "origin_parent_avg_salary_env", "origin_parent_avg_total_comp_env", "origin_parent_avg_seniority_env", "origin_parent_immig_share",
  "origin_parent_origin_div", "origin_parent_uni_div",
  "origin_firm_n_inventors", "origin_firm_patents_mean", "origin_firm_citations_mean",
  "origin_city_n_inventors", "origin_city_patents_mean", "origin_city_citations_mean",
  "origin_metro_n_inventors", "origin_metro_patents_mean", "origin_metro_citations_mean",
  "avg_salary_lag", "avg_total_comp_lag", "avg_seniority_lag", "n_patents_lag", "n_first_inventor_lag", "n_us_citations_lag", "tenure_main_lag"
), names(ana))

pred_summaries <- rbindlist(lapply(predictor_vars, function(v) {
  data.table(
    variable = v,
    move_rate_parent_consec = ana[is.finite(get(v)), mean(leave_parent_consec, na.rm = TRUE)],
    move_rate_firm_consec   = ana[is.finite(get(v)), mean(leave_firm_consec, na.rm = TRUE)],
    move_rate_city_consec   = ana[is.finite(get(v)), mean(leave_city_consec, na.rm = TRUE)],
    move_rate_metro_consec  = ana[is.finite(get(v)), mean(leave_metro_consec, na.rm = TRUE)],
    p25 = ana[is.finite(get(v)), quantile(get(v), 0.25, na.rm = TRUE)],
    p50 = ana[is.finite(get(v)), quantile(get(v), 0.50, na.rm = TRUE)],
    p75 = ana[is.finite(get(v)), quantile(get(v), 0.75, na.rm = TRUE)],
    n = ana[is.finite(get(v)), .N]
  )
}), fill = TRUE)
write_csv(as_tibble(pred_summaries), file.path(panel_dir, "predictor_summaries.csv"))

delta_vars <- intersect(c(
  delta_specs$out,
  "delta_salary", "delta_total_comp", "delta_seniority",
  "delta_parent_same_origin_share", "delta_parent_same_university_share"
), names(ana))

move_masks <- list(
  parent_only = ana$parent_only_move_consec == 1L,
  city_only = ana$city_only_move_consec == 1L,
  metro_only = ana$metro_only_move_consec == 1L,
  within_parent_firm = ana$within_parent_firm_move_consec == 1L,
  across_parent = ana$across_parent_move_consec == 1L,
  joint_parent_city = ana$joint_parent_city_move_consec == 1L,
  joint_parent_metro = ana$joint_parent_metro_move_consec == 1L,
  all_parent_moves = ana$leave_parent_consec == 1L,
  all_firm_moves = ana$leave_firm_consec == 1L,
  all_city_moves = ana$leave_city_consec == 1L,
  all_metro_moves = ana$leave_metro_consec == 1L
)

od_tbl <- rbindlist(lapply(names(move_masks), function(mn) {
  mask <- move_masks[[mn]]
  rbindlist(lapply(delta_vars, function(v) {
    x <- ana[mask & is.finite(get(v)), get(v)]
    data.table(
      move_type = mn,
      variable = v,
      mean = if (length(x)) mean(x, na.rm = TRUE) else NA_real_,
      median = if (length(x)) median(x, na.rm = TRUE) else NA_real_,
      p25 = if (length(x)) quantile(x, 0.25, na.rm = TRUE) else NA_real_,
      p75 = if (length(x)) quantile(x, 0.75, na.rm = TRUE) else NA_real_,
      share_positive = if (length(x)) mean(x > 0, na.rm = TRUE) else NA_real_,
      n = length(x)
    )
  }), fill = TRUE)
}), fill = TRUE)
write_csv(as_tibble(od_tbl), file.path(od_dir, "origin_destination_deltas_by_move_type.csv"))

# =========================================================
# Figures: predictor plots
# Added explicit city-patents and metro-size plots.
# =========================================================
plot_pairs <- list(
  list(x = "avg_salary_lag",            y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged salary"),
  list(x = "origin_parent_patents_mean",y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged parent patents"),
  list(x = "origin_parent_n_inventors", y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged parent size"),
  list(x = "origin_firm_patents_mean",  y = "leave_firm_consec",   ttl = "Pr(firm move) vs lagged firm patents"),
  list(x = "origin_firm_n_inventors",   y = "leave_firm_consec",   ttl = "Pr(firm move) vs lagged firm size"),
  list(x = "origin_city_patents_mean",  y = "leave_city_consec",   ttl = "Pr(city move) vs lagged city patents"),
  list(x = "origin_city_n_inventors",   y = "leave_city_consec",   ttl = "Pr(city move) vs lagged city size"),
  list(x = "origin_metro_patents_mean", y = "leave_metro_consec",  ttl = "Pr(metro move) vs lagged metro patents"),
  list(x = "origin_metro_n_inventors",  y = "leave_metro_consec",  ttl = "Pr(metro move) vs lagged metro size"),
  list(x = "avg_seniority_lag",         y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged seniority"),
  list(x = "n_patents_lag",             y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged patents")
)

for (pp in plot_pairs) {
  if (all(c(pp$x, pp$y) %in% names(ana))) {
    save_plot(
      bin_and_plot(ana, pp$x, pp$y, pp$ttl),
      file.path(fig_dir, paste0("move_vs_origin_", pp$x, "_", pp$y, ".png"))
    )
  }
}

move_type_plot <- ggplot(move_type_tbl, aes(x = reorder(move_type, share), y = share)) +
  geom_col() +
  coord_flip() +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(title = "Shares of mobility event types", x = NULL, y = "Share") +
  theme_minimal(base_size = 12)
save_plot(move_type_plot, file.path(fig_dir, "move_type_decomposition.png"), 9, 6)

# =========================================================
# Figures: destination - origin densities with tenure-split versions
# =========================================================
density_registry <- rbindlist(list(
  data.table(cluster = "parent", move_var = c("leave_parent_consec", "parent_only_move_consec", "joint_parent_city_move_consec", "joint_parent_metro_move_consec", "across_parent_move_consec")),
  data.table(cluster = "firm",   move_var = c("leave_firm_consec", "within_parent_firm_move_consec", "across_parent_move_consec")),
  data.table(cluster = "city",   move_var = c("leave_city_consec", "city_only_move_consec", "joint_parent_city_move_consec")),
  data.table(cluster = "metro",  move_var = c("leave_metro_consec", "metro_only_move_consec", "joint_parent_metro_move_consec"))
), fill = TRUE)

move_label_map <- c(
  leave_parent_consec = "all parent moves",
  parent_only_move_consec = "parent-only moves",
  joint_parent_city_move_consec = "joint parent-city moves",
  joint_parent_metro_move_consec = "joint parent-metro moves",
  across_parent_move_consec = "across-parent moves",
  leave_firm_consec = "all firm moves",
  within_parent_firm_move_consec = "within-parent firm moves",
  leave_city_consec = "all city moves",
  city_only_move_consec = "city-only moves",
  leave_metro_consec = "all metro moves",
  metro_only_move_consec = "metro-only moves"
)

delta_registry <- merge(density_registry, delta_specs[, .(cluster, measure, delta_var = out)], by = "cluster", allow.cartesian = TRUE)
delta_registry[, move_label := move_label_map[move_var]]
delta_registry[is.na(move_label), move_label := move_var]

density_results <- rbindlist(lapply(seq_len(nrow(delta_registry)), function(i) {
  rr <- delta_registry[i]
  file_stub <- paste0(
    "destination_minus_origin_",
    rr$cluster, "_", rr$measure, "_", rr$move_var
  )
  make_delta_density_bundle(
    DT = ana,
    delta_var = rr$delta_var,
    move_var = rr$move_var,
    cluster_label = rr$cluster,
    measure_label = rr$measure,
    move_label = rr$move_label,
    file_stub = file_stub
  )
}), fill = TRUE)

personal_density_specs <- data.table(
  cluster = "person",
  measure = c("salary", "total_comp", "seniority"),
  delta_var = c("delta_salary", "delta_total_comp", "delta_seniority"),
  move_var = "leave_parent_consec",
  move_label = "all parent moves"
)

density_results_personal <- rbindlist(lapply(seq_len(nrow(personal_density_specs)), function(i) {
  rr <- personal_density_specs[i]
  file_stub <- paste0("destination_minus_origin_", rr$cluster, "_", rr$measure, "_", rr$move_var)
  make_delta_density_bundle(
    DT = ana,
    delta_var = rr$delta_var,
    move_var = rr$move_var,
    cluster_label = rr$cluster,
    measure_label = rr$measure,
    move_label = rr$move_label,
    file_stub = file_stub
  )
}), fill = TRUE)

density_results <- rbindlist(list(density_results, density_results_personal), fill = TRUE)
write_csv(as_tibble(density_results), file.path(od_dir, "origin_destination_density_test_stats.csv"))

coverage_tbl <- density_results[, .(
  produced = sum(produced, na.rm = TRUE),
  skipped = sum(1L - produced, na.rm = TRUE)
), by = .(cluster, move_var, plot_variant)]
write_csv(as_tibble(coverage_tbl), file.path(od_dir, "origin_destination_density_coverage.csv"))

# =========================================================
# Event studies
# =========================================================
if (RUN_EVENT_STUDIES) {
  build_event <- function(DT, event_var, out_stub, event_label) {
    first_ev <- DT[get(event_var) == 1L, .(event_year = min(year, na.rm = TRUE)), by = user_id]
    if (!nrow(first_ev)) return(NULL)

    es <- merge(DT, first_ev, by = "user_id", all.x = FALSE)
    es[, event_time := year - event_year]
    es <- es[event_time >= -EVENT_WINDOW & event_time <= EVENT_WINDOW]

    metrics <- intersect(c(
      "n_patents", "n_first_inventor", "n_us_citations", "avg_salary", "avg_total_comp", "avg_seniority",
      "parent_loo_patents", "firm_loo_patents", "city_loo_patents", "metro_loo_patents",
      "parent_loo_citations", "firm_loo_citations", "city_loo_citations", "metro_loo_citations"
    ), names(es))

    tab <- rbindlist(lapply(metrics, function(m) {
      es[, .(metric = m, mean = mean(get(m), na.rm = TRUE), n = .N), by = event_time]
    }), fill = TRUE)

    write_csv(as_tibble(tab), file.path(event_dir, paste0("eventstudy_", out_stub, ".csv")))

    for (m in unique(tab$metric)) {
      sub <- tab[metric == m]
      g <- ggplot(sub, aes(x = event_time, y = mean)) +
        geom_line() +
        geom_point() +
        geom_vline(xintercept = 0, linetype = 2) +
        labs(title = paste(m, "around", event_label), x = "Event time", y = m) +
        theme_minimal(base_size = 12)
      save_plot(g, file.path(fig_dir, paste0("move_eventstudy_", out_stub, "_", m, ".png")))
    }
    invisible(tab)
  }

  build_event(dt, "leave_parent_consec", "first_parent_move", "first parent move")
  build_event(dt, "within_parent_firm_move_consec", "first_within_parent_firm_move", "first within-parent firm move")
  build_event(dt, "across_parent_move_consec", "first_across_parent_move", "first across-parent move")
  build_event(dt, "leave_city_consec", "first_city_move", "first city move")
  build_event(dt, "leave_metro_consec", "first_metro_move", "first metro move")
  build_event(dt, "joint_parent_city_move_consec", "first_joint_parent_city_move", "first joint parent-city move")
}

# =========================================================
# Heterogeneity
# =========================================================
if (RUN_HETEROGENEITY) {
  if ("avg_seniority" %in% names(dt)) {
    qs <- safe_quantile(dt$avg_seniority, c(1/3, 2/3))
    dt[, seniority_band := fifelse(avg_seniority <= qs[1], "low", fifelse(avg_seniority <= qs[2], "mid", "high"))]
  }

  if (!is.null(imm_var)) dt[, immigrant_group := fifelse(imm == 1L, "immigrant", fifelse(imm == 0L, "non_immigrant", NA_character_))]

  dt[, productivity_band := fifelse(
    total_patents >= quantile(total_patents, 0.90, na.rm = TRUE), "top_decile",
    fifelse(total_patents >= quantile(total_patents, 0.75, na.rm = TRUE), "top_quartile_to_decile", "bottom_75")
  )]

  dt[, career_stage := fifelse(!is.na(tenure_main) & tenure_main <= 10, "early",
                        fifelse(!is.na(tenure_main) & tenure_main <= 20, "mid", "late"))]
  dt[, repeat_churner_group := fifelse(repeat_churner == 1L, "repeat_churner", "stable_or_low_churn")]
  dt[, leadership_band := fifelse(leadership_share >= quantile(leadership_share, 0.90, na.rm = TRUE), "high_leadership", "other")]

  het_groups <- intersect(c(
    "immigrant_group", "seniority_band", "productivity_band", "career_stage", "repeat_churner_group", "leadership_band",
    "career_modal_section", "career_modal_class", "au_sex_predicted", "au_ethnicity_predicted"
  ), names(dt))

  het_tbls <- lapply(het_groups, function(gv) {
    dt[!is.na(get(gv)), .(
      p_move_parent_consec = mean(leave_parent_consec, na.rm = TRUE),
      p_move_firm_consec   = mean(leave_firm_consec, na.rm = TRUE),
      p_move_city_consec   = mean(leave_city_consec, na.rm = TRUE),
      p_move_metro_consec  = mean(leave_metro_consec, na.rm = TRUE),
      p_within_parent_firm_consec = mean(within_parent_firm_move_consec, na.rm = TRUE),
      p_joint_parent_city_consec  = mean(joint_parent_city_move_consec, na.rm = TRUE),
      mean_patents = mean(n_patents, na.rm = TRUE),
      mean_first_inventor = mean(n_first_inventor, na.rm = TRUE),
      mean_salary = mean(avg_salary, na.rm = TRUE),
      mean_total_comp = mean(avg_total_comp, na.rm = TRUE),
      mean_seniority = mean(avg_seniority, na.rm = TRUE),
      n = .N
    ), by = gv]
  })

  het_tbl <- rbindlist(Map(function(gv, tab) { tab[, grouping_var := gv][] }, het_groups, het_tbls), fill = TRUE)
  write_csv(as_tibble(het_tbl), file.path(het_dir, "heterogeneity_move_rates.csv"))

  ana_up <- copy(ana)

  if ("imm" %in% names(ana_up)) ana_up[, immigrant_group := fifelse(imm == 1L, "immigrant", fifelse(imm == 0L, "non_immigrant", NA_character_))]
  if ("total_patents" %in% names(ana_up)) ana_up[, productivity_band := fifelse(total_patents >= quantile(total_patents, 0.90, na.rm = TRUE), "top_decile", "bottom_90")]
  if ("avg_seniority" %in% names(ana_up)) {
    qs <- safe_quantile(ana_up$avg_seniority, c(1/3, 2/3))
    ana_up[, seniority_band := fifelse(avg_seniority <= qs[1], "low", fifelse(avg_seniority <= qs[2], "mid", "high"))]
  }

  upgrade_groups <- intersect(c("immigrant_group", "seniority_band", "productivity_band"), names(ana_up))
  if (length(upgrade_groups)) {
    up_tbl <- rbindlist(lapply(upgrade_groups, function(gv) {
      ana_up[parent_only_move_consec == 1L & !is.na(get(gv)), .(
        mean_delta_parent_patents   = mean(delta_parent_patents, na.rm = TRUE),
        mean_delta_parent_citations = mean(delta_parent_citations, na.rm = TRUE),
        mean_delta_city_patents     = mean(delta_city_patents, na.rm = TRUE),
        mean_delta_metro_patents    = mean(delta_metro_patents, na.rm = TRUE),
        mean_delta_salary           = mean(delta_salary, na.rm = TRUE),
        mean_delta_total_comp       = mean(delta_total_comp, na.rm = TRUE),
        mean_delta_seniority        = mean(delta_seniority, na.rm = TRUE),
        n = .N
      ), by = gv][, grouping_var := gv][]
    }), fill = TRUE)

    write_csv(as_tibble(up_tbl), file.path(het_dir, "heterogeneity_destination_upgrades_parent_only.csv"))
  }
}

# =========================================================
# Memo and output index
# =========================================================
interesting_lines <- c(
  "Extends the original churn script into a broader mobility panel with explicit parent, firm, city, and metro move definitions.",
  "Adds within-parent-firm vs across-parent moves, parent-only vs geography-only vs joint move decomposition, and repeat-churner indicators.",
  "Adds destination-minus-origin comparisons for parent, firm, city, and metro clusters using explicit naming throughout.",
  "Replaces histograms with density plots so centered distributions are easier to read, and adds tenure-split density versions for each destination-origin figure.",
  "Writes beta, standard error, p-value, and N for all movers, early movers, late movers, and the late-minus-early difference, and produces a combined_graphs.pdf with all figures.",
  "Keeps all interpretations descriptive: lagged origin variables for predictors, destination-minus-origin deltas for upgrading, and event studies as descriptive only."
)

write_md(file.path(OUT_DIR, "move_motives_memo.md"), c(
  "# Move motives descriptive pipeline memo", "",
  "## Audit of original code",
  "- The original script mainly studied changes in first_parent_rcid and related churn to lagged parent size, immigrant share, patents, and diversity.",
  "- It did not fully separate parent, firm, city, and metro terminology in the origin-destination distribution outputs.",
  "- It also lacked group-specific tests by mover tenure and a single bundled PDF with all graphs.", "",
  "## What changed",
  paste0("- ", interesting_lines), "",
  "## Caution",
  "- Compensation and seniority may be noisy or selectively missing.",
  "- Same-origin and same-university shares depend on proxy quality for origin and university strings.",
  "- All results are descriptive, not causal."
))

write_combined_pdf(file.path(OUT_DIR, "combined_graphs.pdf"))

output_files <- list.files(OUT_DIR, recursive = TRUE)
write_md(file.path(OUT_DIR, "output_index.md"), c("# Output index", "", paste0("- `", output_files, "`")))

ts_msg("DONE. Outputs in:", OUT_DIR)
