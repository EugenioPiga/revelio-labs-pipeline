#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(scales)
})

options(arrow.skip_nul = TRUE)
options(bitmapType = "cairo")
set.seed(123)
data.table::setDTthreads(1)

# =========================================================
# Config
# =========================================================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR   <- "/home/epiga/revelio_labs/output/mobility_churn_extended_light"
YEAR_MIN  <- 2010L
YEAR_MAX  <- 2019L
US_COUNTRY <- "United States"
LAST_EDU_US_ONLY <- TRUE
MIN_POINTS_PLOT <- 250L
TOP_THRESHOLDS <- c(0.75, 0.90, 0.95)
EVENT_WINDOW <- 5L
COMPUTE_LOO <- TRUE
RUN_EVENT_STUDIES <- FALSE
RUN_HETEROGENEITY <- FALSE
TENURE_SPLIT_CUTOFF <- 15L
DENSITY_ADJUST <- 1.10
PLOT_WINSOR_LOWER <- 0.05
PLOT_WINSOR_UPPER <- 0.95

PARENT_VAR  <- "first_parent_rcid"
FIRM_VAR    <- "first_rcid"
CITY_VAR    <- "first_city"
METRO_VAR   <- "first_metro_area"
COUNTRY_VAR <- "first_country"

IMMIG_CANDIDATES <- c("immig_first_deg_or_job_nonUS", "immig1_post", "immig1_ever", "immig2_post", "immig2_ever")
NETWORK_CANDIDATES <- c("first_university_country", "edu_first_university_country")

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
  as.numeric(quantile(x, probs = probs, na.rm = TRUE, names = FALSE, type = 7))
}

winsorize_vec <- function(x, lower = PLOT_WINSOR_LOWER, upper = PLOT_WINSOR_UPPER) {
  out <- x
  b <- safe_quantile(x, c(lower, upper))
  if (length(b) == 2L && all(is.finite(b))) {
    out <- pmin(pmax(out, b[1]), b[2])
  }
  out
}

save_plot <- function(g, path, width = 8.5, height = 6.5) {
  if (is.null(g)) return(invisible(NULL))
  ggsave(path, g, width = width, height = height, dpi = 300)
  invisible(g)
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
    return(list(beta = NA_real_, se = NA_real_, p_value = NA_real_, n = length(z), mean = NA_real_))
  }
  fit <- summary(lm(z ~ 1))
  cf <- coef(fit)
  list(
    beta = unname(cf[1, 1]),
    se = unname(cf[1, 2]),
    p_value = unname(cf[1, 4]),
    n = length(z),
    mean = mean(z, na.rm = TRUE)
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

mean_level <- function(x) {
  z <- x[is.finite(x)]
  if (!length(z)) return(NA_real_)
  mean(z, na.rm = TRUE)
}

safe_frac <- function(num, den) {
  if (is.na(num) || is.na(den) || den == 0) return(NA_real_)
  num / den
}

format_mover_share_line <- function(lbl, mover_n, pop_n) {
  shr <- safe_frac(mover_n, pop_n)
  paste0(
    lbl,
    ": movers = ", scales::comma(mover_n),
    " / ", scales::comma(pop_n),
    " (", ifelse(is.na(shr), "NA", scales::percent(shr, accuracy = 0.1)), ")"
  )
}

format_stat_line <- function(lbl, stats, mean_origin = NA_real_, mean_destination = NA_real_) {
  paste0(
    lbl,
    ": origin mean = ", sprintf("%.4f", mean_origin),
    ", destination mean = ", sprintf("%.4f", mean_destination),
    ", β = ", sprintf("%.4f", stats$beta),
    ", SE = ", sprintf("%.4f", stats$se),
    ", p = ", format_p(stats$p_value),
    ", N = ", scales::comma(stats$n)
  )
}

add_stats_label <- function(g, stats_lines, width = 95) {
  wrapped_lines <- vapply(
    stats_lines,
    function(x) stringr::str_wrap(x, width = width),
    character(1)
  )

  lbl <- paste(wrapped_lines, collapse = "\n")

  g +
    labs(caption = lbl) +
    theme(
      plot.caption.position = "plot",
      plot.caption = element_text(
        hjust = 0,
        size = 10.5,
        lineheight = 1.08,
        margin = margin(t = 14)
      ),
      plot.margin = margin(10, 10, 30, 10)
    )
}

has_density_support <- function(x) length(unique(x[is.finite(x)])) >= 2L

make_density_plot <- function(full_sub, delta_var, title_txt, xlab_txt, stats_lines, split = FALSE) {
  if (!has_density_support(full_sub[[delta_var]])) return(NULL)
  sub <- copy(full_sub)
  sub[, delta_plot := winsorize_vec(get(delta_var))]
  if (!has_density_support(sub$delta_plot)) return(NULL)

  g <- ggplot(sub, aes(x = delta_plot)) +
    geom_vline(xintercept = 0, linetype = 2, linewidth = 0.4) +
    theme_minimal(base_size = 12)

  if (isTRUE(split)) {
    g <- g +
      geom_density(aes(color = tenure_group, fill = tenure_group), adjust = DENSITY_ADJUST, alpha = 0.18, linewidth = 0.9) +
      labs(title = title_txt, x = xlab_txt, y = "Density", color = NULL, fill = NULL,
           caption = sprintf("Density uses winsorized x only (%.0fth–%.0fth pct). Stats use full valid sample.",
                             100 * PLOT_WINSOR_LOWER, 100 * PLOT_WINSOR_UPPER))
  } else {
    g <- g +
      geom_density(fill = "grey70", color = "grey25", adjust = DENSITY_ADJUST, alpha = 0.45, linewidth = 0.9) +
      labs(title = title_txt, x = xlab_txt, y = "Density",
           caption = sprintf("Density uses winsorized x only (%.0fth–%.0fth pct). Stats use full valid sample.",
                             100 * PLOT_WINSOR_LOWER, 100 * PLOT_WINSOR_UPPER))
  }

  add_stats_label(g, stats_lines)
}

make_delta_density_bundle <- function(DT, delta_var, origin_var, dest_var, move_var, cluster_label, measure_label, move_label, file_stub) {
  needed <- c(delta_var, origin_var, dest_var, move_var)
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
      beta = NA_real_, se = NA_real_, p_value = NA_real_, n = NA_integer_, mean = NA_real_,
      mean_origin = NA_real_, mean_destination = NA_real_
    ))
  }

  tenure_source <- if ("tenure_main_lag" %in% names(DT)) "tenure_main_lag" else if ("tenure_main" %in% names(DT)) "tenure_main" else NULL

  base_pop  <- copy(DT[both_us_consec == 1L])
  mover_pop <- copy(DT[both_us_consec == 1L & get(move_var) == 1L])
  sub       <- copy(DT[both_us_consec == 1L & get(move_var) == 1L & is.finite(get(delta_var))])

  base_pop[, tenure_group := NA_character_]
  mover_pop[, tenure_group := NA_character_]
  sub[, tenure_group := NA_character_]

  if (!is.null(tenure_source)) {
    base_pop[, tenure_group := fifelse(
      get(tenure_source) <= TENURE_SPLIT_CUTOFF, "Early tenure (<=15)",
      fifelse(get(tenure_source) > TENURE_SPLIT_CUTOFF, "Late tenure (>15)", NA_character_)
    )]

    mover_pop[, tenure_group := fifelse(
      get(tenure_source) <= TENURE_SPLIT_CUTOFF, "Early tenure (<=15)",
      fifelse(get(tenure_source) > TENURE_SPLIT_CUTOFF, "Late tenure (>15)", NA_character_)
    )]

    sub[, tenure_group := fifelse(
      get(tenure_source) <= TENURE_SPLIT_CUTOFF, "Early tenure (<=15)",
      fifelse(get(tenure_source) > TENURE_SPLIT_CUTOFF, "Late tenure (>15)", NA_character_)
    )]
  }

  all_pop_n   <- uniqueN(base_pop[!is.na(tenure_group), user_id])
  all_mover_n <- uniqueN(mover_pop[!is.na(tenure_group), user_id])

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
      beta = NA_real_, se = NA_real_, p_value = NA_real_, n = nrow(sub), mean_delta = NA_real_,
      mean_origin = NA_real_, mean_destination = NA_real_
    ))
  }
  overall_stats <- delta_test_stats(sub[[delta_var]])
  overall_origin_mean <- mean_level(sub[[origin_var]])
  overall_destination_mean <- mean_level(sub[[dest_var]])
  overall_plot <- make_density_plot(
    full_sub = sub,
    delta_var = delta_var,
    title_txt = paste0("Destination - origin ", cluster_label, " ", measure_label, " (", move_label, ")"),
    xlab_txt = paste0("Destination - origin ", cluster_label, " ", measure_label),
    stats_lines = c(
      format_mover_share_line("Mover share in eligible population", all_mover_n, all_pop_n),
      format_stat_line("All movers", overall_stats, mean_origin = overall_origin_mean, mean_destination = overall_destination_mean)
    ),
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
    n = overall_stats$n,
    mean_delta = overall_stats$mean,
    mean_origin = overall_origin_mean,
    mean_destination = overall_destination_mean
  )

  if (is.null(tenure_source)) return(out)

  split_sub <- sub[!is.na(tenure_group)]
  if (!nrow(split_sub)) return(out)

  early_stats <- delta_test_stats(split_sub[tenure_group == "Early tenure (<=15)", get(delta_var)])
  late_stats  <- delta_test_stats(split_sub[tenure_group == "Late tenure (>15)", get(delta_var)])
  early_origin_mean <- mean_level(split_sub[tenure_group == "Early tenure (<=15)", get(origin_var)])
  early_destination_mean <- mean_level(split_sub[tenure_group == "Early tenure (<=15)", get(dest_var)])
  late_origin_mean <- mean_level(split_sub[tenure_group == "Late tenure (>15)", get(origin_var)])
  late_destination_mean <- mean_level(split_sub[tenure_group == "Late tenure (>15)", get(dest_var)])
  diff_stats  <- delta_diff_stats(split_sub[[delta_var]], split_sub$tenure_group)

  early_pop_n   <- uniqueN(base_pop[tenure_group == "Early tenure (<=15)", user_id])
  late_pop_n    <- uniqueN(base_pop[tenure_group == "Late tenure (>15)", user_id])

  early_mover_n <- uniqueN(mover_pop[tenure_group == "Early tenure (<=15)", user_id])
  late_mover_n  <- uniqueN(mover_pop[tenure_group == "Late tenure (>15)", user_id])

  split_plot <- make_density_plot(
    full_sub = split_sub,
    delta_var = delta_var,
    title_txt = paste0("Destination - origin ", cluster_label, " ", measure_label, " by mover tenure (", move_label, ")"),
    xlab_txt = paste0("Destination - origin ", cluster_label, " ", measure_label),
    stats_lines = c(
       format_mover_share_line("All mover share", all_mover_n, all_pop_n),
       format_mover_share_line("Early mover share", early_mover_n, early_pop_n),
       format_mover_share_line("Late mover share", late_mover_n, late_pop_n),
       sprintf(
         "All movers: origin mean = %.4f, destination mean = %.4f, β = %.4f, SE = %.4f, p = %s, N = %s",
         overall_origin_mean, overall_destination_mean, overall_stats$beta, overall_stats$se,
         format_p(overall_stats$p_value), scales::comma(overall_stats$n)
       ),
       sprintf(
         "Early: origin mean = %.4f, destination mean = %.4f, β = %.4f, SE = %.4f, p = %s, N = %s",
         early_origin_mean, early_destination_mean, early_stats$beta, early_stats$se,
         format_p(early_stats$p_value), scales::comma(early_stats$n)
       ),
       sprintf(
         "Late: origin mean = %.4f, destination mean = %.4f, β = %.4f, SE = %.4f, p = %s, N = %s",
         late_origin_mean, late_destination_mean, late_stats$beta, late_stats$se,
         format_p(late_stats$p_value), scales::comma(late_stats$n)
       ),
       sprintf(
         "Late - Early: β = %.4f, SE = %.4f, p = %s, N = %s",
         diff_stats$beta, diff_stats$se, format_p(diff_stats$p_value), scales::comma(diff_stats$n)
       )
    ),
    split = TRUE
  )
    save_plot(split_plot, file.path(fig_dir, paste0(file_stub, "_density_tenure_split.png")),
               width = 8.8, height = 7.4)
  split_rows <- rbindlist(list(
    data.table(
      file_stub = file_stub, cluster = cluster_label, measure = measure_label, move_var = move_var,
      move_label = move_label, delta_var = delta_var, plot_variant = "tenure_split",
      stat_type = "early", tenure_group = "Early tenure (<=15)",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = early_stats$beta, se = early_stats$se, p_value = early_stats$p_value,
      n = early_stats$n, mean_delta = early_stats$mean,
      mean_origin = early_origin_mean, mean_destination = early_destination_mean
    ),
    data.table(
      file_stub = file_stub, cluster = cluster_label, measure = measure_label, move_var = move_var,
      move_label = move_label, delta_var = delta_var, plot_variant = "tenure_split",
      stat_type = "late", tenure_group = "Late tenure (>15)",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = late_stats$beta, se = late_stats$se, p_value = late_stats$p_value,
      n = late_stats$n, mean_delta = late_stats$mean,
      mean_origin = late_origin_mean, mean_destination = late_destination_mean
    ),
    data.table(
      file_stub = file_stub, cluster = cluster_label, measure = measure_label, move_var = move_var,
      move_label = move_label, delta_var = delta_var, plot_variant = "tenure_split",
      stat_type = "late_minus_early", tenure_group = "difference",
      produced = as.integer(!is.null(split_plot)),
      reason = fifelse(is.null(split_plot), "insufficient_support", NA_character_),
      beta = diff_stats$beta, se = diff_stats$se, p_value = diff_stats$p_value,
      n = diff_stats$n, mean_delta = NA_real_,
      mean_origin = NA_real_, mean_destination = NA_real_
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

required_core <- c("user_id", "year", "n_patents", PARENT_VAR, FIRM_VAR, CITY_VAR, METRO_VAR, COUNTRY_VAR, "last_university_country")
missing_core <- setdiff(required_core, ds_names)
if (length(missing_core) > 0) stop(paste("Missing required columns:", paste(missing_core, collapse = ", ")))

selected_cols <- unique(c(
  "user_id", "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
  "career_modal_section", "career_modal_class", PARENT_VAR, FIRM_VAR,
  CITY_VAR, METRO_VAR, COUNTRY_VAR, "last_country",
  IMMIG_CANDIDATES, NETWORK_CANDIDATES,
  "first_university_country", "last_university_country",
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
rm(ds)
gc()

# =========================================================
# Clean / construct base variables
# =========================================================
char_cols <- names(df)[vapply(df, function(x) is.character(x) || is.factor(x), logical(1))]
for (cc in char_cols) set(df, j = cc, value = nz_trim(df[[cc]]))

num_cols <- intersect(c(
  "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations"
), names(df))
for (cc in num_cols) set(df, j = cc, value = num_or_na(df[[cc]]))

df[, user_id := as.character(user_id)]
df[, year := as.integer(year)]

if (LAST_EDU_US_ONLY) {
  df <- df[!is.na(last_university_country) & last_university_country == US_COUNTRY]
}

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

df[, edu_year := extract_year(first_startdate_edu)]
df[, pos_year := extract_year(first_startdate_pos)]
df[, first_obs_year := min(year, na.rm = TRUE), by = user_id]
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
dt <- df
rm(df)
gc()
setorder(dt, user_id, year)

# Keep all countries here so lag country is truthful.
dt[, lag_year := shift(year), by = user_id]
dt[, is_consecutive := as.integer(!is.na(lag_year) & year - lag_year == 1L)]

lag_map <- list(
  parent_id = PARENT_VAR,
  firm_id   = FIRM_VAR,
  city_id   = CITY_VAR,
  metro_id  = METRO_VAR,
  country_id = COUNTRY_VAR
)

for (nm in names(lag_map)) {
  vv <- lag_map[[nm]]
  dt[, (nm) := get(vv)]
  dt[, paste0(nm, "_lag") := shift(get(nm)), by = user_id]
}

personal_lag_vars <- intersect(c(
  "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
  "tenure_main", "patenting", "imm"
), names(dt))
for (vv in personal_lag_vars) dt[, paste0(vv, "_lag") := shift(get(vv)), by = user_id]

mk_change <- function(cur, lag) as.integer(!is.na(cur) & !is.na(lag) & cur != lag)

dt[, leave_parent_lastobs := mk_change(parent_id, parent_id_lag)]
dt[, leave_firm_lastobs   := mk_change(firm_id, firm_id_lag)]
dt[, leave_city_lastobs   := mk_change(city_id, city_id_lag)]
dt[, leave_metro_lastobs  := mk_change(metro_id, metro_id_lag)]

for (v in c("leave_parent", "leave_firm", "leave_city", "leave_metro")) {
  dt[, paste0(v, "_consec") := as.integer(is_consecutive == 1L & get(paste0(v, "_lastobs")) == 1L)]
}

dt[, within_parent_firm_move_consec := as.integer(leave_firm_consec == 1L & leave_parent_consec == 0L)]
dt[, across_parent_move_consec      := as.integer(leave_parent_consec == 1L)]
dt[, parent_only_move_consec        := as.integer(leave_parent_consec == 1L & leave_city_consec == 0L)]
dt[, city_only_move_consec          := as.integer(leave_parent_consec == 0L & leave_city_consec == 1L)]
dt[, metro_only_move_consec         := as.integer(leave_parent_consec == 0L & leave_metro_consec == 1L)]
dt[, joint_parent_city_move_consec  := as.integer(leave_parent_consec == 1L & leave_city_consec == 1L)]
dt[, joint_parent_metro_move_consec := as.integer(leave_parent_consec == 1L & leave_metro_consec == 1L)]

dt[, destination_us := as.integer(country_id == US_COUNTRY)]
dt[, origin_us := as.integer(country_id_lag == US_COUNTRY)]
dt[, both_us_consec := as.integer(is_consecutive == 1L & destination_us == 1L & origin_us == 1L)]

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
inv_tot[, repeat_churner := as.integer(n_parent_moves >= 3)]
dt <- merge(dt, inv_tot, by = "user_id", all.x = TRUE)
rm(inv_tot)
gc()

# =========================================================
# Group environments (US current-year rows only)
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
    origin_div = hhi_div(origin_country_proxy)
  ), by = c(idvar, "year")]
  setnames(
    out,
    old = c(idvar, setdiff(names(out), c(idvar, "year"))),
    new = c(idvar, paste0(prefix, "_", setdiff(names(out), c(idvar, "year"))))
  )
  out
}

dt_us <- dt[destination_us == 1L]
parent_env <- group_env(dt_us, "parent_id", "parent")
firm_env   <- group_env(dt_us, "firm_id",   "firm")
city_env   <- group_env(dt_us, "city_id",   "city")
metro_env  <- group_env(dt_us, "metro_id",  "metro")
rm(dt_us)
gc()

# =========================================================
# LOO for panel summary only
# =========================================================
if (COMPUTE_LOO) {
  if (!is.null(parent_env)) {
    dt <- merge(dt, parent_env[, .(parent_id, year, parent_n_inventors, parent_patents_sum, parent_citations_sum)],
                by = c("parent_id", "year"), all.x = TRUE)
    dt[, parent_loo_patents := loo_from_group(parent_patents_sum, parent_n_inventors, n_patents)]
    dt[, parent_loo_citations := loo_from_group(parent_citations_sum, parent_n_inventors, n_us_citations)]
  }
  if (!is.null(firm_env)) {
    dt <- merge(dt, firm_env[, .(firm_id, year, firm_n_inventors, firm_patents_sum, firm_citations_sum)],
                by = c("firm_id", "year"), all.x = TRUE)
    dt[, firm_loo_patents := loo_from_group(firm_patents_sum, firm_n_inventors, n_patents)]
    dt[, firm_loo_citations := loo_from_group(firm_citations_sum, firm_n_inventors, n_us_citations)]
  }
  if (!is.null(city_env)) {
    dt <- merge(dt, city_env[, .(city_id, year, city_n_inventors, city_patents_sum, city_citations_sum)],
                by = c("city_id", "year"), all.x = TRUE)
    dt[, city_loo_patents := loo_from_group(city_patents_sum, city_n_inventors, n_patents)]
    dt[, city_loo_citations := loo_from_group(city_citations_sum, city_n_inventors, n_us_citations)]
  }
  if (!is.null(metro_env)) {
    dt <- merge(dt, metro_env[, .(metro_id, year, metro_n_inventors, metro_patents_sum, metro_citations_sum)],
                by = c("metro_id", "year"), all.x = TRUE)
    dt[, metro_loo_patents := loo_from_group(metro_patents_sum, metro_n_inventors, n_patents)]
    dt[, metro_loo_citations := loo_from_group(metro_citations_sum, metro_n_inventors, n_us_citations)]
  }
}

# =========================================================
# Summary tables
# =========================================================
panel_keep <- unique(c(
  "user_id", "year", "tenure_main", "imm", "n_patents", "n_us_citations",
  "n_first_inventor", "patenting", "parent_id", "parent_id_lag", "firm_id", "firm_id_lag", "city_id", "city_id_lag",
  "metro_id", "metro_id_lag", "country_id", "country_id_lag", "both_us_consec",
  grep("^leave_", names(dt), value = TRUE), grep("_move_", names(dt), value = TRUE),
  grep("^cum_", names(dt), value = TRUE), grep("^top_", names(dt), value = TRUE),
  "repeat_churner", "parent_loo_patents", "firm_loo_patents", "city_loo_patents", "metro_loo_patents"
))
write_csv(as_tibble(dt[, ..panel_keep]), file.path(panel_dir, "mobility_panel_compact.csv"))

move_type_tbl <- rbindlist(list(
  dt[destination_us == 1L, .(move_type = "within_parent_firm_consec", share = mean(within_parent_firm_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "across_parent_consec", share = mean(across_parent_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "parent_only_consec", share = mean(parent_only_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "city_only_consec", share = mean(city_only_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "metro_only_consec", share = mean(metro_only_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "joint_parent_city_consec", share = mean(joint_parent_city_move_consec, na.rm = TRUE), n = .N)],
  dt[destination_us == 1L, .(move_type = "joint_parent_metro_consec", share = mean(joint_parent_metro_move_consec, na.rm = TRUE), n = .N)]
), fill = TRUE)
write_csv(as_tibble(move_type_tbl), file.path(panel_dir, "move_type_shares.csv"))

# =========================================================
# Predictor plots and OD summaries done cluster by cluster
# =========================================================
merge_origin_env <- function(base, env, lag_id) {
  if (is.null(env)) return(base)
  val_cols <- setdiff(names(env), c(lag_id, "year"))
  env2 <- copy(env)
  setnames(env2, lag_id, paste0(lag_id, "_lag_join"))
  setnames(env2, val_cols, paste0("origin_", val_cols))
  merge(base, env2,
        by.x = c(paste0(lag_id, "_lag"), "year_lag"),
        by.y = c(paste0(lag_id, "_lag_join"), "year"),
        all.x = TRUE, sort = FALSE)
}

merge_dest_env <- function(base, env, idvar) {
  if (is.null(env)) return(base)
  val_cols <- setdiff(names(env), c(idvar, "year"))
  env2 <- copy(env)
  setnames(env2, val_cols, paste0("dest_", val_cols))
  merge(base, env2, by = c(idvar, "year"), all.x = TRUE, sort = FALSE)
}

cluster_specs <- list(
  list(cluster = "parent", id = "parent_id", env = parent_env,
       move_vars = c("leave_parent_consec", "parent_only_move_consec", "joint_parent_city_move_consec", "joint_parent_metro_move_consec", "across_parent_move_consec"),
       predictor_pairs = list(
         list(x = "origin_parent_patents_mean", y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged parent patents"),
         list(x = "origin_parent_n_inventors", y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged parent size"),
         list(x = "n_patents_lag", y = "leave_parent_consec", ttl = "Pr(parent move) vs lagged patents")
       )),
  list(cluster = "firm", id = "firm_id", env = firm_env,
       move_vars = c("leave_firm_consec", "within_parent_firm_move_consec", "across_parent_move_consec"),
       predictor_pairs = list(
         list(x = "origin_firm_patents_mean", y = "leave_firm_consec", ttl = "Pr(firm move) vs lagged firm patents"),
         list(x = "origin_firm_n_inventors", y = "leave_firm_consec", ttl = "Pr(firm move) vs lagged firm size")
       )),
  list(cluster = "city", id = "city_id", env = city_env,
       move_vars = c("leave_city_consec", "city_only_move_consec", "joint_parent_city_move_consec"),
       predictor_pairs = list(
         list(x = "origin_city_patents_mean", y = "leave_city_consec", ttl = "Pr(city move) vs lagged city patents"),
         list(x = "origin_city_n_inventors", y = "leave_city_consec", ttl = "Pr(city move) vs lagged city size")
       )),
  list(cluster = "metro", id = "metro_id", env = metro_env,
       move_vars = c("leave_metro_consec", "metro_only_move_consec", "joint_parent_metro_move_consec"),
       predictor_pairs = list(
         list(x = "origin_metro_patents_mean", y = "leave_metro_consec", ttl = "Pr(metro move) vs lagged metro patents"),
         list(x = "origin_metro_n_inventors", y = "leave_metro_consec", ttl = "Pr(metro move) vs lagged metro size")
       ))
)

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

all_od_tbl <- list()
all_density_results <- list()

for (sp in cluster_specs) {
  ts_msg("Processing cluster:", sp$cluster)
  idv <- sp$id
  env <- sp$env
  if (is.null(env)) next

  ana <- copy(dt[!is.na(get(idv)) | !is.na(get(paste0(idv, "_lag")))])
  ana[, year_lag := year - 1L]
  keep_cols <- unique(c(
    "user_id", "year", "year_lag", "tenure_main", "tenure_main_lag", "both_us_consec",
    "n_patents_lag", "n_patents", "n_first_inventor", "n_us_citations",
    idv, paste0(idv, "_lag"), sp$move_vars
  ))
  ana <- ana[, ..keep_cols]
  ana <- merge_origin_env(ana, env, idv)
  ana <- merge_dest_env(ana, env, idv)

  delta_spec_rows <- list(
    list(measure = "size",              dest = paste0("dest_", sp$cluster, "_n_inventors"),    origin = paste0("origin_", sp$cluster, "_n_inventors"),    out = paste0("delta_", sp$cluster, "_size")),
    list(measure = "patents",           dest = paste0("dest_", sp$cluster, "_patents_mean"),   origin = paste0("origin_", sp$cluster, "_patents_mean"),   out = paste0("delta_", sp$cluster, "_patents")),
    list(measure = "citations",         dest = paste0("dest_", sp$cluster, "_citations_mean"), origin = paste0("origin_", sp$cluster, "_citations_mean"), out = paste0("delta_", sp$cluster, "_citations")),
    list(measure = "first_inventor",    dest = paste0("dest_", sp$cluster, "_firstinv_mean"),  origin = paste0("origin_", sp$cluster, "_firstinv_mean"),  out = paste0("delta_", sp$cluster, "_first_inventor")),
    list(measure = "immigrant_share",   dest = paste0("dest_", sp$cluster, "_immig_share"),    origin = paste0("origin_", sp$cluster, "_immig_share"),    out = paste0("delta_", sp$cluster, "_immigrant_share")),
    list(measure = "origin_diversity",  dest = paste0("dest_", sp$cluster, "_origin_div"),     origin = paste0("origin_", sp$cluster, "_origin_div"),     out = paste0("delta_", sp$cluster, "_origin_diversity"))
  )
  delta_specs <- rbindlist(lapply(delta_spec_rows, as.data.table), fill = TRUE)
  for (i in seq_len(nrow(delta_specs))) calc_delta(ana, delta_specs$dest[i], delta_specs$origin[i], delta_specs$out[i])

  # predictor plots
  for (pp in sp$predictor_pairs) {
    if (all(c(pp$x, pp$y) %in% names(ana))) {
      save_plot(
        bin_and_plot(ana[both_us_consec == 1L], pp$x, pp$y, pp$ttl),
        file.path(fig_dir, paste0("move_vs_origin_", pp$x, "_", pp$y, ".png"))
      )
    }
  }

  # summary table
  delta_vars <- intersect(delta_specs$out, names(ana))
  move_masks <- lapply(sp$move_vars, function(mv) ana$both_us_consec == 1L & ana[[mv]] == 1L)
  names(move_masks) <- sp$move_vars
  od_tbl <- rbindlist(lapply(names(move_masks), function(mn) {
    mask <- move_masks[[mn]]
    rbindlist(lapply(delta_vars, function(v) {
      x <- ana[mask & is.finite(get(v)), get(v)]
      data.table(
        cluster = sp$cluster,
        move_type = mn,
        variable = v,
        mean = if (length(x)) mean(x, na.rm = TRUE) else NA_real_,
        mean_origin = {
          rowi <- which(delta_specs$out == v)[1]
          ov <- if (!is.na(rowi)) delta_specs$origin[rowi] else NA_character_
          if (!is.na(ov) && ov %in% names(ana)) mean(ana[mask & is.finite(get(v)), get(ov)], na.rm = TRUE) else NA_real_
        },
        mean_destination = {
          rowi <- which(delta_specs$out == v)[1]
          dv <- if (!is.na(rowi)) delta_specs$dest[rowi] else NA_character_
          if (!is.na(dv) && dv %in% names(ana)) mean(ana[mask & is.finite(get(v)), get(dv)], na.rm = TRUE) else NA_real_
        },
        median = if (length(x)) median(x, na.rm = TRUE) else NA_real_,
        p25 = if (length(x)) quantile(x, 0.25, na.rm = TRUE) else NA_real_,
        p75 = if (length(x)) quantile(x, 0.75, na.rm = TRUE) else NA_real_,
        share_positive = if (length(x)) mean(x > 0, na.rm = TRUE) else NA_real_,
        n = length(x)
      )
    }), fill = TRUE)
  }), fill = TRUE)
  all_od_tbl[[sp$cluster]] <- od_tbl

  # density plots + test stats
  density_registry <- merge(
    data.table(cluster = sp$cluster, move_var = sp$move_vars),
    delta_specs[, .(cluster = sp$cluster, measure, delta_var = out, origin_var = origin, dest_var = dest)],
    by = "cluster", allow.cartesian = TRUE
  )
  density_registry[, move_label := move_label_map[move_var]]
  density_registry[is.na(move_label), move_label := move_var]

  density_results <- rbindlist(lapply(seq_len(nrow(density_registry)), function(i) {
    rr <- density_registry[i]
    file_stub <- paste0("destination_minus_origin_", rr$cluster, "_", rr$measure, "_", rr$move_var)
    make_delta_density_bundle(
      DT = ana,
      delta_var = rr$delta_var,
      origin_var = rr$origin_var,
      dest_var = rr$dest_var,
      move_var = rr$move_var,
      cluster_label = rr$cluster,
      measure_label = rr$measure,
      move_label = rr$move_label,
      file_stub = file_stub
    )
  }), fill = TRUE)
  all_density_results[[sp$cluster]] <- density_results

  rm(ana, od_tbl, density_results, density_registry, delta_specs)
  gc()
}

write_csv(as_tibble(rbindlist(all_od_tbl, fill = TRUE)), file.path(od_dir, "origin_destination_deltas_by_move_type.csv"))
write_csv(as_tibble(rbindlist(all_density_results, fill = TRUE)), file.path(od_dir, "origin_destination_density_test_stats.csv"))
coverage_tbl <- rbindlist(all_density_results, fill = TRUE)[, .(
  produced = sum(produced, na.rm = TRUE),
  skipped = sum(1L - produced, na.rm = TRUE)
), by = .(cluster, move_var, plot_variant)]
write_csv(as_tibble(coverage_tbl), file.path(od_dir, "origin_destination_density_coverage.csv"))

# =========================================================
# Event studies (descriptive, US-to-US only)
# =========================================================
if (RUN_EVENT_STUDIES) {
  build_event <- function(DT, event_var, out_stub, event_label) {
    base <- DT[both_us_consec == 1L]
    first_ev <- base[get(event_var) == 1L, .(event_year = min(year, na.rm = TRUE)), by = user_id]
    if (!nrow(first_ev)) return(NULL)
    es <- merge(base, first_ev, by = "user_id", all.x = FALSE)
    es[, event_time := year - event_year]
    es <- es[event_time >= -EVENT_WINDOW & event_time <= EVENT_WINDOW]
    metrics <- intersect(c("n_patents", "n_first_inventor", "n_us_citations", "parent_loo_patents", "firm_loo_patents", "city_loo_patents", "metro_loo_patents"), names(es))
    tab <- rbindlist(lapply(metrics, function(m) es[, .(metric = m, mean = mean(get(m), na.rm = TRUE), n = .N), by = event_time]), fill = TRUE)
    write_csv(as_tibble(tab), file.path(event_dir, paste0("eventstudy_", out_stub, ".csv")))
    invisible(tab)
  }
  build_event(dt, "leave_parent_consec", "first_parent_move", "first parent move")
  build_event(dt, "within_parent_firm_move_consec", "first_within_parent_firm_move", "first within-parent firm move")
  build_event(dt, "across_parent_move_consec", "first_across_parent_move", "first across-parent move")
  build_event(dt, "leave_city_consec", "first_city_move", "first city move")
  build_event(dt, "leave_metro_consec", "first_metro_move", "first metro move")
}

# =========================================================
# Memo and output index
# =========================================================
interesting_lines <- c(
  "Lightened the original churn script by narrowing loaded columns and removing salary, compensation, and university-diversity analyses.",
  "Computes lags before any US-current-year restriction so origin country is measured correctly.",
  "Destination-origin plots and summaries now require both destination and origin to be US and consecutive.",
  "Density plots use winsorized x-values only for visualization; beta, standard error, p-value, N, and means are computed on the full valid sample.",
  "Processes parent, firm, city, and metro origin-destination analyses one cluster at a time to reduce peak memory.",
  "Removes the combined PDF plot registry to avoid keeping all ggplot objects in memory."
)

write_md(file.path(OUT_DIR, "move_motives_memo.md"), c(
  "# Move motives descriptive pipeline memo", "",
  "## What changed",
  paste0("- ", interesting_lines), "",
  "## Caution",
  "- Results remain descriptive, not causal.",
  "- Origin and destination comparisons are now restricted to US-to-US consecutive moves only.",
  sprintf("- Density plots winsorize at %.0fth/%.0fth percentiles for display only.", 100 * PLOT_WINSOR_LOWER, 100 * PLOT_WINSOR_UPPER)
))

output_files <- list.files(OUT_DIR, recursive = TRUE)
write_md(file.path(OUT_DIR, "output_index.md"), c("# Output index", "", paste0("- `", output_files, "`")))

ts_msg("DONE. Outputs in:", OUT_DIR)
