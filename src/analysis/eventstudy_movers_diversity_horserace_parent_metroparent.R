#!/usr/bin/env Rscript
###############################################################################
# EVENT STUDY: MOVES TO MORE DIVERSE CLUSTERS — HORSE RACE VERSION
#
# Design:
#   - Event = inventor's first move across chosen cluster definition
#   - Treatment intensity of interest = destination diversity(t-1) - origin diversity(t-1)
#   - Diversity computed on FULL SAMPLE (no US filter)
#   - US filter applied RIGHT BEFORE regression
#   - Samples: ALL / NATIVE / IMMIGRANT
#   - Cluster definitions: PARENT / METRO_PARENT
#   - Diversity measures: origin / job / edu
#   - Metrics: hhi (=1-HHI) / entropy
#   - Weighting: unweighted / weighted by current cluster-year size
#
# Horse race:
#   - event-time × delta_div
#   - event-time × delta_log_size
#   - event-time × delta_imm_share
#
# Current controls in regression:
#   - log_cluster_size_current
#   - immig_share_current
#   - tenure
#   - tenure_sq
#
# Output:
#   coefficient CSVs + plots
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")
gc(full = TRUE)

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c(
  "arrow", "data.table", "dplyr", "fixest", "ggplot2",
  "stringr", "readr", "lubridate", "tibble", "scales"
)

for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(fixest)
  library(ggplot2)
  library(stringr)
  library(readr)
  library(lubridate)
  library(tibble)
  library(scales)
})

data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))
set.seed(123)

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_movers_diversity_horserace_parent_metroparent"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

DIRS <- list(
  coeffs = file.path(OUT_DIR, "coefficients"),
  plots  = file.path(OUT_DIR, "plots"),
  logs   = file.path(OUT_DIR, "intermediate")
)
invisible(lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE))

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
PARENT_VAR <- "first_parent_rcid"
US_COUNTRY <- "United States"
TENURE_MAX <- 50
YEAR_MIN   <- 2010
YEAR_MAX   <- 2024

METRO_CANDIDATES <- c(
  "first_metro", "first_metro_area", "metro", "metro_name",
  "first_msa", "msa", "msa_name", "first_cbsa", "cbsa", "cbsa_name"
)

# Window size passed as argument; defaults to 5
args <- commandArgs(trailingOnly = TRUE)
WIN <- if (length(args) >= 1) as.integer(args[1]) else 5L
T_MAX    <- WIN
MIN_PRE  <- WIN
MIN_POST <- WIN
SUFFIX   <- paste0("t", WIN)

# =========================
# Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

pick_first_existing <- function(candidates, available, what = "variable") {
  hit <- intersect(candidates, available)
  if (length(hit) == 0) {
    stop(paste0("[ERROR] Could not find any ", what, ". Tried: ",
                paste(candidates, collapse = ", ")))
  }
  hit[1]
}

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}

safe_name <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "X", x)
}

et_safe <- function(k) ifelse(k < 0, paste0("m", abs(k)), as.character(k))

parse_event_time_from_term <- function(term) {
  z <- sub("^evt_", "", term)
  z <- sub("_delta_div$", "", z)
  z <- sub("_delta_size$", "", z)
  z <- sub("_delta_imm$", "", z)
  ifelse(grepl("^m", z), -as.numeric(sub("^m", "", z)), as.numeric(z))
}

sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}

theme_eug <- function() {
  theme_minimal(base_size = 13) +
    theme(
      panel.grid.minor = element_blank(),
      plot.title = element_text(face = "bold"),
      legend.position = "right"
    )
}

run_fepois_safe <- function(formula, data, weight_var = NULL) {
  x <- copy(as.data.table(data))

  if (is.null(weight_var)) {
    fixest::fepois(
      formula,
      data = x,
      vcov = ~user_id,
      notes = FALSE,
      warn = FALSE
    )
  } else {
    x[, w_use := pmax(get(weight_var), 1)]
    fixest::fepois(
      formula,
      data = x,
      vcov = ~user_id,
      weights = ~w_use,
      notes = FALSE,
      warn = FALSE
    )
  }
}

# =========================
# Derived row-level vars
# =========================
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA_real_, tenure),
      tenure_sq = tenure^2
    )
}

make_origin_country <- function(df) {
  df %>%
    mutate(
      uni_c  = str_trim(as.character(first_university_country)),
      pos_c  = str_trim(as.character(first_pos_country)),
      uni_ok = !is.na(uni_c) & uni_c != "" & uni_c != US_COUNTRY,
      pos_ok = !is.na(pos_c) & pos_c != "" & pos_c != US_COUNTRY,
      origin_country = case_when(
        .data[[IMMIG_VAR]] == 0L ~ US_COUNTRY,
        .data[[IMMIG_VAR]] == 1L & uni_ok ~ uni_c,
        .data[[IMMIG_VAR]] == 1L & !uni_ok & pos_ok ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# =========================
# Cluster keys
# =========================
make_cluster_keys <- function(df, metro_var) {
  df %>%
    mutate(
      parent_id = as.character(.data[[PARENT_VAR]]),
      metro_val = str_trim(as.character(.data[[metro_var]])),
      metro_val = ifelse(metro_val == "", NA_character_, metro_val),
      parent_id = ifelse(parent_id == "", NA_character_, parent_id),
      cluster_parent = parent_id,
      cluster_metro_parent = case_when(
        !is.na(parent_id) & !is.na(metro_val) ~ paste0(parent_id, "||", metro_val),
        TRUE ~ NA_character_
      )
    )
}

# =========================
# FULL-SAMPLE cluster controls
# =========================
build_cluster_controls_table <- function(df, cluster_var) {
  dt <- as.data.table(df[, c("user_id", "year", cluster_var, IMMIG_VAR), drop = FALSE])
  setnames(dt, cluster_var, "cluster_id")

  dt[, cluster_id := as.character(cluster_id)]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]

  dt <- unique(
    dt[!is.na(user_id) & !is.na(year) & !is.na(cluster_id),
       .(user_id, year, cluster_id, imm = as.integer(get(IMMIG_VAR)))]
  )

  if (nrow(dt) == 0) {
    return(list(
      overall = data.table(
        cluster_id = character(),
        year = integer(),
        cluster_size_current = integer(),
        log_cluster_size_current = numeric(),
        immig_share_current = numeric()
      ),
      loo = data.table(
        user_id = character(),
        cluster_id = character(),
        year = integer(),
        loo_log_cluster_size_value = numeric(),
        loo_immig_share_value = numeric()
      )
    ))
  }

  agg <- dt[, .(
    cluster_size_current = .N,
    imm_count = sum(imm, na.rm = TRUE),
    immig_share_current = mean(imm, na.rm = TRUE)
  ), by = .(cluster_id, year)]

  agg[, log_cluster_size_current := log(pmax(cluster_size_current, 1))]
  overall <- agg[, .(cluster_id, year, cluster_size_current, log_cluster_size_current, immig_share_current)]

  loo <- merge(dt, agg, by = c("cluster_id", "year"), all.x = TRUE)

  loo[, loo_cluster_size_value := fifelse(cluster_size_current <= 1, NA_real_, cluster_size_current - 1)]
  loo[, loo_log_cluster_size_value := fifelse(cluster_size_current <= 1, NA_real_, log(cluster_size_current - 1))]
  loo[, loo_immig_share_value := fifelse(
    cluster_size_current <= 1,
    NA_real_,
    (imm_count - imm) / (cluster_size_current - 1)
  )]

  loo <- loo[, .(user_id, cluster_id, year, loo_log_cluster_size_value, loo_immig_share_value)]

  list(overall = overall, loo = loo)
}

# =========================
# FULL-SAMPLE diversity tables (overall + LOO)
# =========================
build_diversity_tables <- function(df, cluster_var, cat_var, metric = c("hhi", "entropy")) {
  metric <- match.arg(metric)

  dt <- as.data.table(df[, c("user_id", "year", cluster_var, cat_var), drop = FALSE])
  setnames(dt, c(cluster_var, cat_var), c("cluster_id", "cat_value"))

  dt[, cluster_id := as.character(cluster_id)]
  dt[, cat_value := trimws(as.character(cat_value))]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]
  dt[cat_value == "" | cat_value == "empty" | cat_value == "NA", cat_value := NA_character_]

  dt <- unique(
    dt[!is.na(user_id) & !is.na(year) & !is.na(cluster_id) & !is.na(cat_value),
       .(user_id, year, cluster_id, cat_value)]
  )

  if (nrow(dt) == 0) {
    return(list(
      overall = data.table(cluster_id = character(), year = integer(), div_value = numeric()),
      loo = data.table(user_id = character(), cluster_id = character(), year = integer(), loo_div_value = numeric())
    ))
  }

  cnt <- dt[, .(n_cat = .N), by = .(cluster_id, year, cat_value)]
  agg <- cnt[, .(
    cluster_size = sum(n_cat),
    S2   = sum(n_cat^2),
    Slog = sum(n_cat * log(n_cat))
  ), by = .(cluster_id, year)]

  overall <- unique(merge(cnt, agg, by = c("cluster_id", "year"), all.x = TRUE)[,
    .(cluster_id, year, cluster_size, S2, Slog)
  ])

  if (metric == "hhi") {
    overall[, div_value := ifelse(cluster_size > 0, 1 - S2 / (cluster_size^2), NA_real_)]
  } else {
    overall[, div_value := ifelse(cluster_size > 0, log(cluster_size) - Slog / cluster_size, NA_real_)]
  }
  overall <- overall[, .(cluster_id, year, div_value)]

  loo <- merge(dt, cnt, by = c("cluster_id", "year", "cat_value"), all.x = TRUE)
  loo <- merge(loo, agg, by = c("cluster_id", "year"), all.x = TRUE)

  if (metric == "hhi") {
    loo[, loo_div_value := fifelse(
      cluster_size <= 1,
      NA_real_,
      1 - ((S2 - n_cat^2 + pmax(n_cat - 1L, 0L)^2) / ((cluster_size - 1)^2))
    )]
  } else {
    loo[, adj_slog := Slog - n_cat * log(n_cat) +
          fifelse(n_cat > 1, (n_cat - 1) * log(n_cat - 1), 0)]
    loo[, loo_div_value := fifelse(
      cluster_size <= 1,
      NA_real_,
      log(cluster_size - 1) - adj_slog / (cluster_size - 1)
    )]
  }

  loo <- loo[, .(user_id, cluster_id, year, loo_div_value)]

  list(overall = overall, loo = loo)
}

# =========================
# Move tagging
# =========================
tag_first_move <- function(dt, cluster_var) {
  x <- copy(as.data.table(dt))
  x[, cluster_id_move := as.character(get(cluster_var))]
  x[cluster_id_move == "" | cluster_id_move == "NA", cluster_id_move := NA_character_]
  setorder(x, user_id, year)

  x[, cluster_lag := shift(cluster_id_move), by = user_id]
  x[, moved_now := !is.na(cluster_id_move) & !is.na(cluster_lag) & cluster_id_move != cluster_lag]

  mv <- x[moved_now == TRUE, .SD[1], by = user_id]
  mv <- mv[, .(
    user_id,
    move_year = year,
    origin_cluster = cluster_lag,
    dest_cluster = cluster_id_move
  )]

  x <- merge(x, mv, by = "user_id", all.x = TRUE)
  x[, c("cluster_lag", "moved_now") := NULL]
  x[]
}

# =========================
# Attach mover-specific deltas
# =========================
attach_move_deltas <- function(dt, div_tables, ctrl_tables, cluster_var) {
  x <- copy(as.data.table(dt))

  movers <- unique(x[!is.na(move_year), .(user_id, move_year, origin_cluster, dest_cluster)])
  if (nrow(movers) == 0) {
    x[, `:=`(
      delta_div = 0,
      delta_log_size = 0,
      delta_imm_share = 0
    )]
    return(x[])
  }

  # Origin values at t-1: LOO, since mover is still in origin cluster
  origin_q <- movers[, .(
    user_id,
    year = move_year - 1L,
    cluster_id = origin_cluster
  )]

  origin_div <- merge(origin_q, div_tables$loo, by = c("user_id", "year", "cluster_id"), all.x = TRUE)
  setnames(origin_div, "loo_div_value", "origin_div_pre")

  origin_ctrl <- merge(origin_q, ctrl_tables$loo, by = c("user_id", "year", "cluster_id"), all.x = TRUE)
  setnames(origin_ctrl, c("loo_log_cluster_size_value", "loo_immig_share_value"),
           c("origin_log_size_pre", "origin_imm_share_pre"))

  # Destination values at t-1: overall, since mover is not yet there
  dest_q <- movers[, .(
    user_id,
    year = move_year - 1L,
    cluster_id = dest_cluster
  )]

  dest_div <- merge(dest_q, div_tables$overall, by = c("year", "cluster_id"), all.x = TRUE)
  setnames(dest_div, "div_value", "dest_div_pre")

  dest_ctrl <- merge(dest_q, ctrl_tables$overall, by = c("year", "cluster_id"), all.x = TRUE)
  setnames(dest_ctrl, c("log_cluster_size_current", "immig_share_current"),
           c("dest_log_size_pre", "dest_imm_share_pre"))

  mv <- merge(origin_div[, .(user_id, origin_div_pre)],
              dest_div[, .(user_id, dest_div_pre)],
              by = "user_id", all = TRUE)

  mv <- merge(mv,
              origin_ctrl[, .(user_id, origin_log_size_pre, origin_imm_share_pre)],
              by = "user_id", all = TRUE)

  mv <- merge(mv,
              dest_ctrl[, .(user_id, dest_log_size_pre, dest_imm_share_pre)],
              by = "user_id", all = TRUE)

  mv[, delta_div       := dest_div_pre       - origin_div_pre]
  mv[, delta_log_size  := dest_log_size_pre  - origin_log_size_pre]
  mv[, delta_imm_share := dest_imm_share_pre - origin_imm_share_pre]

  x <- merge(
    x,
    mv[, .(user_id, delta_div, delta_log_size, delta_imm_share)],
    by = "user_id",
    all.x = TRUE
  )

  x[is.na(move_year), `:=`(
    delta_div = 0,
    delta_log_size = 0,
    delta_imm_share = 0
  )]

  x[]
}

# =========================
# Event-time prep AFTER US filter
# =========================
prep_event_time_for_reg <- function(dt) {
  x <- copy(as.data.table(dt))
  setorder(x, user_id, year)

  x[, event_time_raw := fifelse(is.na(move_year), NA_integer_, year - move_year)]

  supp <- x[!is.na(event_time_raw), .(
    min_r = suppressWarnings(min(event_time_raw, na.rm = TRUE)),
    max_r = suppressWarnings(max(event_time_raw, na.rm = TRUE))
  ), by = user_id]

  supp[, has_support := (min_r <= -MIN_PRE & max_r >= MIN_POST)]
  x <- merge(x, supp[, .(user_id, has_support)], by = "user_id", all.x = TRUE)

  # keep movers with enough support + all never-movers
  x <- x[(has_support == TRUE | is.na(move_year))]

  x[, event_time := fifelse(
    is.na(event_time_raw),
    NA_integer_,
    pmax(pmin(event_time_raw, T_MAX), -T_MAX)
  )]

  x[, c("event_time_raw", "has_support") := NULL]
  x[]
}

add_event_dummies <- function(df) {
  x <- copy(df)

  # reference period among movers
  ref_val <- ifelse(any(!is.na(x$event_time) & x$event_time == -1L), -1L, 0L)
  event_levels <- setdiff(seq(-T_MAX, T_MAX), ref_val)

  for (k in event_levels) {
    sk <- et_safe(k)
    v_main <- paste0("evt_", sk)
    x[[v_main]] <- as.numeric(!is.na(x$event_time) & x$event_time == k)

    x[[paste0("evt_", sk, "_delta_div")]]  <- x[[v_main]] * x$delta_div
    x[[paste0("evt_", sk, "_delta_size")]] <- x[[v_main]] * x$delta_log_size
    x[[paste0("evt_", sk, "_delta_imm")]]  <- x[[v_main]] * x$delta_imm_share
  }

  list(data = x, ref_val = ref_val, event_levels = event_levels)
}

# =========================
# Sample splits
# =========================
apply_sample_split <- function(df, sample_tag) {
  if (sample_tag == "ALL") {
    df
  } else if (sample_tag == "NATIVE") {
    df %>% filter(.data[[IMMIG_VAR]] == 0L)
  } else if (sample_tag == "IMMIGRANT") {
    df %>% filter(.data[[IMMIG_VAR]] == 1L)
  } else {
    stop("Unknown sample_tag")
  }
}

# =========================
# Output helpers
# =========================
extract_event_coefs <- function(model, meta) {
  ct <- fixest::coeftable(model)
  if (is.null(ct) || nrow(ct) == 0) return(NULL)

  out <- tibble(
    term = rownames(ct),
    estimate = as.numeric(ct[, 1]),
    std.error = as.numeric(ct[, 2]),
    stat = as.numeric(ct[, 3]),
    p.value = as.numeric(ct[, 4])
  ) %>%
    mutate(
      ci_low = estimate - 1.96 * std.error,
      ci_high = estimate + 1.96 * std.error,
      component = case_when(
        str_detect(term, "_delta_div$")  ~ "Delta_div",
        str_detect(term, "_delta_size$") ~ "Delta_size",
        str_detect(term, "_delta_imm$")  ~ "Delta_imm",
        str_detect(term, "^evt_")        ~ "Mover_main",
        TRUE ~ "Other"
      )
    ) %>%
    filter(component != "Other") %>%
    mutate(
      event_time = parse_event_time_from_term(term),
      stars = sig_stars(p.value),
      cluster_def = meta$cluster_def,
      diversity_source = meta$diversity_source,
      diversity_metric = meta$diversity_metric,
      sample = meta$sample,
      weight = meta$weight,
      n_obs = meta$n_obs,
      n_users = meta$n_users,
      n_movers = meta$n_movers,
      ref_event_time = meta$ref_event_time
    )

  out
}

make_event_plot <- function(coef_df, meta, component_keep) {
  z <- coef_df %>% filter(component == component_keep)
  if (nrow(z) == 0) return(NULL)

  ttl <- switch(
    component_keep,
    "Mover_main" = "Average move path",
    "Delta_div"  = "Interaction with destination-origin diversity gap",
    "Delta_size" = "Interaction with destination-origin log-size gap",
    "Delta_imm"  = "Interaction with destination-origin immigrant-share gap"
  )

  ylab <- switch(
    component_keep,
    "Mover_main" = "Beta on event-time dummy",
    "Delta_div"  = "Beta on event-time × delta diversity",
    "Delta_size" = "Beta on event-time × delta log size",
    "Delta_imm"  = "Beta on event-time × delta immigrant share"
  )

  ggplot(z, aes(x = event_time, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = -0.5, linetype = "dotted") +
    geom_ribbon(aes(ymin = ci_low, ymax = ci_high), alpha = 0.15) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = sort(unique(z$event_time))) +
    labs(
      title = paste0(meta$cluster_def, " | ", meta$diversity_source, " | ",
                     toupper(meta$diversity_metric), " | ", meta$sample, " | ", meta$weight),
      subtitle = ttl,
      x = "Event time (move year = 0)",
      y = ylab,
      caption = paste0("US-only estimation sample; diversity computed on full sample; ref = ", meta$ref_event_time)
    ) +
    theme_eug()
}

linear_combo_event <- function(b, V, coef_names, weights_vec) {
  present <- coef_names %in% names(b)
  if (!any(present)) return(list(est = 0, se = 0))

  nms <- coef_names[present]
  g   <- weights_vec[present]
  beta_vec <- b[nms]

  Vsub <- V[nms, nms, drop = FALSE]
  est <- sum(g * beta_vec)
  var <- as.numeric(t(g) %*% Vsub %*% g)
  se  <- sqrt(max(var, 0))

  list(est = est, se = se)
}

extract_combined_paths <- function(model, df_used, meta, event_levels, ref_val) {
  V <- stats::vcov(model)
  b <- stats::coef(model)

  mover_delta <- df_used %>%
    filter(!is.na(move_year)) %>%
    distinct(user_id, delta_div, delta_log_size, delta_imm_share) %>%
    filter(is.finite(delta_div), is.finite(delta_log_size), is.finite(delta_imm_share))

  if (nrow(mover_delta) == 0) return(NULL)

  q_div <- as.numeric(stats::quantile(mover_delta$delta_div, probs = c(0.25, 0.50, 0.75), na.rm = TRUE))
  q_lab <- c("p25", "p50", "p75")

  med_size <- stats::median(mover_delta$delta_log_size, na.rm = TRUE)
  med_imm  <- stats::median(mover_delta$delta_imm_share, na.rm = TRUE)

  all_k <- sort(c(ref_val, event_levels))
  out <- list()
  idx <- 0L

  for (j in seq_along(q_div)) {
    qd <- q_div[j]
    ql <- q_lab[j]

    for (k in all_k) {
      if (k == ref_val) {
        est <- 0
        se  <- 0
      } else {
        sk <- et_safe(k)

        coef_names <- c(
          paste0("evt_", sk),
          paste0("evt_", sk, "_delta_div"),
          paste0("evt_", sk, "_delta_size"),
          paste0("evt_", sk, "_delta_imm")
        )

        weights_vec <- c(1, qd, med_size, med_imm)
        lc <- linear_combo_event(b, V, coef_names, weights_vec)
        est <- lc$est
        se  <- lc$se
      }

      idx <- idx + 1L
      out[[idx]] <- tibble(
        event_time = k,
        delta_quantile = ql,
        delta_value = qd,
        size_gap_fixed = med_size,
        imm_gap_fixed = med_imm,
        estimate = est,
        std.error = se,
        ci_low = est - 1.96 * se,
        ci_high = est + 1.96 * se,
        cluster_def = meta$cluster_def,
        diversity_source = meta$diversity_source,
        diversity_metric = meta$diversity_metric,
        sample = meta$sample,
        weight = meta$weight,
        ref_event_time = ref_val
      )
    }
  }

  bind_rows(out)
}

make_combined_plot <- function(path_df, meta) {
  med_size <- unique(round(path_df$size_gap_fixed, 3))
  med_imm  <- unique(round(path_df$imm_gap_fixed, 3))

  ggplot(path_df, aes(x = event_time, y = estimate,
                      group = delta_quantile, color = delta_quantile, fill = delta_quantile)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = -0.5, linetype = "dotted") +
    geom_ribbon(aes(ymin = ci_low, ymax = ci_high), alpha = 0.12, linewidth = 0) +
    geom_line(linewidth = 1) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = sort(unique(path_df$event_time))) +
    labs(
      title = paste0(meta$cluster_def, " | ", meta$diversity_source, " | ",
                     toupper(meta$diversity_metric), " | ", meta$sample, " | ", meta$weight),
      subtitle = "Combined diversity path: beta_k + q_div × theta_div,k + med(size gap) × theta_size,k + med(imm gap) × theta_imm,k",
      x = "Event time (move year = 0)",
      y = "Estimated move effect for chosen diversity gap",
      color = "Delta diversity",
      fill = "Delta diversity",
      caption = paste0(
        "Curves shown at p25 / p50 / p75 of destination-origin diversity gap; ",
        "holding delta log size at median = ", med_size,
        " and delta immigrant share at median = ", med_imm,
        "; ref = ", meta$ref_event_time
      )
    ) +
    theme_eug()
}

# =========================
# Main regression runner
# =========================
run_one_event_reg <- function(df, meta, weight_var = NULL) {
  design <- add_event_dummies(df)
  x <- design$data

  rhs_vars <- c(
    paste0("evt_", sapply(design$event_levels, et_safe)),
    paste0("evt_", sapply(design$event_levels, et_safe), "_delta_div"),
    paste0("evt_", sapply(design$event_levels, et_safe), "_delta_size"),
    paste0("evt_", sapply(design$event_levels, et_safe), "_delta_imm"),
    "log_cluster_size_current",
    "immig_share_current",
    "tenure",
    "tenure_sq"
  )

  form <- as.formula(
    paste0("n_patents ~ ", paste(rhs_vars, collapse = " + "), " | user_id + year_fe")
  )

  est <- run_fepois_safe(form, x, weight_var = weight_var)

  meta$ref_event_time <- design$ref_val
  meta$n_obs   <- nrow(x)
  meta$n_users <- dplyr::n_distinct(x$user_id)
  meta$n_movers <- dplyr::n_distinct(x$user_id[!is.na(x$move_year)])

  coef_df <- extract_event_coefs(est, meta)
  if (is.null(coef_df)) return(NULL)

  out_stub <- paste(
    meta$cluster_def, meta$diversity_source, meta$diversity_metric,
    meta$sample, meta$weight, SUFFIX, sep = "__"
  )

  write_csv(coef_df, file.path(DIRS$coeffs, paste0("coefficients__", out_stub, ".csv")))

  # plots: main + three interaction components
  for (comp in c("Mover_main", "Delta_div", "Delta_size", "Delta_imm")) {
    g <- make_event_plot(coef_df, meta, comp)
    if (!is.null(g)) {
      fname <- switch(
        comp,
        "Mover_main" = "plot_main__",
        "Delta_div"  = "plot_delta_div__",
        "Delta_size" = "plot_delta_size__",
        "Delta_imm"  = "plot_delta_imm__"
      )
      ggsave(
        filename = file.path(DIRS$plots, paste0(fname, out_stub, ".png")),
        plot = g, width = 10, height = 7, dpi = 300
      )
    }
  }

  # combined diversity path holding size/imm gaps fixed at medians
  combined_df <- extract_combined_paths(
    model = est,
    df_used = x,
    meta = meta,
    event_levels = design$event_levels,
    ref_val = design$ref_val
  )

  if (!is.null(combined_df)) {
    write_csv(
      combined_df,
      file.path(DIRS$coeffs, paste0("combined_paths__", out_stub, ".csv"))
    )

    g3 <- make_combined_plot(combined_df, meta)
    ggsave(
      filename = file.path(DIRS$plots, paste0("plot_combined__", out_stub, ".png")),
      plot = g3, width = 11, height = 8, dpi = 300
    )
  }

  coef_df
}

# =========================
# Load data
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT_DIR, format = "parquet")
ds_names <- names(ds)

METRO_VAR <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro variable")
ts_msg("Using metro variable:", METRO_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents", "first_country", "last_country",
  "first_university_country", "first_startdate_edu", "first_startdate_pos",
  IMMIG_VAR, PARENT_VAR, METRO_VAR
))

# first_pos_country from full panel before year filtering
ts_msg("Computing first_pos_country from full panel...")
ds_pos <- ds %>%
  select(all_of(c("user_id", "year", "first_country", "last_country"))) %>%
  mutate(
    pos_country_y = if_else(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = if_else(pos_country_y == "", NA_character_, pos_country_y)
  ) %>%
  filter(!is.na(user_id), !is.na(year), !is.na(pos_country_y))

ds_first_year <- ds_pos %>%
  group_by(user_id) %>%
  summarise(first_pos_year = min(year, na.rm = TRUE), .groups = "drop")

first_pos_tbl <- ds_pos %>%
  inner_join(ds_first_year, by = "user_id") %>%
  filter(year == first_pos_year) %>%
  select(user_id, first_pos_country = pos_country_y) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    first_pos_country = str_trim(as.character(first_pos_country))
  ) %>%
  distinct(user_id, .keep_all = TRUE)

ts_msg("Collecting full panel...")
df_full <- ds %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(all_of(c(
    "user_id", "year", "n_patents", "first_country",
    "first_university_country", "first_startdate_edu", "first_startdate_pos",
    IMMIG_VAR, PARENT_VAR, METRO_VAR
  ))) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    n_patents = ifelse(is.na(n_patents), 0, n_patents),
    first_country = str_trim(as.character(first_country)),
    first_university_country = str_trim(as.character(first_university_country)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!METRO_VAR := str_trim(as.character(.data[[METRO_VAR]]))
  ) %>%
  left_join(first_pos_tbl, by = "user_id") %>%
  compute_tenure() %>%
  make_origin_country() %>%
  make_cluster_keys(METRO_VAR) %>%
  mutate(year_fe = factor(year))

# =========================
# Build full-sample objects
# =========================
cluster_defs <- list(
  PARENT = "cluster_parent",
  METRO_PARENT = "cluster_metro_parent"
)

div_sources <- list(
  origin = "origin_country",
  job    = "first_pos_country",
  edu    = "first_university_country"
)

div_metrics <- c("hhi", "entropy")

div_store     <- list()
control_store <- list()

for (clab in names(cluster_defs)) {
  cvar <- cluster_defs[[clab]]

  ts_msg("Building cluster-year controls for:", clab)
  control_store[[clab]] <- build_cluster_controls_table(df_full, cvar)

  for (src in names(div_sources)) {
    svar <- div_sources[[src]]
    for (met in div_metrics) {
      key <- paste(clab, src, met, sep = "__")
      ts_msg("Building diversity:", key)
      div_store[[key]] <- build_diversity_tables(df_full, cvar, svar, met)
    }
  }
}

# =========================
# Run event studies
# =========================
all_results <- list()
idx <- 0L

for (clab in names(cluster_defs)) {
  cvar <- cluster_defs[[clab]]
  ts_msg("Tagging first move for:", clab)

  base_dt <- tag_first_move(df_full, cvar) %>%
    mutate(cluster_join_id = as.character(.data[[cvar]])) %>%
    left_join(
      control_store[[clab]]$overall %>% rename(cluster_join_id = cluster_id),
      by = c("cluster_join_id", "year")
    ) %>%
    select(-cluster_join_id) %>%
    mutate(
      cluster_size_current = ifelse(is.na(cluster_size_current) | cluster_size_current <= 0, 1, cluster_size_current),
      log_cluster_size_current = ifelse(is.na(log_cluster_size_current), log(1), log_cluster_size_current),
      immig_share_current = ifelse(is.na(immig_share_current), 0, immig_share_current)
    )

  for (src in names(div_sources)) {
    for (met in div_metrics) {
      key <- paste(clab, src, met, sep = "__")
      ts_msg("Attaching mover deltas for:", key)

      dt_div <- attach_move_deltas(base_dt, div_store[[key]], control_store[[clab]], cvar) %>%
        as_tibble()

      # US filter RIGHT BEFORE regression
      reg_us <- dt_div %>%
        filter(!is.na(first_country), first_country == US_COUNTRY) %>%
        prep_event_time_for_reg() %>%
        as_tibble()

      for (sample_tag in c("ALL", "NATIVE", "IMMIGRANT")) {
        reg_s <- apply_sample_split(reg_us, sample_tag) %>%
          filter(
            is.finite(log_cluster_size_current),
            is.finite(immig_share_current),
            is.finite(tenure),
            is.finite(tenure_sq)
          ) %>%
          filter(
            is.na(move_year) |
              (is.finite(delta_div) & is.finite(delta_log_size) & is.finite(delta_imm_share))
          )

        if (nrow(reg_s) == 0) next
        if (dplyr::n_distinct(reg_s$user_id) < 50) next
        if (sum(!is.na(reg_s$move_year)) == 0) next

        for (wtag in c("unweighted", "weighted")) {
          ts_msg("Running:", clab, "|", src, "|", met, "|", sample_tag, "|", wtag)

          meta <- list(
            cluster_def = clab,
            diversity_source = src,
            diversity_metric = met,
            sample = sample_tag,
            weight = wtag
          )

          weight_var <- if (wtag == "weighted") "cluster_size_current" else NULL

          res <- tryCatch(
            run_one_event_reg(reg_s, meta, weight_var = weight_var),
            error = function(e) {
              ts_msg("FAILED:", clab, "|", src, "|", met, "|", sample_tag, "|", wtag, "|", e$message)
              NULL
            }
          )

          if (!is.null(res)) {
            idx <- idx + 1L
            all_results[[idx]] <- res
          }
        }
      }
    }
  }
}

if (length(all_results) > 0) {
  final_tab <- bind_rows(all_results)
  write_csv(final_tab, file.path(OUT_DIR, paste0("ALL_EVENTSTUDY_COEFFICIENTS__", SUFFIX, ".csv")))
}

ts_msg("DONE. Outputs in:", OUT_DIR)
