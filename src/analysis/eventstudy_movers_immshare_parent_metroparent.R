#!/usr/bin/env Rscript
###############################################################################
# EVENT STUDY: MOVES TO HIGHER-IMMIGRANT-SHARE CLUSTERS
#
# Design:
#   - Event = inventor's first move across chosen cluster definition
#   - Treatment intensity of interest =
#         destination immigrant share(t-1) - origin immigrant share(t-1)
#   - Immigrant share computed on FULL SAMPLE (no US filter)
#   - US filter applied RIGHT BEFORE regression
#   - Samples: ALL / NATIVE / IMMIGRANT
#   - Cluster definitions: PARENT / METRO_PARENT
#   - Weighting: unweighted / weighted by current cluster-year size
#
# Two versions:
#   (1) NO_HORSE_RACE:
#       n_patents ~ event dummies + event dummies x delta immigrant share
#                   + log_cluster_size_current + tenure + tenure_sq
#                   | user_id + year
#
#   (2) HORSE_RACE:
#       n_patents ~ event dummies + event dummies x delta immigrant share
#                   + event dummies x delta_log_size
#                   + log_cluster_size_current + tenure + tenure_sq
#                   | user_id + year
#
# Output:
#   coefficient CSVs + plots
#
# Notes:
#   - Origin immigrant share at t-1 is inventor-specific LOO
#   - Destination immigrant share at t-1 is overall cluster-year value
#   - Current cluster size is built on FULL SAMPLE and used for:
#       (i) control: log_cluster_size_current
#       (ii) weights in weighted specs
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
  "stringr", "readr", "tibble", "scales"
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
  library(tibble)
  library(scales)
})

data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))
set.seed(123)

# =========================
# Config
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_movers_immshare_parent_metroparent"
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
  if (length(miss) > 0) {
    stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
  }
}

et_safe <- function(k) ifelse(k < 0, paste0("m", abs(k)), as.character(k))

parse_event_time_from_term <- function(term) {
  z <- sub("^evt_", "", term)
  z <- sub("_delta_imm$", "", z)
  z <- sub("_delta_size$", "", z)
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
# FULL-SAMPLE size tables
# =========================
build_cluster_size_tables <- function(df, cluster_var) {
  dt <- as.data.table(df[, c("user_id", "year", cluster_var), drop = FALSE])
  setnames(dt, cluster_var, "cluster_id")

  dt[, cluster_id := as.character(cluster_id)]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]

  dt <- unique(
    dt[!is.na(user_id) & !is.na(year) & !is.na(cluster_id),
       .(user_id, year, cluster_id)]
  )

  if (nrow(dt) == 0) {
    return(list(
      overall = data.table(
        cluster_id = character(),
        year = integer(),
        cluster_size_current = integer(),
        log_cluster_size_current = numeric()
      ),
      loo = data.table(
        user_id = character(),
        cluster_id = character(),
        year = integer(),
        loo_log_cluster_size_value = numeric()
      )
    ))
  }

  agg <- dt[, .(
    cluster_size_current = .N
  ), by = .(cluster_id, year)]

  agg[, log_cluster_size_current := log(pmax(cluster_size_current, 1))]

  overall <- agg[, .(
    cluster_id, year, cluster_size_current, log_cluster_size_current
  )]

  loo <- merge(dt, agg, by = c("cluster_id", "year"), all.x = TRUE)
  loo[, loo_log_cluster_size_value := fifelse(
    cluster_size_current <= 1,
    NA_real_,
    log(cluster_size_current - 1)
  )]

  loo <- loo[, .(user_id, cluster_id, year, loo_log_cluster_size_value)]

  list(overall = overall, loo = loo)
}

# =========================
# FULL-SAMPLE immigrant-share tables
# immigrant share = mean(IMMIG_VAR)
# =========================
build_immigrant_share_tables <- function(df, cluster_var) {
  dt <- as.data.table(df[, c("user_id", "year", cluster_var, IMMIG_VAR), drop = FALSE])
  setnames(dt, cluster_var, "cluster_id")

  dt[, cluster_id := as.character(cluster_id)]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]

  dt <- unique(
    dt[!is.na(user_id) & !is.na(year) & !is.na(cluster_id) & !is.na(get(IMMIG_VAR)),
       .(user_id, year, cluster_id, imm = as.integer(get(IMMIG_VAR)))]
  )

  if (nrow(dt) == 0) {
    return(list(
      overall = data.table(cluster_id = character(), year = integer(), imm_share_value = numeric()),
      loo = data.table(user_id = character(), cluster_id = character(), year = integer(), loo_imm_share_value = numeric())
    ))
  }

  agg <- dt[, .(
    n_imm_obs = .N,
    imm_sum   = sum(imm, na.rm = TRUE)
  ), by = .(cluster_id, year)]

  agg[, imm_share_value := imm_sum / n_imm_obs]
  overall <- agg[, .(cluster_id, year, imm_share_value)]

  loo <- merge(dt, agg, by = c("cluster_id", "year"), all.x = TRUE)
  loo[, loo_imm_share_value := fifelse(
    n_imm_obs <= 1,
    NA_real_,
    (imm_sum - imm) / (n_imm_obs - 1)
  )]
  loo <- loo[, .(user_id, cluster_id, year, loo_imm_share_value)]

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
attach_move_deltas <- function(dt, imm_tables, size_tables, cluster_var) {
  x <- copy(as.data.table(dt))

  movers <- unique(x[!is.na(move_year), .(user_id, move_year, origin_cluster, dest_cluster)])
  if (nrow(movers) == 0) {
    x[, `:=`(
      delta_imm_share = 0,
      delta_log_size  = 0
    )]
    return(x[])
  }

  # Origin values at t-1: LOO because mover is still in origin cluster
  origin_q <- movers[, .(
    user_id,
    year = move_year - 1L,
    cluster_id = origin_cluster
  )]

  origin_imm <- merge(origin_q, imm_tables$loo, by = c("user_id", "year", "cluster_id"), all.x = TRUE)
  setnames(origin_imm, "loo_imm_share_value", "origin_imm_pre")

  origin_size <- merge(origin_q, size_tables$loo, by = c("user_id", "year", "cluster_id"), all.x = TRUE)
  setnames(origin_size, "loo_log_cluster_size_value", "origin_log_size_pre")

  # Destination values at t-1: overall because mover has not arrived yet
  dest_q <- movers[, .(
    user_id,
    year = move_year - 1L,
    cluster_id = dest_cluster
  )]

  dest_imm <- merge(dest_q, imm_tables$overall, by = c("year", "cluster_id"), all.x = TRUE)
  setnames(dest_imm, "imm_share_value", "dest_imm_pre")

  dest_size <- merge(dest_q, size_tables$overall, by = c("year", "cluster_id"), all.x = TRUE)
  setnames(dest_size, "log_cluster_size_current", "dest_log_size_pre")

  mv <- merge(
    origin_imm[, .(user_id, origin_imm_pre)],
    dest_imm[, .(user_id, dest_imm_pre)],
    by = "user_id", all = TRUE
  )

  mv <- merge(
    mv,
    origin_size[, .(user_id, origin_log_size_pre)],
    by = "user_id", all = TRUE
  )

  mv <- merge(
    mv,
    dest_size[, .(user_id, dest_log_size_pre)],
    by = "user_id", all = TRUE
  )

  mv[, delta_imm_share := dest_imm_pre - origin_imm_pre]
  mv[, delta_log_size  := dest_log_size_pre - origin_log_size_pre]

  x <- merge(
    x,
    mv[, .(user_id, delta_imm_share, delta_log_size)],
    by = "user_id",
    all.x = TRUE
  )

  x[is.na(move_year), `:=`(
    delta_imm_share = 0,
    delta_log_size  = 0
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

  # Keep movers with enough support + all never-movers
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

  ref_val <- ifelse(any(!is.na(x$event_time) & x$event_time == -1L), -1L, 0L)
  event_levels <- setdiff(seq(-T_MAX, T_MAX), ref_val)

  for (k in event_levels) {
    sk <- et_safe(k)
    v_main <- paste0("evt_", sk)
    v_imm  <- paste0("evt_", sk, "_delta_imm")
    v_size <- paste0("evt_", sk, "_delta_size")

    x[[v_main]] <- as.numeric(!is.na(x$event_time) & x$event_time == k)
    x[[v_imm]]  <- x[[v_main]] * x$delta_imm_share
    x[[v_size]] <- x[[v_main]] * x$delta_log_size
  }

  list(data = x, ref_val = ref_val, event_levels = event_levels)
}

# =========================
# Sample splits
# =========================
apply_sample_split <- function(df, sample_tag) {
  if (sample_tag == "ALL") {
    df %>% filter(!is.na(.data[[IMMIG_VAR]]))
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
        str_detect(term, "_delta_imm$")  ~ "Delta_imm_share",
        str_detect(term, "_delta_size$") ~ "Delta_size",
        str_detect(term, "^evt_")        ~ "Mover_main",
        TRUE ~ "Other"
      )
    ) %>%
    filter(component != "Other") %>%
    mutate(
      event_time = parse_event_time_from_term(term),
      stars = sig_stars(p.value),
      cluster_def = meta$cluster_def,
      outcome_measure = "immigrant_share",
      sample = meta$sample,
      spec = meta$spec,
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
    "Mover_main"      = "Average move path",
    "Delta_imm_share" = "Interaction with destination-origin immigrant-share gap",
    "Delta_size"      = "Interaction with destination-origin log-size gap"
  )

  ylab <- switch(
    component_keep,
    "Mover_main"      = "Beta on event-time dummy",
    "Delta_imm_share" = "Beta on event-time × delta immigrant share",
    "Delta_size"      = "Beta on event-time × delta log size"
  )

  ggplot(z, aes(x = event_time, y = estimate)) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = -0.5, linetype = "dotted") +
    geom_ribbon(aes(ymin = ci_low, ymax = ci_high), alpha = 0.15) +
    geom_line(linewidth = 0.9) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = sort(unique(z$event_time))) +
    labs(
      title = paste0(meta$cluster_def, " | IMM SHARE | ", meta$sample, " | ", meta$spec, " | ", meta$weight),
      subtitle = ttl,
      x = "Event time (move year = 0)",
      y = ylab,
      caption = paste0(
        "US-only estimation sample; immigrant share computed on full sample; ",
        "ref = ", meta$ref_event_time
      )
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
    distinct(user_id, delta_imm_share, delta_log_size) %>%
    filter(is.finite(delta_imm_share), is.finite(delta_log_size))

  if (nrow(mover_delta) == 0) return(NULL)

  q_imm <- as.numeric(stats::quantile(
    mover_delta$delta_imm_share,
    probs = c(0.25, 0.50, 0.75),
    na.rm = TRUE
  ))
  q_lab <- c("p25", "p50", "p75")

  med_size <- stats::median(mover_delta$delta_log_size, na.rm = TRUE)

  all_k <- sort(c(ref_val, event_levels))
  out <- list()
  idx <- 0L

  for (j in seq_along(q_imm)) {
    qi <- q_imm[j]
    ql <- q_lab[j]

    for (k in all_k) {
      if (k == ref_val) {
        est <- 0
        se  <- 0
      } else {
        sk <- et_safe(k)

        coef_names <- c(
          paste0("evt_", sk),
          paste0("evt_", sk, "_delta_imm"),
          paste0("evt_", sk, "_delta_size")
        )

        if (meta$spec == "NO_HORSE_RACE") {
          weights_vec <- c(1, qi, 0)
        } else {
          weights_vec <- c(1, qi, med_size)
        }

        lc <- linear_combo_event(b, V, coef_names, weights_vec)
        est <- lc$est
        se  <- lc$se
      }

      idx <- idx + 1L
      out[[idx]] <- tibble(
        event_time = k,
        imm_gap_quantile = ql,
        imm_gap_value = qi,
        size_gap_fixed = med_size,
        estimate = est,
        std.error = se,
        ci_low = est - 1.96 * se,
        ci_high = est + 1.96 * se,
        cluster_def = meta$cluster_def,
        sample = meta$sample,
        spec = meta$spec,
        weight = meta$weight,
        ref_event_time = ref_val
      )
    }
  }

  bind_rows(out)
}

make_combined_plot <- function(path_df, meta) {
  med_size <- unique(round(path_df$size_gap_fixed, 3))

  subtitle_txt <- if (meta$spec == "NO_HORSE_RACE") {
    "Combined path: beta_k + q_imm × theta_imm,k"
  } else {
    paste0(
      "Combined path: beta_k + q_imm × theta_imm,k + med(size gap) × theta_size,k",
      " | median size gap = ", med_size
    )
  }

  ggplot(path_df, aes(
    x = event_time, y = estimate,
    group = imm_gap_quantile, color = imm_gap_quantile, fill = imm_gap_quantile
  )) +
    geom_hline(yintercept = 0, linetype = "dashed") +
    geom_vline(xintercept = -0.5, linetype = "dotted") +
    geom_ribbon(aes(ymin = ci_low, ymax = ci_high), alpha = 0.12, linewidth = 0) +
    geom_line(linewidth = 1) +
    geom_point(size = 2) +
    scale_x_continuous(breaks = sort(unique(path_df$event_time))) +
    labs(
      title = paste0(meta$cluster_def, " | IMM SHARE | ", meta$sample, " | ", meta$spec, " | ", meta$weight),
      subtitle = subtitle_txt,
      x = "Event time (move year = 0)",
      y = "Estimated move effect for chosen immigrant-share gap",
      color = "Delta imm share",
      fill = "Delta imm share",
      caption = paste0(
        "Curves shown at p25 / p50 / p75 of destination-origin immigrant-share gap; ",
        "ref = ", meta$ref_event_time
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

  main_terms <- paste0("evt_", sapply(design$event_levels, et_safe))
  imm_terms  <- paste0(main_terms, "_delta_imm")
  size_terms <- paste0(main_terms, "_delta_size")

  rhs_vars <- c(
    main_terms,
    imm_terms,
    "log_cluster_size_current",
    "tenure",
    "tenure_sq"
  )

  if (meta$spec == "HORSE_RACE") {
    rhs_vars <- c(rhs_vars, size_terms)
  }

  form <- as.formula(
    paste0("n_patents ~ ", paste(rhs_vars, collapse = " + "), " | user_id + year_fe")
  )

  est <- run_fepois_safe(form, x, weight_var = weight_var)

  meta$ref_event_time <- design$ref_val
  meta$n_obs    <- nrow(x)
  meta$n_users  <- dplyr::n_distinct(x$user_id)
  meta$n_movers <- dplyr::n_distinct(x$user_id[!is.na(x$move_year)])

  coef_df <- extract_event_coefs(est, meta)
  if (is.null(coef_df)) return(NULL)

  out_stub <- paste(
    meta$cluster_def,
    "immshare",
    meta$sample,
    meta$spec,
    meta$weight,
    SUFFIX,
    sep = "__"
  )

  write_csv(coef_df, file.path(DIRS$coeffs, paste0("coefficients__", out_stub, ".csv")))

  g1 <- make_event_plot(coef_df, meta, "Mover_main")
  if (!is.null(g1)) {
    ggsave(
      filename = file.path(DIRS$plots, paste0("plot_main__", out_stub, ".png")),
      plot = g1, width = 10, height = 7, dpi = 300
    )
  }

  g2 <- make_event_plot(coef_df, meta, "Delta_imm_share")
  if (!is.null(g2)) {
    ggsave(
      filename = file.path(DIRS$plots, paste0("plot_delta_imm__", out_stub, ".png")),
      plot = g2, width = 10, height = 7, dpi = 300
    )
  }

  if (meta$spec == "HORSE_RACE") {
    g3 <- make_event_plot(coef_df, meta, "Delta_size")
    if (!is.null(g3)) {
      ggsave(
        filename = file.path(DIRS$plots, paste0("plot_delta_size__", out_stub, ".png")),
        plot = g3, width = 10, height = 7, dpi = 300
      )
    }
  }

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

    g4 <- make_combined_plot(combined_df, meta)
    ggsave(
      filename = file.path(DIRS$plots, paste0("plot_combined__", out_stub, ".png")),
      plot = g4, width = 11, height = 8, dpi = 300
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
  "user_id", "year", "n_patents", "first_country",
  "first_startdate_edu", "first_startdate_pos",
  IMMIG_VAR, PARENT_VAR, METRO_VAR
))

ts_msg("Collecting full panel...")
df_full <- ds %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(all_of(c(
    "user_id", "year", "n_patents", "first_country",
    "first_startdate_edu", "first_startdate_pos",
    IMMIG_VAR, PARENT_VAR, METRO_VAR
  ))) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    n_patents = ifelse(is.na(n_patents), 0, n_patents),
    first_country = str_trim(as.character(first_country)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!METRO_VAR  := str_trim(as.character(.data[[METRO_VAR]]))
  ) %>%
  compute_tenure() %>%
  make_cluster_keys(METRO_VAR) %>%
  mutate(year_fe = factor(year))

# =========================
# Build full-sample objects
# =========================
cluster_defs <- list(
  PARENT = "cluster_parent",
  METRO_PARENT = "cluster_metro_parent"
)

size_store <- list()
imm_store  <- list()

for (clab in names(cluster_defs)) {
  cvar <- cluster_defs[[clab]]

  ts_msg("Building cluster-year size tables for:", clab)
  size_store[[clab]] <- build_cluster_size_tables(df_full, cvar)

  ts_msg("Building cluster-year immigrant-share tables for:", clab)
  imm_store[[clab]] <- build_immigrant_share_tables(df_full, cvar)
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
      size_store[[clab]]$overall %>% rename(cluster_join_id = cluster_id),
      by = c("cluster_join_id", "year")
    ) %>%
    select(-cluster_join_id) %>%
    mutate(
      cluster_size_current = ifelse(is.na(cluster_size_current) | cluster_size_current <= 0, 1, cluster_size_current),
      log_cluster_size_current = ifelse(is.na(log_cluster_size_current), log(1), log_cluster_size_current)
    )

  ts_msg("Attaching mover-specific immigrant-share and size deltas for:", clab)
  dt_imm <- attach_move_deltas(base_dt, imm_store[[clab]], size_store[[clab]], cvar) %>%
    as_tibble()

  # US filter RIGHT BEFORE regression
  reg_us <- dt_imm %>%
    filter(!is.na(first_country), first_country == US_COUNTRY) %>%
    prep_event_time_for_reg() %>%
    as_tibble()

  for (sample_tag in c("ALL", "NATIVE", "IMMIGRANT")) {
    reg_s <- apply_sample_split(reg_us, sample_tag) %>%
      filter(
        is.finite(log_cluster_size_current),
        is.finite(tenure),
        is.finite(tenure_sq)
      ) %>%
      filter(
        is.na(move_year) |
          (is.finite(delta_imm_share) & is.finite(delta_log_size))
      )

    if (nrow(reg_s) == 0) next
    if (dplyr::n_distinct(reg_s$user_id) < 50) next
    if (sum(!is.na(reg_s$move_year)) == 0) next

    for (spec_tag in c("NO_HORSE_RACE", "HORSE_RACE")) {
      for (wtag in c("unweighted", "weighted")) {
        ts_msg("Running:", clab, "|", sample_tag, "|", spec_tag, "|", wtag)

        meta <- list(
          cluster_def = clab,
          sample = sample_tag,
          spec = spec_tag,
          weight = wtag
        )

        weight_var <- if (wtag == "weighted") "cluster_size_current" else NULL

        res <- tryCatch(
          run_one_event_reg(reg_s, meta, weight_var = weight_var),
          error = function(e) {
            ts_msg("FAILED:", clab, "|", sample_tag, "|", spec_tag, "|", wtag, "|", e$message)
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

if (length(all_results) > 0) {
  final_tab <- bind_rows(all_results)
  write_csv(final_tab, file.path(OUT_DIR, paste0("ALL_EVENTSTUDY_COEFFICIENTS__immshare__", SUFFIX, ".csv")))
}

ts_msg("DONE. Outputs in:", OUT_DIR)
