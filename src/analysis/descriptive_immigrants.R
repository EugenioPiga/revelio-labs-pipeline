#!/usr/bin/env Rscript
###############################################################################
# Descriptives — Weighted scatter + slope tests (Immigrants vs Natives)
#
# Requests addressed:
#  (i) Are fit lines weighted by # inventors per point? YES (weights below).
# (ii) Report standard errors & test slope differences? YES (robust HC1 SE).
#(iii) Robust across immigrant definitions and years? YES (loops).
#
# Outputs:
#   /home/epiga/revelio_labs/output/descriptives_immigrants/
#     plots/
#     regressions/
#     cluster_summaries/
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow", "dplyr", "tidyr", "readr", "ggplot2",
          "broom", "sandwich", "lmtest")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}

library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(ggplot2)
library(broom)
library(sandwich)
library(lmtest)

# ============================
# Paths + knobs
# ============================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants"

PLOTS_DIR <- file.path(OUT_DIR, "plots")
REG_DIR   <- file.path(OUT_DIR, "regressions")
SUM_DIR   <- file.path(OUT_DIR, "cluster_summaries")

dir.create(PLOTS_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(REG_DIR,   recursive = TRUE, showWarnings = FALSE)
dir.create(SUM_DIR,   recursive = TRUE, showWarnings = FALSE)

MIN_CLUSTER <- 50   # min total inventors per cluster-year to keep points stable
USE_LAST_N_YEARS <- 15  # robustness: run last N years present in data

geos <- list(
  state = "first_state",
  metro = "first_metro_area"
)

immig_defs <- c(
  "immig_job_first_nonUS",
  "immig_deg_first_nonUS",
  "immig_first_deg_or_job_nonUS"
)

# ============================
# Helpers: robust SE (HC1) + delta method for slope sums
# ============================
tidy_robust <- function(model) {
  V <- sandwich::vcovHC(model, type = "HC1")
  ct <- lmtest::coeftest(model, vcov. = V)
  tibble(
    term      = rownames(ct),
    estimate  = ct[,1],
    std_error = ct[,2],
    statistic = ct[,3],
    p_value   = ct[,4]
  )
}

# slope for immigrant = beta_log + beta_interaction
delta_se_sum <- function(V, idx_a, idx_b) {
  # SE(a + b) = sqrt(Var(a)+Var(b)+2Cov(a,b))
  sqrt(V[idx_a, idx_a] + V[idx_b, idx_b] + 2 * V[idx_a, idx_b])
}

# ============================
# Build cluster summary for a given (year, geo, immig_def)
#  - cluster_size = # unique inventors in cluster-year
#  - n_immig / n_nonimmig = # unique inventors by group
#  - avg_patents by group = mean n_patents (inventor-year) in that group
# ============================
build_cluster_summary <- function(ds, year_val, geo_var, immig_var, min_cluster = 50) {

  base <- ds %>%
    select(user_id, year, n_patents, first_country,
           all_of(geo_var), all_of(immig_var)) %>%
    filter(first_country == "United States") %>%
    filter(year == year_val) %>%
    filter(!is.na(.data[[geo_var]])) %>%
    mutate(immig_flag = as.integer(.data[[immig_var]])) %>%
    filter(!is.na(immig_flag), immig_flag %in% c(0L, 1L)) %>%
    collect()

  if (nrow(base) == 0) return(tibble())

  base <- base %>%
    mutate(
      group = ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
      group = factor(group, levels = c("nonimmigrant", "immigrant"))
    )

  # total cluster size (# unique inventors in cluster-year)
  by_total <- base %>%
    group_by(across(all_of(geo_var))) %>%
    summarise(cluster_size = n_distinct(user_id), .groups = "drop")

  # counts by group (unique inventors)
  by_counts <- base %>%
    group_by(across(all_of(geo_var)), group) %>%
    summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from = group,
      values_from = n_inventors,
      values_fill = 0,
      names_prefix = "n_"
    )

  # average patents by group (mean patents per inventor-year in that cluster-year)
  by_avgs <- base %>%
    group_by(across(all_of(geo_var)), group) %>%
    summarise(avg_patents = mean(n_patents, na.rm = TRUE), .groups = "drop") %>%
    tidyr::pivot_wider(
      names_from = group,
      values_from = avg_patents,
      names_prefix = "avg_"
    )

  out <- by_total %>%
    left_join(by_counts, by = geo_var) %>%
    left_join(by_avgs,   by = geo_var)

  # Ensure all expected columns exist (some years/defs may have zero immigrants everywhere)
  if (!("n_immigrant" %in% names(out)))     out$n_immigrant <- 0
  if (!("n_nonimmigrant" %in% names(out)))  out$n_nonimmigrant <- 0
  if (!("avg_immigrant" %in% names(out)))   out$avg_immigrant <- NA_real_
  if (!("avg_nonimmigrant" %in% names(out))) out$avg_nonimmigrant <- NA_real_

  out <- out %>%
    mutate(
      immig_share = ifelse(cluster_size > 0, n_immigrant / cluster_size, NA_real_),
      log_cluster = log(cluster_size)
    ) %>%
    filter(cluster_size >= min_cluster)

  out
}


# ============================
# Regression + plots per combination
# ============================
run_one <- function(summ, year_val, geo_name, geo_var, immig_def) {
  if (nrow(summ) == 0) return(invisible(NULL))

  tag <- paste0("y", year_val, "__", geo_name, "__", immig_def)

  # ----------------------------
  # (A) Immigrant share vs cluster size
  #     Weighted by cluster_size
  #     Save linear + quadratic (to capture U-shape)
  # ----------------------------
  m_share_lin  <- lm(immig_share ~ log_cluster, data = summ, weights = cluster_size)
  m_share_quad <- lm(immig_share ~ log_cluster + I(log_cluster^2), data = summ, weights = cluster_size)

  share_lin_tidy  <- tidy_robust(m_share_lin)  %>% mutate(spec = "share_linear")
  share_quad_tidy <- tidy_robust(m_share_quad) %>% mutate(spec = "share_quadratic")

  write_csv(bind_rows(share_lin_tidy, share_quad_tidy),
            file.path(REG_DIR, paste0("reg_share__", tag, ".csv")))

  p_share <- ggplot(summ, aes(x = log_cluster, y = immig_share)) +
    geom_point(alpha = 0.35) +
    geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
    labs(
      title = paste0("Immigrant share vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      x = "log(cluster size)  [cluster size = # inventors]",
      y = "Immigrant share"
    ) +
    theme_minimal()

  ggsave(file.path(PLOTS_DIR, paste0("scatter_share__", tag, ".png")),
         p_share, width = 8, height = 5, dpi = 200)

  # ----------------------------
  # (B) Avg patents vs cluster size (immigrant vs nonimmigrant)
  #     Weighted by group size within cluster (n_*)
  #     Regression: avg_patents ~ log_cluster * group
  #     Robust HC1 SE + slope comparison
  # ----------------------------
  pat_long <- bind_rows(
    summ %>%
      transmute(
        geo_level   = .data[[geo_var]],
        cluster_size,
        log_cluster,
        group = "nonimmigrant",
        group_n = n_nonimmigrant,
        avg_patents = avg_nonimmigrant
      ),
    summ %>%
      transmute(
        geo_level   = .data[[geo_var]],
        cluster_size,
        log_cluster,
        group = "immigrant",
        group_n = n_immigrant,
        avg_patents = avg_immigrant
      )
  ) %>%
    filter(!is.na(avg_patents), group_n > 0) %>%
    mutate(group = factor(group, levels = c("nonimmigrant", "immigrant")))

  # If one group basically doesn’t exist, skip gracefully
  if (n_distinct(pat_long$group) < 2) {
    cat("[WARN] Only one group present in pat_long; skipping patents slope comparison for:", tag, "\n")
  } else {
    m_pat <- lm(avg_patents ~ log_cluster * group, data = pat_long, weights = group_n)

    V <- sandwich::vcovHC(m_pat, type = "HC1")
    ct <- lmtest::coeftest(m_pat, vcov. = V)

    coef_names <- rownames(ct)
    b <- as.numeric(ct[,1]); names(b) <- coef_names

    term_log <- "log_cluster"
    term_int <- "log_cluster:groupimmigrant"

    # In case R uses a different interaction naming, find it robustly
    if (!(term_int %in% names(b))) {
      term_int <- grep("^log_cluster:group", names(b), value = TRUE)[1]
    }

    slope_native <- b[term_log]
    se_native    <- sqrt(V[term_log, term_log])

    slope_immig  <- b[term_log] + b[term_int]
    se_immig     <- sqrt(V[term_log, term_log] + V[term_int, term_int] + 2*V[term_log, term_int])

    diff_slope <- b[term_int]
    se_diff   <- sqrt(V[term_int, term_int])
    p_diff    <- 2 * pnorm(abs(diff_slope / se_diff), lower.tail = FALSE)

    reg_pat_tidy <- tidy_robust(m_pat) %>% mutate(spec = "avg_patents_interaction")

    slope_summary <- tibble(
      term      = c("slope_nonimmigrant", "slope_immigrant", "slope_diff(immig-native)"),
      estimate  = c(slope_native, slope_immig, diff_slope),
      std_error = c(se_native, se_immig, se_diff),
      statistic = c(slope_native/se_native, slope_immig/se_immig, diff_slope/se_diff),
      p_value   = c(
        2*pnorm(abs(slope_native/se_native), lower.tail = FALSE),
        2*pnorm(abs(slope_immig/se_immig), lower.tail = FALSE),
        p_diff
      ),
      spec = "slope_summary"
    )

    write_csv(bind_rows(reg_pat_tidy, slope_summary),
              file.path(REG_DIR, paste0("reg_avg_patents__", tag, ".csv")))

    p_pat <- ggplot(pat_long, aes(x = log_cluster, y = avg_patents, color = group)) +
      geom_point(alpha = 0.40) +
      geom_smooth(method = "lm", aes(weight = group_n), se = FALSE) +
      labs(
        title = paste0("Avg patents vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
        x = "log(cluster size)  [cluster size = # inventors]",
        y = "Average patents per inventor-year",
        color = "Group"
      ) +
      theme_minimal()

    ggsave(file.path(PLOTS_DIR, paste0("scatter_avg_patents__", tag, ".png")),
           p_pat, width = 8, height = 5, dpi = 200)
  }

  # Save cluster summary for sanity checks
  summ_out <- summ %>%
    mutate(year = year_val, geo = geo_name, immig_def = immig_def)

  write_csv(summ_out, file.path(SUM_DIR, paste0("cluster_summary__", tag, ".csv")))

  invisible(NULL)
}

# ============================
# MAIN: determine years + loop
# ============================
cat("\n[INFO] Opening Arrow dataset...\n")
ds <- open_dataset(INPUT, format = "parquet")

# ============================
# Restrict to Top 10% inventors (compute once, then filter ds)
# ============================
cat("[INFO] Computing top 10% inventors...\n")

inv_tot <- ds %>%
  select(user_id, n_patents) %>%
  group_by(user_id) %>%
  summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop") %>%
  collect()

p90  <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)

cat("[INFO] Top10 inventors:", length(top10), "\n")

# Filter the Arrow dataset so everything downstream uses only top10 inventors
ds <- ds %>% filter(user_id %in% top10)
cat("[INFO] Applied top 10% inventor filter.\n\n")

cat("[INFO] Discovering available years...\n")
years_all <- ds %>%
  select(year) %>%
  distinct() %>%
  collect() %>%
  pull(year) %>%
  sort()

if (length(years_all) == 0) stop("[ERROR] No years found in dataset.")

years_to_run <- tail(years_all, USE_LAST_N_YEARS)
cat("[INFO] Running years:", paste(years_to_run, collapse = ", "), "\n")
cat("[INFO] MIN_CLUSTER =", MIN_CLUSTER, "\n")

for (immig_def in immig_defs) {
  for (geo_name in names(geos)) {

    geo_var <- geos[[geo_name]]

    for (yy in years_to_run) {
      cat("\n====================================================\n")
      cat("[INFO] Year:", yy, "| Geo:", geo_name, "| Immig def:", immig_def, "\n")
      cat("====================================================\n")

      summ <- build_cluster_summary(
        ds = ds,
        year_val = yy,
        geo_var = geo_var,
        immig_var = immig_def,
        min_cluster = MIN_CLUSTER
      )

      if (nrow(summ) < 10) {
        cat("[WARN] Too few clusters after filters. Skipping.\n")
        next
      }

      run_one(summ, yy, geo_name, geo_var, immig_def)
      cat("[INFO] Done:", "y", yy, geo_name, immig_def, "\n")
    }
  }
}

cat("\n[DONE] All weighted descriptives + regressions saved in:\n")
cat("       ", OUT_DIR, "\n\n")
