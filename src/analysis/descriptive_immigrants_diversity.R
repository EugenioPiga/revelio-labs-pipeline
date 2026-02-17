############################################################################
# DIVERSITY (ORIGIN + JOB + EDUCATION) 
# Computes ONLY:
#   - Firm-year and Metro-year diversity measures (1 - HHI):
#       (i) origin_country     (first_university, else first_pos_country)
#      (ii) first_pos_country  (earliest observed position country)
#     (iii) first_university_country (NO fallback)
#
# Regressions (inventor-year):
#   - fixest::feols (fast) with cluster-robust SE at the cluster level
#   - Pooled specs:
#       raw:           y ~ div
#       size:          y ~ div + log(size)
#       size_cfe:      y ~ div + log(size) | countryFE(var depends on div)
#       size_sh:       y ~ div + log(size) + immig_share
#       size_sh_cfe:   y ~ div + log(size) + immig_share | countryFE(...)
#       full:          y ~ div + log(size) + immig_share + tenure + tenure^2 | year
#
#   - Year-by-year specs (within year; no year FE):
#       raw, size, size_cfe, size_sh, size_sh_cfe, full (same RHS as above, no "| year")
#
# Output tables:
#   - For EACH regression run: coefficient + SE + t + p for:
#       div, log_size, immig_share, tenure, tenure_sq (when present)
#
# 2018 descriptives (for firm and metro; for each div measure):
#   - Distribution (hist) + summary stats
#   - Bin-scatter (15 equal-width bins on x):
#       y=div vs x=log_cluster
#       y=div vs x=immig_share
#     Points are binned weighted means of y (weights=cluster_size), NO lines.
#     Regression line is from UNBINNED weighted regression (weights=cluster_size).
#     Annotation reports unbinned slope and SE.
############################################################################

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","ggplot2","tidyr","scales")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(ggplot2); library(tidyr); library(scales)
})

set.seed(123)

# =========================
# Paths + knobs
# =========================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_origin_job_edu_firm_metro"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

TABLES_DIR <- file.path(OUT_DIR, "tables")
MODELS_DIR <- file.path(OUT_DIR, "models")
PLOTS_DIR  <- file.path(OUT_DIR, "plots_2018")
dir.create(TABLES_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(MODELS_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(PLOTS_DIR,  recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR <- "immig_first_deg_or_job_nonUS"
FIRM_VAR  <- "first_parent_rcid"
METRO_VAR <- "first_metro_area"

USE_LAST_N_YEARS <- 10
SKIP_YEARS <- c(2020,2021,2022,2023,2024,2025)

# years for individual regressions
years_vec <- 2016:2019

MIN_CLUSTER <- 10
TENURE_MAX <- 50

# 2018 bin-scatter settings
YEAR_PLOTS <- 2018
N_BINS_X   <- 15

# =========================
# Helpers
# =========================
need_cols <- function(ds, cols) {
  nm <- names(ds)
  missing <- setdiff(cols, nm)
  if (length(missing) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(missing, collapse=", ")))
}

compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    )
}

# Build first_pos_country from earliest observed year per user
build_first_pos_country_map <- function(ds) {
  pos_df <- ds %>%
    select(user_id, year, first_country, last_country) %>%
    mutate(pos_country_y = coalesce(last_country, first_country)) %>%
    filter(!is.na(pos_country_y), pos_country_y != "") %>%
    distinct(user_id, year, pos_country_y) %>%
    collect()

  pos_df %>%
    group_by(user_id) %>%
    slice_min(order_by = year, n = 1, with_ties = FALSE) %>%
    ungroup() %>%
    transmute(user_id, first_pos_country = pos_country_y)
}

# origin_country: natives -> US; immigrants -> non-US uni else non-US first_pos_country
make_origin_country <- function(df) {
  df %>%
    mutate(
      uni_c = str_trim(as.character(first_university_country)),
      pos_c = str_trim(as.character(first_pos_country)),
      uni_ok = !is.na(uni_c) & uni_c != "" & uni_c != "United States",
      pos_ok = !is.na(pos_c) & pos_c != "" & pos_c != "United States",
      origin_country = case_when(
        .data[[IMMIG_VAR]] == 0L ~ "United States",
        .data[[IMMIG_VAR]] == 1L & uni_ok ~ uni_c,
        .data[[IMMIG_VAR]] == 1L & !uni_ok & pos_ok ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# Compute 1 - HHI on unique inventors (data.table), per cluster-year on a given country var
cluster_year_div_stats_dt <- function(df, cluster_var, label, country_var,
                                      div_outname, min_cluster = MIN_CLUSTER) {

  keep <- c("user_id","year",IMMIG_VAR,cluster_var,country_var)
  dt <- as.data.table(df[, keep])

  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "cvar_raw")

  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]
  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year)]
  dt <- dt[!is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]

  # unique inventor composition
  comp <- unique(dt[, .(user_id, year, cluster_id, imm, cvar_raw)])

  size_df <- comp[, .(
    cluster_size = uniqueN(user_id),
    immig_share  = mean(imm, na.rm = TRUE)
  ), by = .(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]
  size_df[, log_cluster := log(cluster_size)]
  size_df[, cluster_level := label]

  # diversity computed on non-missing cvar_raw
  tmp <- comp[!is.na(cvar_raw) & cvar_raw != "" & cvar_raw != "empty",
              .(n = uniqueN(user_id)),
              by = .(cluster_id, year, v = cvar_raw)]
  tmp[, tot := sum(n), by = .(cluster_id, year)]
  tmp[, share_sq := (n / tot)^2]
  div_df <- tmp[, .(div = 1 - sum(share_sq)), by = .(cluster_id, year)]
  setnames(div_df, "div", div_outname)

  out <- merge(size_df, div_df, by = c("cluster_id","year"), all.x = TRUE)
  out <- out[is.finite(get(div_outname)) & is.finite(log_cluster) & is.finite(immig_share)]
  as_tibble(out)
}

# Extract coef table for selected terms (including p-value)
extract_terms_fixest <- function(fit, terms_wanted) {
  ct <- fixest::coeftable(fit)
  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(term = terms_wanted, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_))
  }
  rn <- rownames(ct)
  out <- lapply(terms_wanted, function(tt) {
    if (tt %in% rn) {
      tibble(
        term = tt,
        estimate = unname(ct[tt, 1]),
        se       = unname(ct[tt, 2]),
        t_stat   = unname(ct[tt, 3]),
        p_value  = unname(ct[tt, 4])
      )
    } else {
      tibble(term = tt, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_)
    }
  })
  bind_rows(out)
}

# Run pooled regressions with additional country-FE specs
run_div_regressions <- function(d, prefix, level_label, out_dir,
                                div_suffix, country_fe_var,
                                use_ppml = FALSE) {

  div   <- paste0(prefix, "_", div_suffix)
  logsz <- paste0(prefix, "_log_size")
  share <- paste0(prefix, "_immig_share")
  clid  <- paste0(prefix, "_cluster_id")

  # specs (country FE inserted after size and after size_sh; FULL has year + country FE)
  specs <- list(
    raw         = as.formula(paste0("n_patents ~ ", div)),
    size        = as.formula(paste0("n_patents ~ ", div, " + ", logsz)),
    size_cfe    = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " | ", country_fe_var)),
    size_sh     = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share)),
    size_sh_cfe = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share, " | ", country_fe_var)),
    full        = as.formula(paste0(
      "n_patents ~ ", div, " + ", logsz, " + ", share, " + tenure + tenure_sq | year + ", country_fe_var
    ))
  )

  terms_wanted <- unique(c(div, logsz, share, "tenure", "tenure_sq"))
  rows_all <- list()

  for (nm in names(specs)) {
    f <- specs[[nm]]

    needed <- unique(c(
       all.vars(f),
       clid,
       if (nm %in% c("size_cfe","size_sh_cfe","full")) country_fe_var else NULL,
       if (nm == "full") "year" else NULL
    ))

    dd <- d %>%
      select(all_of(needed)) %>%
      filter(!is.na(n_patents)) %>%
      filter(!is.na(.data[[div]]), is.finite(.data[[div]])) %>%
      filter(!is.na(.data[[clid]]), .data[[clid]] != "") %>%
      mutate(!!clid := as.factor(.data[[clid]]))

    if (logsz %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[logsz]]))
    if (share %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[share]]))
    if (any(c("tenure","tenure_sq") %in% all.vars(f))) dd <- dd %>% filter(is.finite(tenure), is.finite(tenure_sq))

    # FE specs: enforce non-missing country FE values
    has_country_fe <- nm %in% c("size_cfe", "size_sh_cfe", "full")
    if (has_country_fe) {
      dd <- dd %>% filter(!is.na(.data[[country_fe_var]]), .data[[country_fe_var]] != "")
    }

    # pooled only: FULL has year FE too, but year should never be missing; keep if you want:
    if (nm == "full") {
      dd <- dd %>% filter(!is.na(year))
    }

    if (nrow(dd) < 1000) stop(paste0("[ERROR] Too few rows for ", level_label, " / ", nm, ": ", nrow(dd)))

    if (use_ppml) {
      fit <- fixest::feglm(f, data = dd, family = "poisson",
                           vcov = as.formula(paste0("~", clid)), warn = FALSE)
    } else {
      fit <- fixest::feols(f, data = dd,
                           vcov = as.formula(paste0("~", clid)), warn = FALSE)
    }

    capture.output(summary(fit),
                   file = file.path(out_dir, paste0(level_label, "__", div_suffix, "__", nm, ".txt")))

    ct_df <- extract_terms_fixest(fit, terms_wanted) %>%
      mutate(cluster_level = level_label,
             diversity_measure = div_suffix,
             spec = nm,
             n_obs = nobs(fit)) %>%
      select(cluster_level, diversity_measure, spec, n_obs, term, estimate, se, t_stat, p_value)

    rows_all[[nm]] <- ct_df
  }

  out <- bind_rows(rows_all)
  write_csv(out, file.path(out_dir, paste0(level_label, "__", div_suffix, "__ALLTERMS.csv")))
  out
}


# Run the specs within a single year (no year FE); FULL still has country FE
run_div_regressions_one_year <- function(d, prefix, level_label, out_dir,
                                         div_suffix, country_fe_var,
                                         use_ppml = FALSE) {

  div   <- paste0(prefix, "_", div_suffix)
  logsz <- paste0(prefix, "_log_size")
  share <- paste0(prefix, "_immig_share")
  clid  <- paste0(prefix, "_cluster_id")

  specs <- list(
    raw         = as.formula(paste0("n_patents ~ ", div)),
    size        = as.formula(paste0("n_patents ~ ", div, " + ", logsz)),
    size_cfe    = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " | ", country_fe_var)),
    size_sh     = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share)),
    size_sh_cfe = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share, " | ", country_fe_var)),
    full        = as.formula(paste0(
      "n_patents ~ ", div, " + ", logsz, " + ", share, " + tenure + tenure_sq | ", country_fe_var
    ))
  )

  terms_wanted <- unique(c(div, logsz, share, "tenure", "tenure_sq"))
  rows_all <- list()

  for (nm in names(specs)) {
    f <- specs[[nm]]
    
    needed <- unique(c(
       all.vars(f),
       clid,
       if (nm %in% c("size_cfe","size_sh_cfe","full")) country_fe_var else NULL
    ))
    
    dd <- d %>%
      select(all_of(needed)) %>%
      filter(!is.na(n_patents)) %>%
      filter(!is.na(.data[[div]]), is.finite(.data[[div]])) %>%
      filter(!is.na(.data[[clid]]), .data[[clid]] != "") %>%
      mutate(!!clid := as.factor(.data[[clid]]))

    if (logsz %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[logsz]]))
    if (share %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[share]]))
    if (any(c("tenure","tenure_sq") %in% all.vars(f))) dd <- dd %>% filter(is.finite(tenure), is.finite(tenure_sq))

    has_country_fe <- nm %in% c("size_cfe", "size_sh_cfe", "full")
    if (has_country_fe) {
      dd <- dd %>% filter(!is.na(.data[[country_fe_var]]), .data[[country_fe_var]] != "")
    }

    if (nrow(dd) < 1000) {
      rows_all[[nm]] <- tibble(
        cluster_level = level_label,
        diversity_measure = div_suffix,
        spec = nm,
        n_obs = nrow(dd),
        term = terms_wanted,
        estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_
      )
      next
    }

    if (use_ppml) {
      fit <- fixest::feglm(f, data = dd, family = "poisson",
                           vcov = as.formula(paste0("~", clid)), warn = FALSE)
    } else {
      fit <- fixest::feols(f, data = dd,
                           vcov = as.formula(paste0("~", clid)), warn = FALSE)
    }

    ct_df <- extract_terms_fixest(fit, terms_wanted) %>%
      mutate(cluster_level = level_label,
             diversity_measure = div_suffix,
             spec = nm,
             n_obs = nobs(fit)) %>%
      select(cluster_level, diversity_measure, spec, n_obs, term, estimate, se, t_stat, p_value)

    rows_all[[nm]] <- ct_df
  }

  bind_rows(rows_all)
}

# ---------- 2018 plots helpers ----------
bin_weighted_points <- function(df, x, y, w, n_bins = N_BINS_X) {
  df2 <- df %>% filter(is.finite(.data[[x]]), is.finite(.data[[y]]), is.finite(.data[[w]]), .data[[w]] > 0)
  if (nrow(df2) == 0) return(tibble())
  xr <- range(df2[[x]], na.rm = TRUE)
  if (!all(is.finite(xr)) || xr[1] == xr[2]) return(tibble())

  br <- seq(xr[1], xr[2], length.out = n_bins + 1)

  df2 %>%
    mutate(x_bin = cut(.data[[x]], breaks = br, include.lowest = TRUE, right = FALSE)) %>%
    group_by(x_bin) %>%
    summarise(
      x_mid = mean(.data[[x]], na.rm = TRUE),
      y_bin = weighted.mean(.data[[y]], w = .data[[w]], na.rm = TRUE),
      w_sum = sum(.data[[w]], na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(is.finite(x_mid), is.finite(y_bin), w_sum > 0)
}

weighted_slope_se <- function(df, x, y, w) {
  df2 <- df %>% filter(is.finite(.data[[x]]), is.finite(.data[[y]]), is.finite(.data[[w]]), .data[[w]] > 0)
  if (nrow(df2) < 5) return(list(beta = NA_real_, se = NA_real_, n = nrow(df2), a0 = NA_real_))
  fit <- lm(reformulate(x, y), data = df2, weights = df2[[w]])
  co <- summary(fit)$coefficients
  if (!(x %in% rownames(co))) return(list(beta = NA_real_, se = NA_real_, n = nrow(df2), a0 = NA_real_))
  list(beta = unname(co[x, "Estimate"]), se = unname(co[x, "Std. Error"]), n = nrow(df2), a0 = unname(co["(Intercept)", "Estimate"]))
}

make_binscatter_plot <- function(df, x, y, w, title, xlab, ylab, file_out) {
  pts <- bin_weighted_points(df, x = x, y = y, w = w, n_bins = N_BINS_X)
  ws  <- weighted_slope_se(df, x = x, y = y, w = w)

  note <- if (is.finite(ws$beta) && is.finite(ws$se)) {
    paste0("Unbinned weighted slope:\n",
           "beta = ", sprintf("%.4f", ws$beta), "\n",
           "se   = ", sprintf("%.4f", ws$se), "\n",
           "N(clusters) = ", ws$n)
  } else {
    "Unbinned weighted slope: NA"
  }

  g <- ggplot() +
    geom_point(data = pts, aes(x = x_mid, y = y_bin), size = 2) +
    geom_abline(intercept = ws$a0, slope = ws$beta, linewidth = 0.9) +
    labs(title = title, x = xlab, y = ylab) +
    annotate("label", x = Inf, y = Inf, label = note, hjust = 1.05, vjust = 1.05, size = 3) +
    theme_minimal(base_size = 12)

  ggsave(file_out, g, width = 7.5, height = 5.2, dpi = 200)
}

div_summary_stats <- function(df, div_col) {
  x <- df[[div_col]]
  tibble(
    n = sum(is.finite(x)),
    mean = mean(x, na.rm = TRUE),
    sd   = sd(x, na.rm = TRUE),
    p10  = quantile(x, 0.10, na.rm = TRUE),
    p50  = quantile(x, 0.50, na.rm = TRUE),
    p90  = quantile(x, 0.90, na.rm = TRUE),
    min  = min(x, na.rm = TRUE),
    max  = max(x, na.rm = TRUE)
  )
}

plot_div_hist <- function(df, div_col, title, file_out) {
  g <- ggplot(df, aes(x = .data[[div_col]])) +
    geom_histogram(bins = 40) +
    labs(title = title, x = div_col, y = "Count") +
    theme_minimal(base_size = 12)
  ggsave(file_out, g, width = 7.5, height = 5.0, dpi = 200)
}

# =========================
# Open Arrow dataset
# =========================
cat("\n[INFO] Opening Arrow dataset...\n")
ds <- open_dataset(INPUT, format = "parquet")

need_cols(ds, c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR,
  FIRM_VAR, METRO_VAR
))

# years to run
years_all <- ds %>% select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
years_to_run <- tail(years_all, USE_LAST_N_YEARS)
years_to_run <- setdiff(years_to_run, SKIP_YEARS)
cat("[INFO] Running years:", paste(years_to_run, collapse=", "), "\n")

ds_y <- ds %>% filter(year %in% years_to_run)

# first_pos_country map
cat("[INFO] Building first_pos_country map...\n")
first_pos_map <- build_first_pos_country_map(ds_y)

# collect working df
cat("[INFO] Collecting working columns to memory...\n")
df <- ds_y %>%
  select(
    user_id, year, n_patents,
    first_country, last_country,
    first_university_country,
    first_startdate_edu, first_startdate_pos,
    !!sym(IMMIG_VAR),
    !!sym(FIRM_VAR), !!sym(METRO_VAR)
  ) %>%
  collect() %>%
  mutate(
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!FIRM_VAR  := as.character(.data[[FIRM_VAR]]),
    !!METRO_VAR := as.character(.data[[METRO_VAR]]),
    n_patents = as.numeric(n_patents),
    first_university_country = str_trim(as.character(first_university_country))
  )

# enrich
df <- df %>%
  left_join(first_pos_map, by = "user_id") %>%
  mutate(first_pos_country = str_trim(as.character(first_pos_country))) %>%
  compute_tenure() %>%
  filter(!is.na(tenure)) %>%
  make_origin_country()

# =========================
# Compute cluster-year diversity + controls
# =========================
cat("[INFO] Computing FIRM-year origin diversity + controls...\n")
firm_origin_stats <- cluster_year_div_stats_dt(df, FIRM_VAR, "FIRM_year",
                                               "origin_country", "origin_div", MIN_CLUSTER)

cat("[INFO] Computing FIRM-year first-job-country diversity + controls...\n")
firm_job_stats <- cluster_year_div_stats_dt(df, FIRM_VAR, "FIRM_year",
                                            "first_pos_country", "job_div", MIN_CLUSTER)

cat("[INFO] Computing FIRM-year first-university-country diversity + controls...\n")
firm_edu_stats <- cluster_year_div_stats_dt(df, FIRM_VAR, "FIRM_year",
                                            "first_university_country", "edu_div", MIN_CLUSTER)

firm_stats <- firm_origin_stats %>%
  left_join(firm_job_stats %>% select(cluster_id, year, job_div), by = c("cluster_id","year")) %>%
  left_join(firm_edu_stats %>% select(cluster_id, year, edu_div), by = c("cluster_id","year"))

write_csv(firm_stats, file.path(TABLES_DIR, "firm_year_diversity_origin_job_edu.csv"))

cat("[INFO] Computing METRO-year origin diversity + controls...\n")
metro_origin_stats <- cluster_year_div_stats_dt(df, METRO_VAR, "METRO_year",
                                                "origin_country", "origin_div", MIN_CLUSTER)

cat("[INFO] Computing METRO-year first-job-country diversity + controls...\n")
metro_job_stats <- cluster_year_div_stats_dt(df, METRO_VAR, "METRO_year",
                                             "first_pos_country", "job_div", MIN_CLUSTER)

cat("[INFO] Computing METRO-year first-university-country diversity + controls...\n")
metro_edu_stats <- cluster_year_div_stats_dt(df, METRO_VAR, "METRO_year",
                                             "first_university_country", "edu_div", MIN_CLUSTER)

metro_stats <- metro_origin_stats %>%
  left_join(metro_job_stats %>% select(cluster_id, year, job_div), by = c("cluster_id","year")) %>%
  left_join(metro_edu_stats %>% select(cluster_id, year, edu_div), by = c("cluster_id","year"))

write_csv(metro_stats, file.path(TABLES_DIR, "metro_year_diversity_origin_job_edu.csv"))

# =========================
# Merge back to inventor-year (explicit cluster_id columns)
# =========================
df_reg <- df %>%
  mutate(
    firm_cluster_id  = as.character(.data[[FIRM_VAR]]),
    metro_cluster_id = as.character(.data[[METRO_VAR]]),
    origin_country = str_trim(as.character(origin_country)),
    first_pos_country = str_trim(as.character(first_pos_country)),
    first_university_country = str_trim(as.character(first_university_country))
  )

# attach firm stats
df_reg <- df_reg %>%
  mutate(cluster_id = firm_cluster_id) %>%
  left_join(
    firm_stats %>%
      transmute(cluster_id, year,
                firm_origin_div   = origin_div,
                firm_job_div      = job_div,
                firm_edu_div      = edu_div,
                firm_log_size     = log_cluster,
                firm_immig_share  = immig_share),
    by = c("cluster_id","year")
  ) %>%
  select(-cluster_id)

# attach metro stats
df_reg <- df_reg %>%
  mutate(cluster_id = metro_cluster_id) %>%
  left_join(
    metro_stats %>%
      transmute(cluster_id, year,
                metro_origin_div  = origin_div,
                metro_job_div     = job_div,
                metro_edu_div     = edu_div,
                metro_log_size    = log_cluster,
                metro_immig_share = immig_share),
    by = c("cluster_id","year")
  ) %>%
  select(-cluster_id)

write_csv(
  df_reg %>%
    select(user_id, year, n_patents, tenure, tenure_sq,
           origin_country, first_pos_country, first_university_country,
           firm_cluster_id, firm_origin_div, firm_job_div, firm_edu_div, firm_log_size, firm_immig_share,
           metro_cluster_id, metro_origin_div, metro_job_div, metro_edu_div, metro_log_size, metro_immig_share) %>%
    sample_n(min(200000, nrow(df_reg))),
  file.path(TABLES_DIR, "sample_inventor_year_with_div_controls.csv")
)

# =========================
# Run pooled regressions (ALL TERMS output)
# =========================
cat("[INFO] Running pooled regressions...\n")

pooled_results <- list()

# Firm
for (div_suf in c("origin_div","job_div","edu_div")) {
  cfe_var <- switch(div_suf,
                    origin_div = "origin_country",
                    job_div    = "first_pos_country",
                    edu_div    = "first_university_country")
  lvl <- paste0("FIRM_year_", sub("_div","", div_suf))
  cat("[INFO]  - ", lvl, "\n")
  d <- df_reg %>% filter(!is.na(.data[[paste0("firm_", div_suf)]]))
  pooled_results[[lvl]] <- run_div_regressions(
    d = d, prefix = "firm", level_label = lvl, out_dir = MODELS_DIR,
    div_suffix = div_suf, country_fe_var = cfe_var, use_ppml = FALSE
  )
}

# Metro
for (div_suf in c("origin_div","job_div","edu_div")) {
  cfe_var <- switch(div_suf,
                    origin_div = "origin_country",
                    job_div    = "first_pos_country",
                    edu_div    = "first_university_country")
  lvl <- paste0("METRO_year_", sub("_div","", div_suf))
  cat("[INFO]  - ", lvl, "\n")
  d <- df_reg %>% filter(!is.na(.data[[paste0("metro_", div_suf)]]))
  pooled_results[[lvl]] <- run_div_regressions(
    d = d, prefix = "metro", level_label = lvl, out_dir = MODELS_DIR,
    div_suffix = div_suf, country_fe_var = cfe_var, use_ppml = FALSE
  )
}

res_pooled_all <- bind_rows(pooled_results, .id = "run_id")
write_csv(res_pooled_all, file.path(MODELS_DIR, "POOLED_ALLTERMS__firm_vs_metro__origin_job_edu.csv"))

# =========================
# Year-by-year regressions (ALL TERMS output)
# =========================
cat("[INFO] Running YEAR-BY-YEAR regressions...\n")
all_year_results <- list()

for (yy in years_vec) {
  cat("[INFO]  Year ", yy, "\n")
  dyy <- df_reg %>% filter(year == yy)

  one_year_runs <- list()

  # Firm
  for (div_suf in c("origin_div","job_div","edu_div")) {
    cfe_var <- switch(div_suf,
                      origin_div = "origin_country",
                      job_div    = "first_pos_country",
                      edu_div    = "first_university_country")
    lvl <- paste0("FIRM_year_", sub("_div","", div_suf))
    r <- run_div_regressions_one_year(
      d = dyy, prefix = "firm", level_label = lvl, out_dir = MODELS_DIR,
      div_suffix = div_suf, country_fe_var = cfe_var, use_ppml = FALSE
    ) %>% mutate(year = yy)
    one_year_runs[[paste0(lvl,"__",div_suf)]] <- r
  }

  # Metro
  for (div_suf in c("origin_div","job_div","edu_div")) {
    cfe_var <- switch(div_suf,
                      origin_div = "origin_country",
                      job_div    = "first_pos_country",
                      edu_div    = "first_university_country")
    lvl <- paste0("METRO_year_", sub("_div","", div_suf))
    r <- run_div_regressions_one_year(
      d = dyy, prefix = "metro", level_label = lvl, out_dir = MODELS_DIR,
      div_suffix = div_suf, country_fe_var = cfe_var, use_ppml = FALSE
    ) %>% mutate(year = yy)
    one_year_runs[[paste0(lvl,"__",div_suf)]] <- r
  }

  all_year_results[[as.character(yy)]] <- bind_rows(one_year_runs, .id = "run_id")
}

res_by_year <- bind_rows(all_year_results)
write_csv(res_by_year, file.path(MODELS_DIR, "BYYEAR_ALLTERMS__firm_vs_metro__origin_job_edu.csv"))

# =========================
# 2018 distributions + bin-scatters
# =========================
cat("[INFO] Building 2018 distribution + bin-scatter plots...\n")

make_2018_outputs <- function(stats_df, level_name) {
  df18 <- stats_df %>% filter(year == YEAR_PLOTS)
  if (nrow(df18) == 0) return(invisible(NULL))

  # summary stats table
  summ <- bind_rows(
    div_summary_stats(df18, "origin_div") %>% mutate(measure = "origin_div"),
    div_summary_stats(df18, "job_div")    %>% mutate(measure = "job_div"),
    div_summary_stats(df18, "edu_div")    %>% mutate(measure = "edu_div")
  ) %>% select(measure, everything())

  write_csv(summ, file.path(TABLES_DIR, paste0("dist_summary_", level_name, "_", YEAR_PLOTS, ".csv")))

  # histograms
  plot_div_hist(df18, "origin_div",
                paste0(level_name, " ", YEAR_PLOTS, ": Distribution of origin_div"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_origin_div.png")))

  plot_div_hist(df18, "job_div",
                paste0(level_name, " ", YEAR_PLOTS, ": Distribution of job_div"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_job_div.png")))

  plot_div_hist(df18, "edu_div",
                paste0(level_name, " ", YEAR_PLOTS, ": Distribution of edu_div"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_edu_div.png")))

  # bin-scatters: y=div vs x=log_cluster AND x=immig_share
  for (dv in c("origin_div","job_div","edu_div")) {
    # vs log_cluster
    make_binscatter_plot(
      df = df18, x = "log_cluster", y = dv, w = "cluster_size",
      title = paste0(level_name, " ", YEAR_PLOTS, ": ", dv, " vs log(cluster size) (15 bins; w=cluster_size)"),
      xlab = "log(cluster size)", ylab = dv,
      file_out = file.path(PLOTS_DIR, paste0("binscatter_", level_name, "_", YEAR_PLOTS, "_", dv, "_vs_logsize.png"))
    )
    # vs immigrant share
    make_binscatter_plot(
      df = df18, x = "immig_share", y = dv, w = "cluster_size",
      title = paste0(level_name, " ", YEAR_PLOTS, ": ", dv, " vs immigrant share (15 bins; w=cluster_size)"),
      xlab = "Immigrant share in cluster-year", ylab = dv,
      file_out = file.path(PLOTS_DIR, paste0("binscatter_", level_name, "_", YEAR_PLOTS, "_", dv, "_vs_share.png"))
    )
  }
}

make_2018_outputs(firm_stats,  "FIRM_year")
make_2018_outputs(metro_stats, "METRO_year")

cat("\n[DONE]\nOutputs in:\n", OUT_DIR, "\n")
cat("Pooled ALL-TERMS table:\n", file.path(MODELS_DIR, "POOLED_ALLTERMS__firm_vs_metro__origin_job_edu.csv"), "\n")
cat("Year-by-year ALL-TERMS table:\n", file.path(MODELS_DIR, "BYYEAR_ALLTERMS__firm_vs_metro__origin_job_edu.csv"), "\n")
cat("2018 plots dir:\n", PLOTS_DIR, "\n")
