##############################################################################
# DIVERSITY (ORIGIN + JOB + EDUCATION) — Parent firm, firm (first_rcid),
# and Metro clusters
#
# CORRECT PROCEDURE (given our debugging):
# - The "first_*" variables ARE time-varying across years in your panel (first within year).
# - The FE problem happened because we tried to estimate FE on ONE YEAR (cross-section),
#   which makes user_id FE all singletons by construction.
#
# Therefore:
# 1) Build pooled sample = last 15 years EXCLUDING 2025.
# 2) Run baseline PPML on pooled years to recover FE levels:
#      n_patents ~ tenure + tenure_sq | user_id + first_metro_area + year + first_parent_rcid
#      n_patents ~ tenure + tenure_sq | user_id + first_metro_area + year + first_rcid
#    Then save ALL FE blocks we can (user, year, metro, parent, firm).
# 3) Compute cluster-year diversity on the SAME pooled sample.
# 4) Run the diversity regressions (OLS fixest + PPML alpaca).
# 5) For FE-vs-diversity figures, merge firm/parent FE (from pooled baseline) to
#    firm-year diversity in YEAR_PLOTS (e.g., 2018), then plot (unbinned + binned-x=15).
##############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","ggplot2","tidyr","scales","tibble")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}

if (!requireNamespace("remotes", quietly = TRUE))
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
if (!requireNamespace("alpaca", quietly = TRUE)) {
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib, upgrade = "never")
}

suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(ggplot2); library(tidyr); library(scales); library(tibble)
  library(alpaca)
})

set.seed(123)

# =========================
# 1) Paths + knobs
# =========================
INPUT  <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_origin_job_edu_firm_metro"

DIRS <- list(
  tables        = file.path(OUT_DIR, "tables"),
  models        = file.path(OUT_DIR, "models"),
  fe            = file.path(OUT_DIR, "fixed_effects_pooled15"),
  plots_2018    = file.path(OUT_DIR, "plots_2018"),
  fe_plots      = file.path(OUT_DIR, "plots_FE_vs_div_2018"),
  fe_tables     = file.path(OUT_DIR, "tables_FE_vs_div_2018")
)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

MODELS_OLS  <- file.path(DIRS$models, "ols_fixest")
MODELS_PPML <- file.path(DIRS$models, "ppml_alpaca")
dir.create(MODELS_OLS,  recursive = TRUE, showWarnings = FALSE)
dir.create(MODELS_PPML, recursive = TRUE, showWarnings = FALSE)

# Core column names (per your dataset)
IMMIG_VAR <- "immig_first_deg_or_job_nonUS"
FIRM_PARENT_VAR <- "first_parent_rcid"
FIRM_RCID_VAR   <- "first_rcid"
METRO_VAR        <- "first_metro_area"

# Years for pooled estimation
USE_LAST_N_YEARS <- 15
EXCLUDE_YEARS    <- c(2025)     # per your request; add others if desired

# Optional: year-by-year regressions (can be slow; keep as before)
years_vec <- 2016:2019

# Diversity / cleaning knobs
MIN_CLUSTER <- 10
TENURE_MAX  <- 50

# FE-vs-diversity plot year
YEAR_PLOTS <- 2018
N_BINS_X   <- 15

# PPML tolerances
PPML_CTRL <- alpaca::feglmControl(dev.tol = 1e-8, center.tol = 1e-8, iter.max = 80, trace = FALSE)

# =========================
# 2) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}

sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}

parse_fe_vars <- function(fe_part) {
  fe_part <- gsub("\\s+", "", fe_part)
  if (fe_part == "" || fe_part == "fe_dummy") return(character(0))
  strsplit(fe_part, "\\+")[[1]]
}

filter_nonmissing_fe <- function(dd, fe_part) {
  fe_vars <- parse_fe_vars(fe_part)
  for (v in fe_vars) {
    if (!v %in% names(dd)) next
    if (is.character(dd[[v]])) dd <- dd %>% filter(!is.na(.data[[v]]), .data[[v]] != "")
    else                       dd <- dd %>% filter(!is.na(.data[[v]]))
  }
  dd
}

# =========================
# 3) Derived variables (tenure, first_pos_country, origin_country)
# =========================
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

add_first_pos_country <- function(df) {
  dt <- as.data.table(df)
  dt[, pos_country_y := fifelse(!is.na(last_country) & last_country != "", last_country, first_country)]
  dt[pos_country_y == ""] <- NA_character_
  setorder(dt, user_id, year)
  dt[, first_pos_country := pos_country_y[which(!is.na(pos_country_y))[1]], by = user_id]
  as_tibble(dt[, !"pos_country_y"])
}

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

# =========================
# 4) Diversity computation (data.table, unique inventors)
# =========================
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

compute_cluster_bundle <- function(df, cluster_var, label) {
  ts_msg("Computing", label, "origin/job/edu diversity...")
  origin_stats <- cluster_year_div_stats_dt(df, cluster_var, label, "origin_country", "origin_div", MIN_CLUSTER)
  job_stats    <- cluster_year_div_stats_dt(df, cluster_var, label, "first_pos_country", "job_div", MIN_CLUSTER)
  edu_stats    <- cluster_year_div_stats_dt(df, cluster_var, label, "first_university_country", "edu_div", MIN_CLUSTER)

  origin_stats %>%
    left_join(job_stats %>% select(cluster_id, year, job_div), by = c("cluster_id","year")) %>%
    left_join(edu_stats %>% select(cluster_id, year, edu_div), by = c("cluster_id","year"))
}

# =========================
# 5) Regression builders + estimators (same as your logic)
# =========================
make_specs <- function(div, logsz, share, country_fe_var, pooled = TRUE) {
  fe0 <- "fe_dummy"
  if (pooled) {
    list(
      raw         = list(rhs = paste0(div),                                 fe = fe0),
      size        = list(rhs = paste0(div, " + ", logsz),                   fe = fe0),
      size_cfe    = list(rhs = paste0(div, " + ", logsz),                   fe = country_fe_var),
      size_sh     = list(rhs = paste0(div, " + ", logsz, " + ", share),     fe = fe0),
      size_sh_cfe = list(rhs = paste0(div, " + ", logsz, " + ", share),     fe = country_fe_var),
      full        = list(rhs = paste0(div, " + ", logsz, " + ", share, " + tenure + tenure_sq"),
                        fe  = paste0("year + ", country_fe_var))
    )
  } else {
    list(
      raw         = list(rhs = paste0(div),                                 fe = fe0),
      size        = list(rhs = paste0(div, " + ", logsz),                   fe = fe0),
      size_cfe    = list(rhs = paste0(div, " + ", logsz),                   fe = country_fe_var),
      size_sh     = list(rhs = paste0(div, " + ", logsz, " + ", share),     fe = fe0),
      size_sh_cfe = list(rhs = paste0(div, " + ", logsz, " + ", share),     fe = country_fe_var),
      full        = list(rhs = paste0(div, " + ", logsz, " + ", share, " + tenure + tenure_sq"),
                        fe  = country_fe_var)
    )
  }
}

prep_reg_data <- function(d, needed_vars) {
  if ("fe_dummy" %in% needed_vars && !"fe_dummy" %in% names(d)) {
    d <- d %>% mutate(fe_dummy = as.factor(1L))
  }
  d %>% select(all_of(needed_vars)) %>% filter(!is.na(n_patents), n_patents >= 0)
}

extract_terms_fixest <- function(fit, terms_wanted) {
  ct <- fixest::coeftable(fit)
  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(term = terms_wanted, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_))
  }
  rn <- rownames(ct)
  bind_rows(lapply(terms_wanted, function(tt) {
    if (tt %in% rn) {
      tibble(term = tt,
             estimate = unname(ct[tt,1]),
             se       = unname(ct[tt,2]),
             t_stat   = unname(ct[tt,3]),
             p_value  = unname(ct[tt,4]))
    } else {
      tibble(term = tt, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_)
    }
  }))
}

extract_terms_alpaca <- function(model, cluster_var, terms_wanted) {
  sm <- summary(model, type = "clustered", cluster = reformulate(cluster_var))
  ct <- as.matrix(coef(sm))
  rn <- rownames(ct)
  pcol <- grep("^Pr\\(", colnames(ct), value = TRUE)
  pcol <- if (length(pcol)) pcol[1] else NA_character_

  bind_rows(lapply(terms_wanted, function(tt) {
    if (tt %in% rn) {
      tibble(term     = tt,
             estimate = unname(ct[tt,"Estimate"]),
             se       = unname(ct[tt,"Std. error"]),
             t_stat   = unname(ct[tt,"z value"]),
             p_value  = if (!is.na(pcol)) unname(ct[tt,pcol]) else NA_real_)
    } else {
      tibble(term = tt, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_)
    }
  }))
}

run_specs_ols_fixest <- function(d, prefix, level_label, out_dir, div_suffix, country_fe_var, pooled = TRUE) {
  div   <- paste0(prefix, "_", div_suffix)
  logsz <- paste0(prefix, "_log_size")
  share <- paste0(prefix, "_immig_share")
  clid  <- paste0(prefix, "_cluster_id")

  specs <- make_specs(div, logsz, share, country_fe_var, pooled = pooled)
  terms_wanted <- unique(c(div, logsz, share, "tenure", "tenure_sq"))
  rows_all <- list()

  for (nm in names(specs)) {
    rhs <- specs[[nm]]$rhs
    fe  <- specs[[nm]]$fe

    f <- if (fe == "fe_dummy") as.formula(paste0("n_patents ~ ", rhs))
         else                  as.formula(paste0("n_patents ~ ", rhs, " | ", fe))

    needed <- unique(c(all.vars(f), clid))
    dd <- prep_reg_data(d, needed)

    dd <- dd %>%
      filter(!is.na(.data[[clid]]), .data[[clid]] != "") %>%
      filter(!is.na(.data[[div]]), is.finite(.data[[div]]))

    if (logsz %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[logsz]]))
    if (share %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[share]]))
    if (any(c("tenure","tenure_sq") %in% all.vars(f))) dd <- dd %>% filter(is.finite(tenure), is.finite(tenure_sq))
    if (fe != "fe_dummy") dd <- filter_nonmissing_fe(dd, fe)

    if (nrow(dd) < 1000) {
      rows_all[[nm]] <- tibble(cluster_level = level_label, diversity_measure = div_suffix, estimator = "OLS_fixest",
                               spec = nm, n_obs = nrow(dd),
                               term = terms_wanted, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_)
      next
    }

    fit <- fixest::feols(f, data = dd, vcov = as.formula(paste0("~", clid)), warn = FALSE)
    capture.output(summary(fit),
                   file = file.path(out_dir, paste0("OLS__", level_label, "__", div_suffix, "__", nm, ".txt")))

    rows_all[[nm]] <- extract_terms_fixest(fit, terms_wanted) %>%
      mutate(cluster_level = level_label, diversity_measure = div_suffix, estimator = "OLS_fixest",
             spec = nm, n_obs = as.integer(nobs(fit))) %>%
      select(cluster_level, diversity_measure, estimator, spec, n_obs, term, estimate, se, t_stat, p_value)
  }
  bind_rows(rows_all)
}

run_specs_ppml_alpaca <- function(d, prefix, level_label, out_dir, div_suffix, country_fe_var, pooled = TRUE) {
  div   <- paste0(prefix, "_", div_suffix)
  logsz <- paste0(prefix, "_log_size")
  share <- paste0(prefix, "_immig_share")
  clid  <- paste0(prefix, "_cluster_id")

  specs <- make_specs(div, logsz, share, country_fe_var, pooled = pooled)
  terms_wanted <- unique(c(div, logsz, share, "tenure", "tenure_sq"))
  rows_all <- list()

  for (nm in names(specs)) {
    rhs     <- specs[[nm]]$rhs
    fe_part <- specs[[nm]]$fe

    f <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_part, " | ", clid))

    needed <- unique(all.vars(f))
    dd <- prep_reg_data(d, needed)

    dd <- dd %>%
      filter(!is.na(.data[[div]]), is.finite(.data[[div]])) %>%
      filter(!is.na(.data[[clid]]), .data[[clid]] != "") %>%
      mutate(!!clid := as.factor(.data[[clid]]))

    if ("year" %in% names(dd))    dd <- dd %>% mutate(year = as.factor(year))
    if ("user_id" %in% names(dd)) dd <- dd %>% mutate(user_id = as.factor(user_id))

    if (logsz %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[logsz]]))
    if (share %in% all.vars(f)) dd <- dd %>% filter(is.finite(.data[[share]]))
    if (any(c("tenure","tenure_sq") %in% all.vars(f))) dd <- dd %>% filter(is.finite(tenure), is.finite(tenure_sq))

    fe_vars <- strsplit(gsub("\\s+", "", fe_part), "\\+")[[1]]
    fe_vars <- fe_vars[fe_vars != ""]
    for (v in fe_vars) {
      if (!v %in% names(dd)) next
      if (is.character(dd[[v]])) dd <- dd %>% filter(!is.na(.data[[v]]), .data[[v]] != "")
      else                       dd <- dd %>% filter(!is.na(.data[[v]]))
    }

    if (nrow(dd) < 1000) {
      rows_all[[nm]] <- tibble(cluster_level = level_label, diversity_measure = div_suffix, estimator = "PPML_alpaca",
                               spec = nm, n_obs = nrow(dd),
                               term = terms_wanted, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_)
      next
    }

    model <- suppressWarnings(alpaca::feglm(formula = f, data = dd, family = poisson(), control = PPML_CTRL))
    capture.output(summary(model),
                   file = file.path(out_dir, paste0("PPML__", level_label, "__", div_suffix, "__", nm, ".txt")))

    tmp <- extract_terms_alpaca(model, cluster_var = clid, terms_wanted = terms_wanted)
    tmp$cluster_level     <- level_label
    tmp$diversity_measure <- div_suffix
    tmp$estimator         <- "PPML_alpaca"
    tmp$spec              <- nm
    tmp$n_obs             <- as.integer(nrow(dd))

    rows_all[[nm]] <- tmp %>%
      select(cluster_level, diversity_measure, estimator, spec, n_obs, term, estimate, se, t_stat, p_value)
  }

  bind_rows(rows_all)
}

# =========================
# 6) Plot helpers (FE vs diversity)
# =========================
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

wls_slope_se <- function(df, x, y, w) {
  df2 <- df %>% filter(is.finite(.data[[x]]), is.finite(.data[[y]]), is.finite(.data[[w]]), .data[[w]] > 0)
  if (nrow(df2) < 10) return(list(beta = NA_real_, se = NA_real_, a0 = NA_real_, n = nrow(df2), p = NA_real_))
  fit <- lm(reformulate(x, y), data = df2, weights = df2[[w]])
  co <- summary(fit)$coefficients
  list(beta = unname(co[x, "Estimate"]),
       se   = unname(co[x, "Std. Error"]),
       a0   = unname(co["(Intercept)", "Estimate"]),
       n    = nrow(df2),
       p    = unname(co[x, "Pr(>|t|)"]))
}

plot_fe_vs_div <- function(df, div_col, out_prefix, title_prefix, fe_col = "fe_value") {
  ws <- wls_slope_se(df, x = div_col, y = fe_col, w = "cluster_size")
  note <- paste0("Unbinned WLS (w=cluster_size)\n",
                 "β=", sprintf("%.3f", ws$beta), "  se=", sprintf("%.3f", ws$se), "  ", sig_stars(ws$p), "\n",
                 "N=", ws$n)

  p1 <- ggplot(df, aes(x = .data[[div_col]], y = .data[[fe_col]])) +
    geom_point(aes(size = cluster_size), alpha = 0.18) +
    geom_abline(intercept = ws$a0, slope = ws$beta, linewidth = 0.9) +
    scale_size_continuous(labels = comma, guide = "none") +
    labs(title = paste0(title_prefix, " (unbinned)"), x = div_col, y = fe_col) +
    annotate("text", x = Inf, y = -Inf, label = note, hjust = 1.02, vjust = -0.2, size = 3.3) +
    theme_minimal(base_size = 12)
  ggsave(paste0(out_prefix, "__unbinned.png"), p1, width = 8.2, height = 5.6, dpi = 180)

  pts <- bin_weighted_points(df, x = div_col, y = fe_col, w = "cluster_size", n_bins = N_BINS_X)
  p2 <- ggplot(pts, aes(x = x_mid, y = y_bin)) +
    geom_point(size = 2.2) +
    geom_abline(intercept = ws$a0, slope = ws$beta, linewidth = 0.9) +
    labs(title = paste0(title_prefix, " (binned x=15, line from unbinned WLS)"),
         x = paste0(div_col, " (binned)"), y = paste0(fe_col, " (binned mean)")) +
    annotate("text", x = Inf, y = -Inf, label = note, hjust = 1.02, vjust = -0.2, size = 3.3) +
    theme_minimal(base_size = 12)
  ggsave(paste0(out_prefix, "__binnedx15.png"), p2, width = 8.2, height = 5.6, dpi = 180)

  tibble(measure = div_col, n = ws$n, beta = ws$beta, se = ws$se, p = ws$p, stars = sig_stars(ws$p))
}

# =========================
# 7) Load Arrow + choose pooled years (last 15 excluding 2025)
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")

need_cols(names(ds), c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR,
  FIRM_PARENT_VAR, FIRM_RCID_VAR, METRO_VAR
))

years_all <- ds %>% select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
years_pool <- tail(years_all, USE_LAST_N_YEARS)
years_pool <- setdiff(years_pool, EXCLUDE_YEARS)
ts_msg("Pooled years:", paste(years_pool, collapse = ", "))

ds_y <- ds %>% filter(year %in% years_pool)

# =========================
# 8) Collect working columns to memory
# =========================
ts_msg("Collecting working columns to memory (single collect)...")
df <- ds_y %>%
  select(
    user_id, year, n_patents,
    first_country, last_country,
    first_university_country,
    first_startdate_edu, first_startdate_pos,
    !!sym(IMMIG_VAR),
    !!sym(FIRM_PARENT_VAR), !!sym(FIRM_RCID_VAR), !!sym(METRO_VAR)
  ) %>%
  collect() %>%
  mutate(
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    n_patents = as.numeric(n_patents),
    first_university_country = str_trim(as.character(first_university_country)),
    !!FIRM_PARENT_VAR := as.character(.data[[FIRM_PARENT_VAR]]),
    !!FIRM_RCID_VAR   := as.character(.data[[FIRM_RCID_VAR]]),
    !!METRO_VAR       := as.character(.data[[METRO_VAR]])
  )

ts_msg("Building first_pos_country + tenure + origin_country...")
df <- df %>%
  add_first_pos_country() %>%
  mutate(first_pos_country = str_trim(as.character(first_pos_country))) %>%
  compute_tenure() %>%
  filter(!is.na(tenure)) %>%
  make_origin_country()

# =========================
# 9) Cluster-year diversity tables (pooled years)
# =========================
parent_stats <- compute_cluster_bundle(df, FIRM_PARENT_VAR, "PARENT_year")
firm_stats   <- compute_cluster_bundle(df, FIRM_RCID_VAR,   "FIRM_year")
metro_stats  <- compute_cluster_bundle(df, METRO_VAR,       "METRO_year")

write_csv(parent_stats, file.path(DIRS$tables, "parent_year_diversity_origin_job_edu.csv"))
write_csv(firm_stats,   file.path(DIRS$tables, "firm_year_diversity_origin_job_edu.csv"))
write_csv(metro_stats,  file.path(DIRS$tables, "metro_year_diversity_origin_job_edu.csv"))

# =========================
# 10) Merge diversity back to inventor-year (df_reg)
# =========================
df_reg <- df %>%
  mutate(
    parent_cluster_id = as.character(.data[[FIRM_PARENT_VAR]]),
    firm_cluster_id   = as.character(.data[[FIRM_RCID_VAR]]),
    metro_cluster_id  = as.character(.data[[METRO_VAR]])
  )

attach_stats <- function(df_reg, stats_df, cluster_id_col, prefix) {
  tmp <- stats_df %>%
    transmute(
      cluster_id, year,
      !!paste0(prefix, "_origin_div")  := origin_div,
      !!paste0(prefix, "_job_div")     := job_div,
      !!paste0(prefix, "_edu_div")     := edu_div,
      !!paste0(prefix, "_log_size")    := log_cluster,
      !!paste0(prefix, "_immig_share") := immig_share,
      !!paste0(prefix, "_cluster_size") := cluster_size
    )

  df_reg %>%
    mutate(cluster_id = .data[[cluster_id_col]]) %>%
    left_join(tmp, by = c("cluster_id","year")) %>%
    select(-cluster_id)
}

df_reg <- df_reg %>%
  attach_stats(parent_stats, "parent_cluster_id", "parent") %>%
  attach_stats(firm_stats,   "firm_cluster_id",   "firm") %>%
  attach_stats(metro_stats,  "metro_cluster_id",  "metro")

# =========================
# 11) Run diversity regressions (pooled + by-year)
# =========================
run_all_estimators <- function(df_reg, pooled = TRUE) {
  pooled_tag <- ifelse(pooled, "POOLED", "BYYEAR")
  res <- list()

  clusters <- list(
    parent = list(prefix = "parent", label = "PARENT_year", cluster_id = "parent_cluster_id"),
    firm   = list(prefix = "firm",   label = "FIRM_year",   cluster_id = "firm_cluster_id"),
    metro  = list(prefix = "metro",  label = "METRO_year",  cluster_id = "metro_cluster_id")
  )

  for (cl in names(clusters)) {
    info <- clusters[[cl]]
    prefix <- info$prefix
    label  <- info$label

    for (div_suf in c("origin_div","job_div","edu_div")) {
      cfe_var <- switch(div_suf,
                        origin_div = "origin_country",
                        job_div    = "first_pos_country",
                        edu_div    = "first_university_country")
      lvl <- paste0(label, "_", sub("_div","", div_suf))
      ts_msg(pooled_tag, ":", lvl)

      d_use <- df_reg %>% filter(!is.na(.data[[paste0(prefix, "_", div_suf)]]))

      res[[paste0("OLS__", pooled_tag, "__", lvl, "__", div_suf)]] <-
        run_specs_ols_fixest(d_use, prefix, lvl, MODELS_OLS, div_suf, cfe_var, pooled = pooled)

      res[[paste0("PPML__", pooled_tag, "__", lvl, "__", div_suf)]] <-
        run_specs_ppml_alpaca(d_use, prefix, lvl, MODELS_PPML, div_suf, cfe_var, pooled = pooled)
    }
  }
  bind_rows(res, .id = "run_id")
}

ts_msg("Running POOLED regressions (OLS + PPML)...")
res_pooled <- run_all_estimators(df_reg, pooled = TRUE)
write_csv(res_pooled, file.path(DIRS$models, "POOLED_ALLTERMS__ols_fixest__ppml_alpaca.csv"))

ts_msg("Running YEAR-BY-YEAR regressions (OLS + PPML)...")
all_year <- list()
for (yy in years_vec) {
  ts_msg("Year", yy)
  dyy <- df_reg %>% filter(year == yy)
  all_year[[as.character(yy)]] <- run_all_estimators(dyy, pooled = FALSE) %>% mutate(year = yy)
}
res_by_year <- bind_rows(all_year)
write_csv(res_by_year, file.path(DIRS$models, "BYYEAR_ALLTERMS__ols_fixest__ppml_alpaca.csv"))

# =========================
# 12) BASELINE PPML (POOLED YEARS) to recover + SAVE ALL FE blocks
# =========================
save_fixef_block <- function(fes, block_name, out_path, id_col) {
  v <- fes[[block_name]]
  if (is.null(v)) return(FALSE)
  out <- tibble(!!id_col := names(v), fe_value = as.numeric(v))
  write_csv(out, out_path)
  TRUE
}

run_baseline_fe_and_save <- function(df_reg, firm_var, tag) {
  ts_msg("Baseline FE estimation (pooled years):", tag)

  needed <- c("user_id","year","n_patents","tenure","tenure_sq", METRO_VAR, firm_var)
  dd <- df_reg %>%
    select(all_of(needed)) %>%
    filter(!is.na(n_patents), n_patents >= 0,
           is.finite(tenure), is.finite(tenure_sq),
           !is.na(user_id), user_id != "",
           !is.na(year),
           !is.na(.data[[METRO_VAR]]), .data[[METRO_VAR]] != "",
           !is.na(.data[[firm_var]]), .data[[firm_var]] != "") %>%
    mutate(
      user_id = as.factor(user_id),
      year    = as.factor(year),
      !!sym(METRO_VAR) := as.factor(.data[[METRO_VAR]]),
      !!sym(firm_var)  := as.factor(.data[[firm_var]])
    )

  fml <- as.formula(paste0(
    "n_patents ~ tenure + tenure_sq | user_id + ", METRO_VAR, " + year + ", firm_var
  ))

  # Use fixest for FE recovery (very robust)
  m <- fixest::fepois(fml, data = dd, notes = FALSE, warn = FALSE)
  fes <- fixest::fixef(m)

  # Save all FE blocks
  save_fixef_block(fes, "user_id",
                   file.path(DIRS$fe, paste0("FE_user__", tag, ".csv")),
                   "user_id")
  save_fixef_block(fes, "year",
                   file.path(DIRS$fe, paste0("FE_year__", tag, ".csv")),
                   "year")
  save_fixef_block(fes, METRO_VAR,
                   file.path(DIRS$fe, paste0("FE_metro__", tag, ".csv")),
                   METRO_VAR)
  save_fixef_block(fes, firm_var,
                   file.path(DIRS$fe, paste0("FE_", tag, ".csv")),
                   firm_var)

  ts_msg("Saved FE blocks for:", tag)
  list(model = m, fes = fes)
}

ts_msg("Estimating pooled-years baseline FE models and saving FE vectors...")
base_parent <- run_baseline_fe_and_save(df_reg, FIRM_PARENT_VAR, "parent")
base_firm   <- run_baseline_fe_and_save(df_reg, FIRM_RCID_VAR,   "firm")

# =========================
# 13) FE vs diversity plots (YEAR_PLOTS), FE from pooled baseline
# =========================
make_fe_div_plots <- function(stats_df, fe_csv_path, id_col_name, label_tag) {
  fe_df <- read_csv(fe_csv_path, show_col_types = FALSE) %>%
    rename(firm_id = !!sym(id_col_name)) %>%
    mutate(firm_id = as.character(firm_id))

  dfY <- stats_df %>%
    filter(year == YEAR_PLOTS) %>%
    transmute(
      firm_id = as.character(cluster_id),
      cluster_size = as.numeric(cluster_size),
      log_cluster  = as.numeric(log_cluster),
      immig_share  = as.numeric(immig_share),
      origin_div   = as.numeric(origin_div),
      job_div      = as.numeric(job_div),
      edu_div      = as.numeric(edu_div)
    ) %>%
    left_join(fe_df, by = "firm_id") %>%
    filter(is.finite(cluster_size), cluster_size > 0, is.finite(fe_value))

  rows <- list()
  for (m in c("origin_div","job_div","edu_div")) {
    dd <- dfY %>% filter(is.finite(.data[[m]]))
    if (nrow(dd) < 50) next
    out_prefix <- file.path(DIRS$fe_plots, paste0("FE_", label_tag, "_", m, "__", YEAR_PLOTS))
    title_pref <- paste0(YEAR_PLOTS, " FE (", label_tag, ", pooled15) vs ", m)
    rows[[m]] <- plot_fe_vs_div(dd, div_col = m, out_prefix = out_prefix, title_prefix = title_pref, fe_col = "fe_value")
  }
  bind_rows(rows)
}

ts_msg("Plotting FE vs diversity (unbinned + binned-x=15) in", YEAR_PLOTS, "...")
tab_parent <- make_fe_div_plots(parent_stats,
                               fe_csv_path = file.path(DIRS$fe, "FE_parent.csv"),
                               id_col_name = FIRM_PARENT_VAR,
                               label_tag   = "parent")

tab_firm   <- make_fe_div_plots(firm_stats,
                               fe_csv_path = file.path(DIRS$fe, "FE_firm.csv"),
                               id_col_name = FIRM_RCID_VAR,
                               label_tag   = "firm")

write_csv(bind_rows(
  tab_parent %>% mutate(fe_def = "parent"),
  tab_firm   %>% mutate(fe_def = "firm")
), file.path(DIRS$fe_tables, paste0("FE_vs_div_slopes__", YEAR_PLOTS, ".csv")))

ts_msg("DONE. Outputs in:", OUT_DIR)
