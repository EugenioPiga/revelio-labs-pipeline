############################################################################
# DIVERSITY (ORIGIN + JOB + EDUCATION) — PARENT RCID + FIRM RCID + METRO
#
# Adds (per your request):
#   (A) Firm FE vs Firm Diversity plot: UNBINNED + BINNED (15 bins on diversity)
#       - Points in binned plot are weighted means of firm_fe (weights = cluster_size)
#       - Regression line is from UNBINNED WLS (weights = cluster_size)
#   (B) "Firm variant" = run the SAME pipeline you run for first_parent_rcid
#       also for first_rcid.
#
# IMPORTANT: No PPML estimation is run in this script.
#            We only *read* FE CSVs produced elsewhere.
############################################################################

# =========================
# 0) Packages
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","ggplot2","tidyr","scales","tibble")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(ggplot2); library(tidyr); library(scales); library(tibble)
})

set.seed(123)

# =========================
# 1) Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_origin_job_edu_parent_and_rcid"

TABLES_DIR <- file.path(OUT_DIR, "tables")
MODELS_DIR <- file.path(OUT_DIR, "models_ols")        # OLS only here
PLOTS_DIR  <- file.path(OUT_DIR, "plots_2018")
PLOTS_FE_DIR  <- file.path(OUT_DIR, "plots_firmFE_vs_div_2018")
TABLES_FE_DIR <- file.path(OUT_DIR, "tables_firmFE_vs_div_2018")

dir.create(TABLES_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(MODELS_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(PLOTS_DIR,  recursive = TRUE, showWarnings = FALSE)
dir.create(PLOTS_FE_DIR,  recursive = TRUE, showWarnings = FALSE)
dir.create(TABLES_FE_DIR, recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
PARENT_VAR <- "first_parent_rcid"
RCID_VAR   <- "first_rcid"
METRO_VAR  <- "first_metro_area"

USE_LAST_N_YEARS <- 8
SKIP_YEARS <- c(2019,2020,2021,2022,2023,2024,2025)
years_vec  <- 2017:2018

MIN_CLUSTER <- 10
TENURE_MAX  <- 50

YEAR_PLOTS <- 2018
N_BINS_X   <- 15

# ---- FE files ----
FE_PARENT_PATH <- "/home/epiga/revelio_labs/output/regressions/ppml_loop_runs/fe/metro_parent/fe_metro_parent_first_parent_rcid.csv"
FE_RCID_PATH   <- "/home/epiga/revelio_labs/output/regressions/ppml_loop_runs/fe/metro_rcid/fe_metro_rcid_first_rcid.csv"

# =========================
# 2) Helpers: validation + construction
# =========================
need_cols <- function(obj, cols) {
  nm <- names(obj)
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse=", ")))
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
# 3) Helpers: diversity stats (1 - HHI)
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

compute_stats_bundle <- function(df, cluster_var, level_label) {
  origin <- cluster_year_div_stats_dt(df, cluster_var, level_label, "origin_country", "origin_div", MIN_CLUSTER)
  job    <- cluster_year_div_stats_dt(df, cluster_var, level_label, "first_pos_country", "job_div", MIN_CLUSTER)
  edu    <- cluster_year_div_stats_dt(df, cluster_var, level_label, "first_university_country", "edu_div", MIN_CLUSTER)

  origin %>%
    left_join(job %>% select(cluster_id, year, job_div), by = c("cluster_id","year")) %>%
    left_join(edu %>% select(cluster_id, year, edu_div), by = c("cluster_id","year"))
}

# =========================
# 4) Helpers: OLS regressions (fixest)
# =========================
extract_terms_fixest <- function(fit, terms_wanted) {
  ct <- fixest::coeftable(fit)
  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(term = terms_wanted, estimate = NA_real_, se = NA_real_, t_stat = NA_real_, p_value = NA_real_))
  }
  rn <- rownames(ct)
  bind_rows(lapply(terms_wanted, function(tt) {
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
  }))
}

run_div_regressions_ols <- function(d, prefix, level_label, out_dir,
                                    div_suffix, country_fe_var) {

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
    full        = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share, " + tenure + tenure_sq | year + ", country_fe_var))
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
    if (nm %in% c("size_cfe","size_sh_cfe","full")) dd <- dd %>% filter(!is.na(.data[[country_fe_var]]), .data[[country_fe_var]] != "")
    if (nm == "full") dd <- dd %>% filter(!is.na(year))

    if (nrow(dd) < 1000) stop(paste0("[ERROR] Too few rows for ", level_label, " / ", nm, ": ", nrow(dd)))

    fit <- fixest::feols(f, data = dd, vcov = as.formula(paste0("~", clid)), warn = FALSE)

    capture.output(summary(fit),
                   file = file.path(out_dir, paste0("OLS__", level_label, "__", div_suffix, "__", nm, ".txt")))

    rows_all[[nm]] <- extract_terms_fixest(fit, terms_wanted) %>%
      mutate(cluster_level = level_label,
             diversity_measure = div_suffix,
             spec = nm,
             n_obs = nobs(fit)) %>%
      select(cluster_level, diversity_measure, spec, n_obs, term, estimate, se, t_stat, p_value)
  }

  out <- bind_rows(rows_all)
  write_csv(out, file.path(out_dir, paste0("OLS__", level_label, "__", div_suffix, "__ALLTERMS.csv")))
  out
}

run_div_regressions_one_year_ols <- function(d, prefix, level_label, out_dir,
                                             div_suffix, country_fe_var) {

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
    full        = as.formula(paste0("n_patents ~ ", div, " + ", logsz, " + ", share, " + tenure + tenure_sq | ", country_fe_var))
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
    if (nm %in% c("size_cfe","size_sh_cfe","full")) dd <- dd %>% filter(!is.na(.data[[country_fe_var]]), .data[[country_fe_var]] != "")

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

    fit <- fixest::feols(f, data = dd, vcov = as.formula(paste0("~", clid)), warn = FALSE)

    rows_all[[nm]] <- extract_terms_fixest(fit, terms_wanted) %>%
      mutate(cluster_level = level_label,
             diversity_measure = div_suffix,
             spec = nm,
             n_obs = nobs(fit)) %>%
      select(cluster_level, diversity_measure, spec, n_obs, term, estimate, se, t_stat, p_value)
  }

  bind_rows(rows_all)
}

# =========================
# 5) Helpers: 2018 plots (hist + bin-scatter)
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

weighted_slope_se <- function(df, x, y, w) {
  df2 <- df %>% filter(is.finite(.data[[x]]), is.finite(.data[[y]]), is.finite(.data[[w]]), .data[[w]] > 0)
  if (nrow(df2) < 5) return(list(beta = NA_real_, se = NA_real_, n = nrow(df2), a0 = NA_real_))
  fit <- lm(reformulate(x, y), data = df2, weights = df2[[w]])
  co <- summary(fit)$coefficients
  if (!(x %in% rownames(co))) return(list(beta = NA_real_, se = NA_real_, n = nrow(df2), a0 = NA_real_))
  list(
    beta = unname(co[x, "Estimate"]),
    se   = unname(co[x, "Std. Error"]),
    n    = nrow(df2),
    a0   = unname(co["(Intercept)", "Estimate"])
  )
}

make_binscatter_plot <- function(df, x, y, w, title, xlab, ylab, file_out) {
  pts <- bin_weighted_points(df, x = x, y = y, w = w, n_bins = N_BINS_X)
  ws  <- weighted_slope_se(df, x = x, y = y, w = w)

  note <- if (is.finite(ws$beta) && is.finite(ws$se)) {
    paste0("Unbinned weighted slope:\n",
           "beta = ", sprintf("%.4f", ws$beta), "\n",
           "se   = ", sprintf("%.4f", ws$se), "\n",
           "N = ", ws$n)
  } else "Unbinned weighted slope: NA"

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

make_2018_outputs <- function(stats_df, level_name) {
  df18 <- stats_df %>% filter(year == YEAR_PLOTS)
  if (nrow(df18) == 0) return(invisible(NULL))

  summ <- bind_rows(
    div_summary_stats(df18, "origin_div") %>% mutate(measure = "origin_div"),
    div_summary_stats(df18, "job_div")    %>% mutate(measure = "job_div"),
    div_summary_stats(df18, "edu_div")    %>% mutate(measure = "edu_div")
  ) %>% select(measure, everything())

  write_csv(summ, file.path(TABLES_DIR, paste0("dist_summary_", level_name, "_", YEAR_PLOTS, ".csv")))

  plot_div_hist(df18, "origin_div",
                paste0(level_name, " ", YEAR_PLOTS, ": origin_div distribution"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_origin_div.png")))
  plot_div_hist(df18, "job_div",
                paste0(level_name, " ", YEAR_PLOTS, ": job_div distribution"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_job_div.png")))
  plot_div_hist(df18, "edu_div",
                paste0(level_name, " ", YEAR_PLOTS, ": edu_div distribution"),
                file.path(PLOTS_DIR, paste0("hist_", level_name, "_", YEAR_PLOTS, "_edu_div.png")))

  for (dv in c("origin_div","job_div","edu_div")) {
    make_binscatter_plot(
      df = df18, x = "log_cluster", y = dv, w = "cluster_size",
      title = paste0(level_name, " ", YEAR_PLOTS, ": ", dv, " vs log(cluster size) (", N_BINS_X, " bins; w=size)"),
      xlab = "log(cluster size)", ylab = dv,
      file_out = file.path(PLOTS_DIR, paste0("binscatter_", level_name, "_", YEAR_PLOTS, "_", dv, "_vs_logsize.png"))
    )
    make_binscatter_plot(
      df = df18, x = "immig_share", y = dv, w = "cluster_size",
      title = paste0(level_name, " ", YEAR_PLOTS, ": ", dv, " vs immigrant share (", N_BINS_X, " bins; w=size)"),
      xlab = "immigrant share", ylab = dv,
      file_out = file.path(PLOTS_DIR, paste0("binscatter_", level_name, "_", YEAR_PLOTS, "_", dv, "_vs_share.png"))
    )
  }
}

# =========================
# 6) Helpers: Firm FE vs diversity (UNBINNED + BINNED on diversity)
# =========================
read_fe_generic <- function(path) {
  fe_raw <- data.table::fread(path)

  # common column names used by your FE exports:
  lvl_candidates <- intersect(names(fe_raw), c("level","Level","id","ID","fe_level"))
  fe_candidates  <- intersect(names(fe_raw), c("fe","FE","effect","estimate","Estimate"))

  if (length(lvl_candidates) == 0) stop(paste0("[ERROR] Can't find FE level column in: ", path, "\nCols: ", paste(names(fe_raw), collapse=", ")))
  if (length(fe_candidates)  == 0) stop(paste0("[ERROR] Can't find FE value column in: ", path, "\nCols: ", paste(names(fe_raw), collapse=", ")))

  lvl_col <- lvl_candidates[1]
  fe_col  <- fe_candidates[1]

  fe_raw %>%
    transmute(cluster_id = as.character(.data[[lvl_col]]),
              firm_fe    = as.numeric(.data[[fe_col]])) %>%
    filter(!is.na(cluster_id), cluster_id != "", is.finite(firm_fe))
}

sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}

fmt_bse <- function(b, se) {
  if (!is.finite(b) || !is.finite(se)) return("NA")
  paste0(sprintf("%.3f", b), " (", sprintf("%.3f", se), ")")
}

wls_term <- function(df, fmla, term) {
  fit <- lm(fmla, data = df, weights = cluster_size)
  co  <- summary(fit)$coefficients
  if (!(term %in% rownames(co))) {
    return(tibble(beta=NA_real_, se=NA_real_, t=NA_real_, p=NA_real_, n=nrow(df), a0=NA_real_))
  }
  tibble(
    beta = unname(co[term, "Estimate"]),
    se   = unname(co[term, "Std. Error"]),
    t    = unname(co[term, "t value"]),
    p    = unname(co[term, "Pr(>|t|)"]),
    n    = nrow(df),
    a0   = unname(co["(Intercept)", "Estimate"])
  )
}

run_firmfe_vs_div_2018 <- function(stats_df, fe_path, label_for_outputs) {
  if (!file.exists(fe_path)) stop(paste0("[ERROR] Missing FE file: ", fe_path))
  fe_df <- read_fe_generic(fe_path)

  dd0 <- stats_df %>%
    filter(year == YEAR_PLOTS) %>%
    mutate(cluster_id = as.character(cluster_id)) %>%
    left_join(fe_df, by = "cluster_id") %>%
    filter(is.finite(firm_fe),
           is.finite(cluster_size), cluster_size > 0,
           is.finite(log_cluster),
           is.finite(immig_share))

  measures <- c("origin_div","job_div","edu_div")
  rows <- list()

  for (m in measures) {
    dd <- dd0 %>% filter(is.finite(.data[[m]]))
    if (nrow(dd) < 50) next

    raw  <- wls_term(dd, as.formula(paste0("firm_fe ~ ", m)), m) %>%
      mutate(measure = m, spec = "raw", stars = sig_stars(p))
    ctrl <- wls_term(dd, as.formula(paste0("firm_fe ~ ", m, " + log_cluster + immig_share")), m) %>%
      mutate(measure = m, spec = "controls", stars = sig_stars(p))

    rows[[paste0(m,"_raw")]]  <- raw
    rows[[paste0(m,"_ctrl")]] <- ctrl

    note <- paste0(
      "WLS (w=cluster_size)\n",
      "Raw:   β=", fmt_bse(raw$beta, raw$se), " ", raw$stars, "\n",
      "Ctrls: β=", fmt_bse(ctrl$beta, ctrl$se), " ", ctrl$stars
    )

    # ---- UNBINNED scatter + unbinned WLS line (raw) ----
    p_unb <- ggplot(dd, aes(x = .data[[m]], y = firm_fe)) +
      geom_point(aes(size = cluster_size), alpha = 0.18) +
      geom_abline(intercept = raw$a0, slope = raw$beta, linewidth = 0.9) +
      scale_size_continuous(labels = comma, guide = "none") +
      labs(
        title = paste0(YEAR_PLOTS, " Firm FE vs ", m, " — ", label_for_outputs, " (UNBINNED)"),
        x = m,
        y = "Firm fixed effect (from FE csv)"
      ) +
      annotate("text", x = Inf, y = -Inf, label = note, hjust = 1.02, vjust = -0.2, size = 3.3) +
      theme_minimal(base_size = 12)

    ggsave(
      filename = file.path(PLOTS_FE_DIR, paste0("FIRMFE_vs_", m, "__", label_for_outputs, "__", YEAR_PLOTS, "__unbinned.png")),
      plot = p_unb, width = 8.5, height = 5.8, dpi = 180
    )

    # ---- BINNED on DIVERSITY: x = diversity bins; y = weighted mean firm_fe ----
    pts <- bin_weighted_points(dd, x = m, y = "firm_fe", w = "cluster_size", n_bins = N_BINS_X)

    p_bin <- ggplot() +
      geom_point(data = pts, aes(x = x_mid, y = y_bin), size = 2.2) +
      geom_abline(intercept = raw$a0, slope = raw$beta, linewidth = 0.9) +
      labs(
        title = paste0(YEAR_PLOTS, " Firm FE vs ", m, " — ", label_for_outputs, " (", N_BINS_X, " bins on diversity)"),
        x = m,
        y = "Binned firm FE (weighted mean; w=size)"
      ) +
      annotate("text", x = Inf, y = -Inf, label = note, hjust = 1.02, vjust = -0.2, size = 3.3) +
      theme_minimal(base_size = 12)

    ggsave(
      filename = file.path(PLOTS_FE_DIR, paste0("FIRMFE_vs_", m, "__", label_for_outputs, "__", YEAR_PLOTS, "__binned.png")),
      plot = p_bin, width = 8.5, height = 5.8, dpi = 180
    )
  }

  if (length(rows) == 0) {
    warning(paste0("[WARN] No FE-vs-div plots created for ", label_for_outputs, " (likely too few matched clusters)."))
    res_empty <- tibble(measure=character(), spec=character(), n=integer(), beta=double(), se=double(), t=double(), p=double(), stars=character())
    data.table::fwrite(res_empty, file.path(TABLES_FE_DIR, paste0("FIRMFE_corr_diversity__", label_for_outputs, "__", YEAR_PLOTS, ".csv")))
    return(res_empty)
  }

  res <- bind_rows(rows) %>%
    select(measure, spec, n, beta, se, t, p, stars) %>%
    arrange(measure, spec)

  data.table::fwrite(res, file.path(TABLES_FE_DIR, paste0("FIRMFE_corr_diversity__", label_for_outputs, "__", YEAR_PLOTS, ".csv")))
  res
}

# =========================
# 7) Load Arrow dataset + build working df
# =========================
cat("\n[INFO] Opening Arrow dataset...\n")
ds <- open_dataset(INPUT, format = "parquet")

need_cols(ds, c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR,
  PARENT_VAR, RCID_VAR, METRO_VAR
))

years_all <- ds %>% select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
years_to_run <- tail(years_all, USE_LAST_N_YEARS)
years_to_run <- setdiff(years_to_run, SKIP_YEARS)
cat("[INFO] Running years:", paste(years_to_run, collapse=", "), "\n")

ds_y <- ds %>% filter(year %in% years_to_run)

cat("[INFO] Building first_pos_country map...\n")
first_pos_map <- build_first_pos_country_map(ds_y)

cat("[INFO] Collecting working columns to memory...\n")
df <- ds_y %>%
  select(
    user_id, year, n_patents,
    first_country, last_country,
    first_university_country,
    first_startdate_edu, first_startdate_pos,
    !!sym(IMMIG_VAR),
    !!sym(PARENT_VAR), !!sym(RCID_VAR), !!sym(METRO_VAR)
  ) %>%
  collect() %>%
  mutate(
    !!IMMIG_VAR  := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!RCID_VAR   := as.character(.data[[RCID_VAR]]),
    !!METRO_VAR  := as.character(.data[[METRO_VAR]]),
    n_patents = as.numeric(n_patents),
    first_university_country = str_trim(as.character(first_university_country))
  ) %>%
  left_join(first_pos_map, by = "user_id") %>%
  mutate(first_pos_country = str_trim(as.character(first_pos_country))) %>%
  compute_tenure() %>%
  filter(!is.na(tenure)) %>%
  make_origin_country()

# =========================
# 8) Compute diversity stats: parent-firm, rcid-firm, metro
# =========================
cat("[INFO] Computing PARENT-year diversity + controls...\n")
parent_stats <- compute_stats_bundle(df, PARENT_VAR, "PARENT_year")
write_csv(parent_stats, file.path(TABLES_DIR, "parent_year_diversity_origin_job_edu.csv"))

cat("[INFO] Computing RCID-year diversity + controls...\n")
rcid_stats <- compute_stats_bundle(df, RCID_VAR, "RCID_year")
write_csv(rcid_stats, file.path(TABLES_DIR, "rcid_year_diversity_origin_job_edu.csv"))

cat("[INFO] Computing METRO-year diversity + controls...\n")
metro_stats <- compute_stats_bundle(df, METRO_VAR, "METRO_year")
write_csv(metro_stats, file.path(TABLES_DIR, "metro_year_diversity_origin_job_edu.csv"))

# =========================
# 9) 2018 distributions + bin-scatters (same outputs as before)
# =========================
cat("[INFO] Building 2018 distribution + bin-scatter plots...\n")
make_2018_outputs(parent_stats, "PARENT_year")
make_2018_outputs(rcid_stats,   "RCID_year")
make_2018_outputs(metro_stats,  "METRO_year")

# =========================
# 10) Firm FE vs diversity (2018): UNBINNED + BINNED (15 bins on diversity)
# =========================
cat("[INFO] Firm FE vs PARENT diversity (2018)...\n")
res_fe_parent_2018 <- run_firmfe_vs_div_2018(parent_stats, FE_PARENT_PATH, "PARENT")
print(res_fe_parent_2018)

cat("[INFO] Firm FE vs RCID diversity (2018)...\n")
res_fe_rcid_2018 <- run_firmfe_vs_div_2018(rcid_stats, FE_RCID_PATH, "RCID")
print(res_fe_rcid_2018)

# =========================
# 11) Merge stats back to inventor-year (prefixes: parent_, rcid_, metro_)
# =========================
df_reg <- df %>%
  mutate(
    parent_cluster_id = as.character(.data[[PARENT_VAR]]),
    rcid_cluster_id   = as.character(.data[[RCID_VAR]]),
    metro_cluster_id  = as.character(.data[[METRO_VAR]]),
    origin_country = str_trim(as.character(origin_country)),
    first_pos_country = str_trim(as.character(first_pos_country)),
    first_university_country = str_trim(as.character(first_university_country))
  )

# attach parent stats
df_reg <- df_reg %>%
  mutate(cluster_id = parent_cluster_id) %>%
  left_join(
    parent_stats %>%
      transmute(cluster_id, year,
                parent_origin_div  = origin_div,
                parent_job_div     = job_div,
                parent_edu_div     = edu_div,
                parent_log_size    = log_cluster,
                parent_immig_share = immig_share),
    by = c("cluster_id","year")
  ) %>% select(-cluster_id)

# attach rcid stats
df_reg <- df_reg %>%
  mutate(cluster_id = rcid_cluster_id) %>%
  left_join(
    rcid_stats %>%
      transmute(cluster_id, year,
                rcid_origin_div  = origin_div,
                rcid_job_div     = job_div,
                rcid_edu_div     = edu_div,
                rcid_log_size    = log_cluster,
                rcid_immig_share = immig_share),
    by = c("cluster_id","year")
  ) %>% select(-cluster_id)

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
  ) %>% select(-cluster_id)

write_csv(
  df_reg %>%
    select(user_id, year, n_patents, tenure, tenure_sq,
           origin_country, first_pos_country, first_university_country,
           parent_cluster_id, parent_origin_div, parent_job_div, parent_edu_div, parent_log_size, parent_immig_share,
           rcid_cluster_id,   rcid_origin_div,   rcid_job_div,   rcid_edu_div,   rcid_log_size,   rcid_immig_share,
           metro_cluster_id,  metro_origin_div,  metro_job_div,  metro_edu_div,  metro_log_size,  metro_immig_share) %>%
    sample_n(min(200000, nrow(df_reg))),
  file.path(TABLES_DIR, "sample_inventor_year_with_div_controls.csv")
)

# =========================
# 12) Run pooled OLS regressions (parent + rcid + metro)
# =========================
cat("[INFO] Running pooled OLS regressions...\n")
pooled_results <- list()

run_block <- function(prefix, label_base, div_suf, dsub) {
  cfe_var <- switch(div_suf,
                    origin_div = "origin_country",
                    job_div    = "first_pos_country",
                    edu_div    = "first_university_country")
  lvl <- paste0(label_base, "_", sub("_div","", div_suf))
  run_div_regressions_ols(
    d = dsub, prefix = prefix, level_label = lvl, out_dir = MODELS_DIR,
    div_suffix = div_suf, country_fe_var = cfe_var
  )
}

# parent
for (div_suf in c("origin_div","job_div","edu_div")) {
  dsub <- df_reg %>% filter(!is.na(.data[[paste0("parent_", div_suf)]]))
  pooled_results[[paste0("PARENT__",div_suf)]] <- run_block("parent","PARENT_year",div_suf,dsub)
}

# rcid
for (div_suf in c("origin_div","job_div","edu_div")) {
  dsub <- df_reg %>% filter(!is.na(.data[[paste0("rcid_", div_suf)]]))
  pooled_results[[paste0("RCID__",div_suf)]] <- run_block("rcid","RCID_year",div_suf,dsub)
}

# metro
for (div_suf in c("origin_div","job_div","edu_div")) {
  dsub <- df_reg %>% filter(!is.na(.data[[paste0("metro_", div_suf)]]))
  pooled_results[[paste0("METRO__",div_suf)]] <- run_block("metro","METRO_year",div_suf,dsub)
}

res_pooled_all <- bind_rows(pooled_results, .id = "run_id")
write_csv(res_pooled_all, file.path(MODELS_DIR, "POOLED_ALLTERMS__parent_rcid_metro__origin_job_edu__OLS.csv"))

# =========================
# 13) Year-by-year OLS regressions
# =========================
cat("[INFO] Running YEAR-BY-YEAR OLS regressions...\n")
all_year_results <- list()

for (yy in years_vec) {
  cat("[INFO]  Year ", yy, "\n")
  dyy <- df_reg %>% filter(year == yy)
  one_year_runs <- list()

  for (div_suf in c("origin_div","job_div","edu_div")) {
    cfe_var <- switch(div_suf,
                      origin_div = "origin_country",
                      job_div    = "first_pos_country",
                      edu_div    = "first_university_country")

    # parent
    lvl <- paste0("PARENT_year_", sub("_div","", div_suf))
    r_parent <- run_div_regressions_one_year_ols(dyy, "parent", lvl, MODELS_DIR, div_suf, cfe_var) %>% mutate(year=yy)
    one_year_runs[[paste0("PARENT__",div_suf)]] <- r_parent

    # rcid
    lvl <- paste0("RCID_year_", sub("_div","", div_suf))
    r_rcid <- run_div_regressions_one_year_ols(dyy, "rcid", lvl, MODELS_DIR, div_suf, cfe_var) %>% mutate(year=yy)
    one_year_runs[[paste0("RCID__",div_suf)]] <- r_rcid

    # metro
    lvl <- paste0("METRO_year_", sub("_div","", div_suf))
    r_metro <- run_div_regressions_one_year_ols(dyy, "metro", lvl, MODELS_DIR, div_suf, cfe_var) %>% mutate(year=yy)
    one_year_runs[[paste0("METRO__",div_suf)]] <- r_metro
  }

  all_year_results[[as.character(yy)]] <- bind_rows(one_year_runs, .id = "run_id")
}

res_by_year <- bind_rows(all_year_results)
write_csv(res_by_year, file.path(MODELS_DIR, "BYYEAR_ALLTERMS__parent_rcid_metro__origin_job_edu__OLS.csv"))

cat("\n[DONE]\nOutputs in:\n", OUT_DIR, "\n")
cat(" - tables: ", TABLES_DIR, "\n")
cat(" - models: ", MODELS_DIR, "\n")
cat(" - 2018 plots: ", PLOTS_DIR, "\n")
cat(" - FE plots: ", PLOTS_FE_DIR, "\n")
cat(" - FE tables: ", TABLES_FE_DIR, "\n")
