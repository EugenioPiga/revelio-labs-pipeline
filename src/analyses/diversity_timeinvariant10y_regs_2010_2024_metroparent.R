##############################################################################
# TIME-INVARIANT (LAST-10Y) DIVERSITY + SIZE
#
# Main design:
#   - Compute cluster measures on FULL panel (incl. abroad years)
#   - THEN filter regression sample to US inventor-years only:
#       first_country == "United States"
#   - Run on three regression samples separately:
#       ALL / NATIVE / IMMIGRANT
#   - Run on two cluster definitions:
#       (i) PARENT
#       (ii) METRO_PARENT = metro x parent_rcid
#   - Run on two diversity metrics:
#       (i) 1 - HHI
#       (ii) Shannon entropy
#   - Controls:
#       base / imm / imm2
#   - Models:
#       PPML_inventorFE
#       PPML_noInventorFE
#       PPML_inventorFE_firmFE   [firm FE using first_rcid]
#       OLS_inventorFE
#       OLS_noInventorFE
#       OLS_inventorFE_firmFE    [firm FE using first_rcid]
#   - Always include year FE
#   - Run both UNWEIGHTED and WEIGHTED:
#       weights = cluster_size_10y
#
# Notes:
#   - No RDS saved
#   - Stage B completely removed
#   - Diversity is computed on the ENTIRE sample, then regressions are run
#     on US inventor-years only
#   - This version forces single-thread execution for stability on SLURM
##############################################################################

# =========================
# 0) Threading / stability
# =========================
Sys.setenv(
  OMP_NUM_THREADS = "1",
  OPENBLAS_NUM_THREADS = "1",
  MKL_NUM_THREADS = "1",
  VECLIB_MAXIMUM_THREADS = "1",
  NUMEXPR_NUM_THREADS = "1"
)

# =========================
# 1) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","tidyr","tibble")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(readr)
  library(stringr)
  library(data.table)
  library(fixest)
  library(tidyr)
  library(tibble)
})

set.seed(123)
data.table::setDTthreads(1)
if ("setFixest_nthreads" %in% getNamespaceExports("fixest")) {
  fixest::setFixest_nthreads(1)
}

# =========================
# 2) Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_timeinvariant10y_PARENT_AND_METROPARENT"

dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR   <- "immig_first_deg_or_job_nonUS"
PARENT_VAR  <- "first_parent_rcid"
FIRM_FE_VAR <- "first_rcid"

YEAR_START <- 2010
YEAR_END   <- 2024

US_COUNTRY <- "United States"

LAST10_N     <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END
YEARS_LAST10 <- LAST10_START:LAST10_END

MIN_CLUSTER <- 10
TENURE_MAX  <- 50

METRO_CANDIDATES <- c(
  "first_metro", "metro", "metro_name",
  "first_metro_area", "msa", "msa_name",
  "first_cbsa", "cbsa", "cbsa_name"
)

# =========================
# 3) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}

pick_first_existing <- function(candidates, available, what = "variable") {
  hit <- intersect(candidates, available)
  if (length(hit) == 0) {
    stop(paste0(
      "[ERROR] Could not find any candidate ", what, " columns. Tried: ",
      paste(candidates, collapse = ", ")
    ))
  }
  hit[1]
}

sig_stars <- function(p) {
  dplyr::case_when(
    !is.finite(p) ~ "",
    p < 0.01 ~ "***",
    p < 0.05 ~ "**",
    p < 0.1  ~ "*",
    TRUE ~ ""
  )
}

safe_name <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "X", x)
}

get_metric_dirs <- function(metric) {
  base <- file.path(OUT_DIR, toupper(metric))
  dirs <- list(
    base         = base,
    tables       = file.path(base, "tables"),
    cluster_meas = file.path(base, "cluster_measures_timeinvariant10y")
  )
  invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
  dirs
}

compute_diversity_stat <- function(p, method = c("hhi", "entropy")) {
  method <- match.arg(method)
  p <- p[is.finite(p) & p > 0]
  if (length(p) == 0) return(NA_real_)
  if (method == "hhi") {
    1 - sum(p^2)
  } else {
    -sum(p * log(p))
  }
}

extract_fixest_terms <- function(m, model_tag) {
  if (is.null(m)) {
    return(tibble(
      model = model_tag,
      term = "__MODEL_FAILED__",
      estimate = NA_real_,
      se = NA_real_,
      stat = NA_real_,
      p = NA_real_,
      stars = "",
      collinear_dropped = ""
    ))
  }

  ct <- tryCatch(fixest::coeftable(m), error = function(e) NULL)
  dropped <- if (!is.null(m$collin.var) && length(m$collin.var) > 0) {
    paste(m$collin.var, collapse = ";")
  } else {
    ""
  }

  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(
      model = model_tag,
      term = "__NO_ESTIMABLE_COEFFICIENTS__",
      estimate = NA_real_,
      se = NA_real_,
      stat = NA_real_,
      p = NA_real_,
      stars = "",
      collinear_dropped = dropped
    ))
  }

  tibble(
    model    = model_tag,
    term     = rownames(ct),
    estimate = as.numeric(ct[,1]),
    se       = as.numeric(ct[,2]),
    stat     = as.numeric(ct[,3]),
    p        = as.numeric(ct[,4]),
    stars    = sig_stars(as.numeric(ct[,4])),
    collinear_dropped = dropped
  )
}

run_one_model <- function(estimator, rhs, fe_part, data, wform = NULL) {
  fml <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_part))

  tryCatch({
    if (estimator == "ppml") {
      fixest::fepois(
        fml,
        data = data,
        vcov = ~cluster_id,
        weights = wform,
        notes = FALSE,
        warn = FALSE
      )
    } else if (estimator == "ols") {
      fixest::feols(
        fml,
        data = data,
        vcov = ~cluster_id,
        weights = wform,
        notes = FALSE,
        warn = FALSE
      )
    } else {
      stop("Unknown estimator.")
    }
  }, error = function(e) {
    ts_msg("MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

split_sample <- function(df, sample_tag) {
  if (sample_tag == "ALL") {
    df
  } else if (sample_tag == "NATIVE") {
    df %>% filter(.data[[IMMIG_VAR]] == 0L)
  } else if (sample_tag == "IMMIGRANT") {
    df %>% filter(.data[[IMMIG_VAR]] == 1L)
  } else {
    stop("Unknown sample_tag.")
  }
}

# =========================
# 4) Derived variables (row-level)
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

make_metro_parent <- function(df, metro_var) {
  df %>%
    mutate(
      metro_value = str_trim(as.character(.data[[metro_var]])),
      metro_value = ifelse(metro_value == "", NA_character_, metro_value),
      !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
      metro_parent_id = case_when(
        !is.na(.data[[PARENT_VAR]]) & .data[[PARENT_VAR]] != "" &
          !is.na(metro_value) & metro_value != "" ~
          paste0(.data[[PARENT_VAR]], "__", metro_value),
        TRUE ~ NA_character_
      )
    )
}

# =========================
# 5) Diversity: cluster-year, OVERALL only
# =========================
cluster_year_div_overall <- function(df, cluster_var, country_var, div_prefix,
                                     div_method = c("hhi", "entropy"),
                                     min_cluster = MIN_CLUSTER) {
  div_method <- match.arg(div_method)

  keep <- c("user_id", "year", IMMIG_VAR, cluster_var, country_var)
  dt <- as.data.table(df[, keep])

  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "country_raw")

  dt[, cluster_id := trimws(as.character(cluster_raw))]
  dt[, cluster_raw := NULL]

  dt[, country_raw := trimws(as.character(country_raw))]
  dt[country_raw == "" | country_raw == "empty", country_raw := NA_character_]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]

  dt <- dt[!is.na(cluster_id) & !is.na(year) & !is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]

  comp <- unique(dt[, .(user_id, year, cluster_id, imm, country_raw)])

  size_df <- comp[, .(
    cluster_size = uniqueN(user_id),
    immig_share  = mean(imm, na.rm = TRUE)
  ), by = .(cluster_id, year)]

  size_df <- size_df[cluster_size >= min_cluster]

  xdt <- comp[!is.na(country_raw)]
  if (nrow(xdt) == 0) {
    out <- copy(size_df)
    out[, paste0(div_prefix, "_div_all") := NA_real_]
    return(as_tibble(out))
  }

  xdt <- merge(
    xdt,
    size_df[, .(cluster_id, year)],
    by = c("cluster_id", "year"),
    all = FALSE
  )

  tmp <- xdt[, .(n = uniqueN(user_id)), by = .(cluster_id, year, v = country_raw)]
  tmp[, tot := sum(n), by = .(cluster_id, year)]
  tmp[, p := fifelse(tot > 0, n / tot, NA_real_)]

  div_df <- tmp[, .(
    div_value = compute_diversity_stat(p, method = div_method)
  ), by = .(cluster_id, year)]

  setnames(div_df, "div_value", paste0(div_prefix, "_div_all"))

  out <- merge(size_df, div_df, by = c("cluster_id", "year"), all.x = TRUE)
  as_tibble(out)
}

compute_cluster_bundle_overall <- function(df10, cluster_var, div_method) {
  ts_msg("Computing cluster-year diversity bundle for:", cluster_var, "| metric:", div_method)

  origin <- cluster_year_div_overall(df10, cluster_var, "origin_country", "origin", div_method, MIN_CLUSTER)

  job <- cluster_year_div_overall(df10, cluster_var, "first_pos_country", "job", div_method, MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("job_"))

  edu <- cluster_year_div_overall(df10, cluster_var, "first_university_country", "edu", div_method, MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("edu_"))

  origin %>%
    left_join(job, by = c("cluster_id", "year")) %>%
    left_join(edu, by = c("cluster_id", "year"))
}

make_timeinvariant_measures_overall <- function(df_all_fullpanel, cluster_var, label,
                                                div_method = c("hhi", "entropy"),
                                                dirs) {
  div_method <- match.arg(div_method)

  ts_msg("Building time-invariant measures for:", label, "| metric:", div_method, "| FULL PANEL")

  df10 <- df_all_fullpanel %>% filter(year %in% YEARS_LAST10)

  stats_y <- compute_cluster_bundle_overall(df10, cluster_var, div_method)

  write_csv(
    stats_y %>% mutate(cluster_level = label, diversity_metric = div_method),
    file.path(dirs$cluster_meas, paste0(label, "__cluster_year__", div_method, "__", LAST10_START, "_", LAST10_END, ".csv"))
  )

  stats_c <- stats_y %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      immig_share_10y  = mean(immig_share, na.rm = TRUE),
      origin_div_10y   = mean(origin_div_all, na.rm = TRUE),
      job_div_10y      = mean(job_div_all, na.rm = TRUE),
      edu_div_10y      = mean(edu_div_all, na.rm = TRUE),
      years_used_10y   = dplyr::n(),
      .groups = "drop"
    ) %>%
    mutate(
      cluster_level      = label,
      diversity_metric   = div_method,
      immig_share_sq_10y = immig_share_10y^2,
      log_size_10y       = ifelse(is.finite(cluster_size_10y) & cluster_size_10y > 0, log(cluster_size_10y), NA_real_)
    ) %>%
    filter(
      is.finite(cluster_size_10y), cluster_size_10y > 0,
      is.finite(immig_share_10y),
      is.finite(origin_div_10y),
      is.finite(job_div_10y),
      is.finite(edu_div_10y)
    )

  write_csv(
    stats_c,
    file.path(dirs$cluster_meas, paste0(label, "__cluster_const__", div_method, "__", LAST10_START, "_", LAST10_END, ".csv"))
  )

  stats_c
}

# =========================
# 6) Load Arrow + build FULL PANEL df
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")
ds_names <- names(ds)

METRO_VAR <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro")
ts_msg("Using metro variable:", METRO_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents",
  "first_country", "last_country",
  "first_university_country",
  "first_startdate_edu", "first_startdate_pos",
  IMMIG_VAR, PARENT_VAR, FIRM_FE_VAR, METRO_VAR
))

# first_pos_country from FULL panel BEFORE year filtering
ts_msg("Computing first_pos_country from FULL panel (before year filtering)...")
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

# collect FULL PANEL in regression window (NO US FILTER HERE)
ts_msg("Collecting FULL PANEL years:", YEAR_START, "to", YEAR_END, "(NO US FILTER)")
df_full <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(all_of(c(
    "user_id", "year", "n_patents", "first_country",
    "first_university_country", "first_startdate_edu", "first_startdate_pos",
    IMMIG_VAR, PARENT_VAR, FIRM_FE_VAR, METRO_VAR
  ))) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    first_university_country = str_trim(as.character(first_university_country)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!FIRM_FE_VAR := as.character(.data[[FIRM_FE_VAR]]),
    !!METRO_VAR := str_trim(as.character(.data[[METRO_VAR]]))
  ) %>%
  left_join(first_pos_tbl, by = "user_id") %>%
  make_origin_country() %>%
  compute_tenure() %>%
  make_metro_parent(METRO_VAR)

rm(ds, ds_pos, ds_first_year, first_pos_tbl)
gc()

# =========================
# 7) Regression runner
# =========================
run_models <- function(df_fullpanel, cluster_var, label, meas_const, div_method, dirs) {
  ts_msg("=== RUNNING:", label, "| metric:", div_method, "===")

  df_us <- df_fullpanel %>%
    filter(!is.na(first_country), first_country == US_COUNTRY)

  dd0 <- df_us %>%
    mutate(
      cluster_id = as.character(.data[[cluster_var]]),
      firm_fe_id = as.character(.data[[FIRM_FE_VAR]])
    ) %>%
    filter(
      !is.na(cluster_id), cluster_id != "",
      !is.na(user_id), user_id != "",
      !is.na(year), year >= YEAR_START, year <= YEAR_END,
      !is.na(n_patents), n_patents >= 0,
      is.finite(tenure), is.finite(tenure_sq)
    ) %>%
    left_join(meas_const, by = "cluster_id") %>%
    filter(
      is.finite(cluster_size_10y),
      cluster_size_10y > 0,
      is.finite(immig_share_10y),
      is.finite(origin_div_10y),
      is.finite(job_div_10y),
      is.finite(edu_div_10y)
    ) %>%
    mutate(
      user_id    = as.factor(user_id),
      year_fe    = as.factor(year),
      cluster_id = as.factor(cluster_id),
      firm_fe    = as.factor(firm_fe_id)
    )

  SPECS <- list(
    origin = "origin_div_10y",
    job    = "job_div_10y",
    edu    = "edu_div_10y"
  )

  CONTROL_SET <- list(
    base = function(div_var) paste0(div_var, " + cluster_size_10y + tenure + tenure_sq"),
    imm  = function(div_var) paste0(div_var, " + cluster_size_10y + tenure + tenure_sq + immig_share_10y"),
    imm2 = function(div_var) paste0(div_var, " + cluster_size_10y + tenure + tenure_sq + immig_share_10y + immig_share_sq_10y")
  )

  WEIGHTS <- list(
    unweighted = NULL,
    weighted   = ~cluster_size_10y
  )

  MODELS <- list(
    list(model_name = "PPML_inventorFE",        estimator = "ppml", fe = "user_id + year_fe"),
    list(model_name = "PPML_noInventorFE",      estimator = "ppml", fe = "year_fe"),
    list(model_name = "PPML_inventorFE_firmFE", estimator = "ppml", fe = "user_id + year_fe + firm_fe"),
    list(model_name = "OLS_inventorFE",         estimator = "ols",  fe = "user_id + year_fe"),
    list(model_name = "OLS_noInventorFE",       estimator = "ols",  fe = "year_fe"),
    list(model_name = "OLS_inventorFE_firmFE",  estimator = "ols",  fe = "user_id + year_fe + firm_fe")
  )

  SAMPLE_TAGS <- c("ALL", "NATIVE", "IMMIGRANT")

  all_tabs <- list()
  idx <- 0L

  for (sample_tag in SAMPLE_TAGS) {
    dsample0 <- split_sample(dd0, sample_tag)
    ts_msg("Sample:", sample_tag, "| N =", nrow(dsample0))
    if (nrow(dsample0) == 0) next

    for (spec_nm in names(SPECS)) {
      div_var <- SPECS[[spec_nm]]

      dspec <- dsample0 %>%
        filter(
          is.finite(.data[[div_var]]),
          is.finite(cluster_size_10y),
          is.finite(immig_share_10y)
        )

      ts_msg("  Spec:", spec_nm, "| sample:", sample_tag, "| N =", nrow(dspec))
      if (nrow(dspec) == 0) next

      for (ctrl_tag in names(CONTROL_SET)) {
        rhs <- CONTROL_SET[[ctrl_tag]](div_var)

        for (wtag in names(WEIGHTS)) {
          wform <- WEIGHTS[[wtag]]
          ts_msg("    ->", spec_nm, "|", ctrl_tag, "|", wtag, "|", sample_tag, "| N =", nrow(dspec))

          model_tabs <- lapply(MODELS, function(mm) {
            fit <- run_one_model(
              estimator = mm$estimator,
              rhs = rhs,
              fe_part = mm$fe,
              data = dspec,
              wform = wform
            )

            extract_fixest_terms(
              fit,
              paste0(label, "__", div_method, "__", sample_tag, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__", mm$model_name)
            ) %>%
              mutate(
                cluster_level = label,
                diversity_metric = div_method,
                sample = sample_tag,
                spec = spec_nm,
                controls = ctrl_tag,
                weight = wtag,
                n_obs = nrow(dspec),
                n_clusters = dplyr::n_distinct(dspec$cluster_id)
              )
          })

          idx <- idx + 1L
          all_tabs[[idx]] <- bind_rows(model_tabs)
          gc(FALSE)
        }
      }
    }
  }

  tab_all <- if (length(all_tabs) > 0) bind_rows(all_tabs) else tibble()

  write_csv(
    tab_all,
    file.path(dirs$tables, paste0(label, "__", div_method, "__coef_table_ALL.csv"))
  )

  rm(dd0, df_us)
  gc()

  invisible(tab_all)
}

# =========================
# 8) Build measures + run everything
# =========================
for (div_method in c("hhi")) {

  dirs_metric <- get_metric_dirs(div_method)
  ts_msg("============================================================")
  ts_msg("STARTING METRIC:", toupper(div_method))
  ts_msg("============================================================")

  meas_parent <- make_timeinvariant_measures_overall(
    df_all_fullpanel = df_full,
    cluster_var = PARENT_VAR,
    label = "PARENT",
    div_method = div_method,
    dirs = dirs_metric
  )

  meas_metro_parent <- make_timeinvariant_measures_overall(
    df_all_fullpanel = df_full,
    cluster_var = "metro_parent_id",
    label = "METRO_PARENT",
    div_method = div_method,
    dirs = dirs_metric
  )

  ts_msg("Running regressions for PARENT | metric:", div_method)
  out_parent <- run_models(
    df_fullpanel = df_full,
    cluster_var = PARENT_VAR,
    label = "PARENT",
    meas_const = meas_parent,
    div_method = div_method,
    dirs = dirs_metric
  )

  ts_msg("Running regressions for METRO_PARENT | metric:", div_method)
  out_metro_parent <- run_models(
    df_fullpanel = df_full,
    cluster_var = "metro_parent_id",
    label = "METRO_PARENT",
    meas_const = meas_metro_parent,
    div_method = div_method,
    dirs = dirs_metric
  )

  write_csv(
    out_parent,
    file.path(dirs_metric$tables, paste0("PARENT__", div_method, "__coef_table.csv"))
  )
  write_csv(
    out_metro_parent,
    file.path(dirs_metric$tables, paste0("METRO_PARENT__", div_method, "__coef_table.csv"))
  )
  write_csv(
    bind_rows(out_parent, out_metro_parent),
    file.path(dirs_metric$tables, paste0("ALL_CLUSTER_DEFS__", div_method, "__coef_table.csv"))
  )

  rm(meas_parent, meas_metro_parent, out_parent, out_metro_parent)
  gc()
  ts_msg("DONE WITH METRIC:", toupper(div_method))
}

ts_msg("ALL DONE. Outputs in:", OUT_DIR)
