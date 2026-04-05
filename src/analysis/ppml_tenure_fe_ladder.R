#!/usr/bin/env Rscript

##############################################################################
# PPML FE ladder for tenure coefficients
#
# Goal:
#   - Track how tenure coefficients change as we add FE controls.
#   - Run two tenure parameterizations:
#       (i) quadratic: tenure + tenure_sq
#       (ii) bins: 0-5, 6-10, 11-15, ...
#   - Print / save all NON-FE coefficients only.
#   - Focus on PARENT and METRO_PARENT cluster definitions.
#
# FE ladder for each study:
#   1) user FE + year FE
#   2) user FE + year FE + metro FE
#   3) user FE + year FE + study FE
#   4) user FE + year FE + metro FE + study FE
#
# Important note:
#   - cluster_size_10y is time-invariant at the study-cluster level.
#   - Therefore, once study FE are added, cluster_size_10y will generally be
#     collinear and dropped. This is expected and recorded in the outputs.
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

pkgs <- c("arrow", "dplyr", "readr", "stringr", "data.table", "fixest", "tidyr", "tibble")
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
OUT_DIR <- "/home/epiga/revelio_labs/output/ppml_tenure_fe_ladder"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

PARENT_VAR <- "first_parent_rcid"
YEAR_START <- 2010
YEAR_END   <- 2019
US_COUNTRY <- "United States"

LAST10_N     <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END
YEARS_LAST10 <- LAST10_START:LAST10_END

TENURE_MAX      <- 50
TENURE_BIN_STEP <- 5
MIN_CLUSTER     <- 10

METRO_CANDIDATES <- c(
  "first_metro", "metro", "metro_name",
  "first_metro_area", "msa", "msa_name",
  "first_cbsa", "cbsa", "cbsa_name"
)

# Optional: keep only unweighted now. Easy to extend later.
WEIGHTS <- list(
  unweighted = NULL
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

get_dirs <- function() {
  dirs <- list(
    root   = OUT_DIR,
    tables = file.path(OUT_DIR, "tables"),
    logs   = file.path(OUT_DIR, "logs"),
    sizes  = file.path(OUT_DIR, "cluster_size_timeinvariant10y")
  )
  invisible(lapply(dirs, dir.create, recursive = TRUE, showWarnings = FALSE))
  dirs
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
    estimate = as.numeric(ct[, 1]),
    se       = as.numeric(ct[, 2]),
    stat     = as.numeric(ct[, 3]),
    p        = as.numeric(ct[, 4]),
    stars    = sig_stars(as.numeric(ct[, 4])),
    collinear_dropped = dropped
  )
}

run_ppml <- function(rhs, fe_part, data, wform = NULL) {
  fml <- as.formula(paste0("n_patents ~ ", rhs, " | ", fe_part))

  tryCatch({
    fixest::fepois(
      fml,
      data = data,
      vcov = ~cluster_id,
      weights = wform,
      notes = FALSE,
      warn = FALSE
    )
  }, error = function(e) {
    ts_msg("MODEL FAILED:", deparse(fml), "|", conditionMessage(e))
    NULL
  })
}

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

make_tenure_bins <- function(x, max_tenure = TENURE_MAX, step = TENURE_BIN_STEP) {
  x <- suppressWarnings(as.numeric(x))
  breaks <- c(-1, seq(step, max_tenure, by = step))
  lowers <- seq(0, max_tenure - step, by = step) + c(0, rep(1, length(seq(step, max_tenure, by = step)) - 1))
  uppers <- seq(step, max_tenure, by = step)
  labels <- ifelse(lowers == 0,
                   paste0("0-", step),
                   paste0(lowers, "-", uppers))
  cut(x, breaks = breaks, labels = labels, include.lowest = TRUE, right = TRUE)
}

# Size measure computed on FULL PANEL (including abroad years), then merged into US sample.
make_timeinvariant_size_overall <- function(df_all_fullpanel, cluster_var, label, dirs) {
  ts_msg("Building time-invariant size for:", label, "| FULL PANEL")

  keep <- c("user_id", "year", cluster_var)
  dt <- as.data.table(df_all_fullpanel[, keep])
  setnames(dt, cluster_var, "cluster_raw")

  dt[, cluster_id := trimws(as.character(cluster_raw))]
  dt[, cluster_raw := NULL]
  dt[cluster_id == "" | cluster_id == "NA", cluster_id := NA_character_]
  dt <- dt[!is.na(cluster_id) & !is.na(year) & !is.na(user_id)]

  dt10 <- dt[year %in% YEARS_LAST10]

  size_y <- unique(dt10[, .(user_id, year, cluster_id)])[
    , .(cluster_size = uniqueN(user_id)), by = .(cluster_id, year)
  ]

  size_y <- size_y[cluster_size >= MIN_CLUSTER]

  write_csv(
    as_tibble(size_y) %>% mutate(cluster_level = label),
    file.path(dirs$sizes, paste0(label, "__cluster_year_size__", LAST10_START, "_", LAST10_END, ".csv"))
  )

  size_c <- as_tibble(size_y) %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      years_used_10y   = dplyr::n(),
      .groups = "drop"
    ) %>%
    mutate(cluster_level = label) %>%
    filter(is.finite(cluster_size_10y), cluster_size_10y > 0)

  write_csv(
    size_c,
    file.path(dirs$sizes, paste0(label, "__cluster_const_size__", LAST10_START, "_", LAST10_END, ".csv"))
  )

  size_c
}

make_focus_table <- function(tab) {
  tab %>%
    filter(
      term == "cluster_size_10y" |
        term == "tenure" |
        term == "tenure_sq" |
        grepl("tenure_bin", term, fixed = TRUE)
    )
}

make_wide_compare <- function(tab) {
  if (nrow(tab) == 0) return(tibble())

  keep <- tab %>%
    select(study, tenure_form, spec_name, term, estimate, se, p, stars)

  long <- bind_rows(
    keep %>% mutate(metric = "estimate", value = sprintf("%.6f", estimate)),
    keep %>% mutate(metric = "se",       value = sprintf("%.6f", se)),
    keep %>% mutate(metric = "p",        value = sprintf("%.6f", p)),
    keep %>% mutate(metric = "stars",    value = stars)
  ) %>%
    mutate(spec_metric = paste0(spec_name, "__", metric)) %>%
    select(study, tenure_form, term, spec_metric, value)

  long %>%
    tidyr::pivot_wider(
      names_from = spec_metric,
      values_from = value
    ) %>%
    arrange(study, tenure_form, term)
}

# =========================
# 4) Load Arrow + build FULL PANEL df
# =========================
dirs <- get_dirs()

ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")
ds_names <- names(ds)

METRO_VAR <- pick_first_existing(METRO_CANDIDATES, ds_names, what = "metro")
ts_msg("Using metro variable:", METRO_VAR)

need_cols(ds_names, c(
  "user_id", "year", "n_patents", "first_country",
  "first_startdate_edu", "first_startdate_pos",
  PARENT_VAR, METRO_VAR
))

ts_msg("Collecting FULL PANEL years:", YEAR_START, "to", YEAR_END, "(NO US FILTER)")
df_full <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(all_of(c(
    "user_id", "year", "n_patents", "first_country",
    "first_startdate_edu", "first_startdate_pos",
    PARENT_VAR, METRO_VAR
  ))) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    !!METRO_VAR := str_trim(as.character(.data[[METRO_VAR]]))
  ) %>%
  compute_tenure() %>%
  make_metro_parent(METRO_VAR)

rm(ds)
gc()

# =========================
# 5) Build time-invariant size measures
# =========================
size_parent <- make_timeinvariant_size_overall(
  df_all_fullpanel = df_full,
  cluster_var = PARENT_VAR,
  label = "PARENT",
  dirs = dirs
)

size_metro_parent <- make_timeinvariant_size_overall(
  df_all_fullpanel = df_full,
  cluster_var = "metro_parent_id",
  label = "METRO_PARENT",
  dirs = dirs
)

# =========================
# 6) Regression runner
# =========================
run_study <- function(df_fullpanel, study_name, cluster_var, study_fe_var, size_const) {
  ts_msg("============================================================")
  ts_msg("RUNNING STUDY:", study_name)
  ts_msg("============================================================")

  df_us <- df_fullpanel %>%
    filter(!is.na(first_country), first_country == US_COUNTRY)

  dd0 <- df_us %>%
    mutate(
      cluster_id   = as.character(.data[[cluster_var]]),
      metro_fe_id  = as.character(.data[[METRO_VAR]]),
      study_fe_id  = as.character(.data[[study_fe_var]])
    ) %>%
    filter(
      !is.na(cluster_id), cluster_id != "",
      !is.na(user_id), user_id != "",
      !is.na(year), year >= YEAR_START, year <= YEAR_END,
      !is.na(n_patents), n_patents >= 0,
      is.finite(tenure), is.finite(tenure_sq)
    ) %>%
    left_join(size_const, by = "cluster_id") %>%
    filter(is.finite(cluster_size_10y), cluster_size_10y > 0) %>%
    mutate(
      tenure_bin = make_tenure_bins(tenure),
      user_id    = as.factor(user_id),
      year_fe    = as.factor(year),
      metro_fe   = as.factor(metro_fe_id),
      study_fe   = as.factor(study_fe_id),
      cluster_id = as.factor(cluster_id)
    )

  dd0 <- dd0 %>% filter(!is.na(tenure_bin))

  fe_ladder <- list(
    list(spec_name = "base_user_year",            fe = "user_id + year_fe"),
    list(spec_name = "plus_metro_fe",            fe = "user_id + year_fe + metro_fe"),
    list(spec_name = "plus_study_fe",            fe = "user_id + year_fe + study_fe"),
    list(spec_name = "plus_metro_and_study_fe",  fe = "user_id + year_fe + metro_fe + study_fe")
  )

  tenure_forms <- list(
    list(tenure_form = "quadratic", rhs = "cluster_size_10y + tenure + tenure_sq"),
    list(tenure_form = "bins",      rhs = "cluster_size_10y + i(tenure_bin, ref = \"0-5\")")
  )

  out_list <- list()
  k <- 0L

  for (wtag in names(WEIGHTS)) {
    wform <- WEIGHTS[[wtag]]

    for (tf in tenure_forms) {
      for (sp in fe_ladder) {
        ts_msg(" ->", study_name, "|", tf$tenure_form, "|", sp$spec_name, "|", wtag, "| N =", nrow(dd0))

        fit <- run_ppml(
          rhs = tf$rhs,
          fe_part = sp$fe,
          data = dd0,
          wform = wform
        )

        k <- k + 1L
        out_list[[k]] <- extract_fixest_terms(
          fit,
          paste0(study_name, "__", tf$tenure_form, "__", sp$spec_name, "__", wtag)
        ) %>%
          mutate(
            study = study_name,
            tenure_form = tf$tenure_form,
            spec_name = sp$spec_name,
            weight = wtag,
            n_obs = nrow(dd0),
            n_clusters = dplyr::n_distinct(dd0$cluster_id)
          )
      }
    }
  }

  bind_rows(out_list)
}

# =========================
# 7) Run studies
# =========================
results_parent <- run_study(
  df_fullpanel = df_full,
  study_name   = "PARENT",
  cluster_var  = PARENT_VAR,
  study_fe_var = PARENT_VAR,
  size_const   = size_parent
)

results_metro_parent <- run_study(
  df_fullpanel = df_full,
  study_name   = "METRO_PARENT",
  cluster_var  = "metro_parent_id",
  study_fe_var = "metro_parent_id",
  size_const   = size_metro_parent
)

results_all <- bind_rows(results_parent, results_metro_parent)
focus_all   <- make_focus_table(results_all)
wide_all    <- make_wide_compare(focus_all)

# =========================
# 8) Save outputs
# =========================
write_csv(results_parent, file.path(dirs$tables, "PARENT__ppml_tenure_fe_ladder__all_terms.csv"))
write_csv(results_metro_parent, file.path(dirs$tables, "METRO_PARENT__ppml_tenure_fe_ladder__all_terms.csv"))
write_csv(results_all, file.path(dirs$tables, "ALL_STUDIES__ppml_tenure_fe_ladder__all_terms.csv"))

write_csv(
  make_focus_table(results_parent),
  file.path(dirs$tables, "PARENT__ppml_tenure_fe_ladder__focus_terms.csv")
)
write_csv(
  make_focus_table(results_metro_parent),
  file.path(dirs$tables, "METRO_PARENT__ppml_tenure_fe_ladder__focus_terms.csv")
)
write_csv(focus_all, file.path(dirs$tables, "ALL_STUDIES__ppml_tenure_fe_ladder__focus_terms.csv"))
write_csv(wide_all, file.path(dirs$tables, "ALL_STUDIES__ppml_tenure_fe_ladder__focus_terms_wide.csv"))

# Simple log note
log_lines <- c(
  "PPML FE ladder completed.",
  "",
  "Studies:",
  "- PARENT",
  "- METRO_PARENT",
  "",
  "Tenure forms:",
  "- quadratic: tenure + tenure_sq",
  "- bins: 0-5, 6-10, 11-15, ...",
  "",
  "FE ladder:",
  "- user_id + year_fe",
  paste0("- user_id + year_fe + ", METRO_VAR),
  "- user_id + year_fe + study_fe",
  paste0("- user_id + year_fe + ", METRO_VAR, " + study_fe"),
  "",
  "Important note:",
  "- cluster_size_10y is time-invariant at the study-cluster level.",
  "- Once study FE are added, cluster_size_10y will typically be collinear and dropped.",
  "- This is expected and recorded in the column 'collinear_dropped'."
)
writeLines(log_lines, file.path(dirs$logs, "run_notes.txt"))

ts_msg("ALL DONE. Outputs in:", OUT_DIR)
