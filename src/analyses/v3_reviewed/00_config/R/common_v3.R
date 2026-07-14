# Shared helpers for the reviewed V3 analysis suite.
# Every analysis script sources this file. Each remains runnable from its own
# sbatch wrapper and installs an R package only when it is genuinely missing.

v3_bool <- function(x, default = FALSE) {
  if (is.null(x) || !length(x) || is.na(x) || !nzchar(as.character(x))) return(default)
  tolower(trimws(as.character(x))) %in% c("1", "true", "t", "yes", "y")
}

v3_env <- function(name, default = "") {
  x <- Sys.getenv(name, unset = default)
  if (!nzchar(x)) default else x
}

v3_script_path <- function() {
  args <- commandArgs(trailingOnly = FALSE)
  hit <- sub("^--file=", "", args[grepl("^--file=", args)])
  if (length(hit)) return(normalizePath(hit[[1]], mustWork = FALSE))
  NA_character_
}

v3_root <- function(script_path = v3_script_path()) {
  root <- v3_env("REVELIO_ANALYSES_ROOT", "")
  if (nzchar(root) && dir.exists(root)) return(normalizePath(root))
  if (!is.na(script_path) && nzchar(script_path)) {
    # All active scripts live exactly one directory below the suite root.
    return(normalizePath(file.path(dirname(script_path), ".."), mustWork = FALSE))
  }
  stop("Could not resolve REVELIO_ANALYSES_ROOT. Source 00_config/v3_env.sh first.")
}

v3_bootstrap <- function(packages) {
  user_lib <- path.expand(Sys.getenv("R_LIBS_USER", unset = "~/R/library"))
  dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
  .libPaths(unique(c(user_lib, .libPaths())))

  missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(missing)) {
    message("[bootstrap] Installing missing packages: ", paste(missing, collapse = ", "))
    install.packages(missing, repos = "https://cloud.r-project.org", lib = user_lib)
  }

  still_missing <- packages[!vapply(packages, requireNamespace, logical(1), quietly = TRUE)]
  if (length(still_missing)) {
    stop("Could not load required packages after installation: ",
         paste(still_missing, collapse = ", "))
  }

  invisible(lapply(packages, function(p) {
    suppressPackageStartupMessages(library(p, character.only = TRUE))
  }))
}

v3_configure_resources <- function(use_fixest = FALSE) {
  threads <- suppressWarnings(as.integer(v3_env(
    "REVELIO_R_THREADS",
    v3_env("SLURM_CPUS_PER_TASK", "1")
  )))
  if (!is.finite(threads) || threads < 1) threads <- 1L
  threads <- min(threads, 32L)

  Sys.setenv(
    OMP_NUM_THREADS = as.character(threads),
    OPENBLAS_NUM_THREADS = as.character(threads),
    MKL_NUM_THREADS = as.character(threads),
    VECLIB_MAXIMUM_THREADS = as.character(threads),
    NUMEXPR_NUM_THREADS = as.character(threads)
  )

  if (requireNamespace("data.table", quietly = TRUE)) {
    data.table::setDTthreads(threads)
  }
  if (isTRUE(use_fixest) && requireNamespace("fixest", quietly = TRUE) &&
      "setFixest_nthreads" %in% getNamespaceExports("fixest")) {
    fixest::setFixest_nthreads(threads)
  }

  invisible(threads)
}

v3_assert_dir <- function(path, label) {
  if (!nzchar(path) || !dir.exists(path)) {
    stop(label, " is missing or not a directory: ", path)
  }
  normalizePath(path)
}

v3_output_dir <- function(component, root = v3_env("REVELIO_V3_OUTPUT_ROOT", "")) {
  if (!nzchar(root)) stop("REVELIO_V3_OUTPUT_ROOT is unset.")
  out <- file.path(path.expand(root), component)
  dir.create(out, recursive = TRUE, showWarnings = FALSE)
  normalizePath(out)
}

v3_clean_chr <- function(x) {
  x <- trimws(as.character(x))
  x[x %in% c("", "NA", "N/A", "NULL")] <- NA_character_
  x
}

v3_year_from_any <- function(x) {
  y <- suppressWarnings(as.integer(substr(as.character(x), 1, 4)))
  y[!is.finite(y)] <- NA_integer_
  y
}

v3_compute_tenure <- function(dt, max_tenure = 50L) {
  data.table::setDT(dt)
  if (!"first_obs_year" %in% names(dt)) {
    dt[, first_obs_year := min(year, na.rm = TRUE), by = user_id]
  }
  dt[, edu_year_v3 := v3_year_from_any(first_startdate_edu)]
  dt[, pos_year_v3 := v3_year_from_any(first_startdate_pos)]
  dt[, tenure := as.numeric(year - edu_year_v3 + 3L)]
  dt[!is.finite(tenure) | tenure < 0 | tenure > max_tenure,
     tenure := as.numeric(year - pos_year_v3)]
  dt[!is.finite(tenure) | tenure < 0 | tenure > max_tenure,
     tenure := as.numeric(year - first_obs_year)]
  dt[!is.finite(tenure) | tenure < 0 | tenure > max_tenure, tenure := NA_real_]
  invisible(dt[])
}

v3_tenure_levels <- function(max_tenure = 50L, step = 5L) {
  lows <- c(0L, seq(step + 1L, max_tenure - step + 1L, by = step))
  highs <- seq(step, max_tenure, by = step)
  paste0(lows, "-", highs)
}

v3_tenure_bin <- function(x, max_tenure = 50L, step = 5L) {
  lev <- v3_tenure_levels(max_tenure, step)
  breaks <- c(-1, seq(step, max_tenure, by = step))
  factor(cut(x, breaks = breaks, labels = lev, include.lowest = TRUE, right = TRUE),
         levels = lev, ordered = TRUE)
}

v3_safe_ratio <- function(num, den) {
  ifelse(is.finite(den) & den > 0, num / den, NA_real_)
}

v3_require_cols <- function(have, need, context = "dataset") {
  missing <- setdiff(need, have)
  if (length(missing)) {
    stop("Missing required columns in ", context, ": ", paste(missing, collapse = ", "))
  }
  invisible(TRUE)
}

v3_assert_unique_user_year <- function(dt, diagnostic_path = NULL) {
  data.table::setDT(dt)
  dup <- dt[, .N, by = .(user_id, year)][N > 1L]
  if (nrow(dup)) {
    if (!is.null(diagnostic_path)) data.table::fwrite(dup, diagnostic_path)
    stop(
      "The panel is not unique at user_id x year. ",
      "See duplicate diagnostic: ", diagnostic_path %||% "not written"
    )
  }
  invisible(TRUE)
}

`%||%` <- function(x, y) if (is.null(x) || !length(x) || is.na(x)) y else x

v3_write_manifest <- function(path, rows) {
  data.table::fwrite(data.table::as.data.table(rows), path)
}

v3_year_window <- function(tag) {
  map <- list(
    `2000_2019` = c(2000L, 2019L),
    `2010_2019` = c(2010L, 2019L)
  )
  if (!tag %in% names(map)) stop("Unknown window: ", tag)
  map[[tag]]
}

v3_plot_theme <- function() {
  ggplot2::theme_minimal(base_size = 12) +
    ggplot2::theme(
      panel.grid.minor = ggplot2::element_blank(),
      plot.title = ggplot2::element_text(face = "bold"),
      legend.position = "bottom"
    )
}

v3_status_row <- function(name, status, detail = "") {
  data.table::data.table(check = name, status = status, detail = detail)
}
