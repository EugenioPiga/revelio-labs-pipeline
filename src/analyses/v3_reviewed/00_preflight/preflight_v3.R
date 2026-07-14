#!/usr/bin/env Rscript

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "readr"))
v3_configure_resources(FALSE)

out <- v3_output_dir("preflight")
core <- v3_assert_dir(v3_env("REVELIO_V3_INPUT"), "REVELIO_V3_INPUT")
colleague <- v3_assert_dir(v3_env("REVELIO_V3_COLLEAGUE_INPUT"), "REVELIO_V3_COLLEAGUE_INPUT")
crosswalk <- path.expand(v3_env("REVELIO_METRO_DENSITY_CROSSWALK"))

core_need <- c(
  "user_id", "year", "n_patents", "n_us_citations",
  "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "first_parent_rcid", "first_rcid", "first_city", "first_metro_area"
)

colleague_need <- c(
  core_need,
  "avg_seniority",
  "n_cite_links_total", "n_cite_links_same_parent",
  "n_cite_links_received_total", "n_cite_links_received_same_parent",
  "n_collab_links_total", "n_collab_links_same_parent"
)

inspect_schema <- function(path, need, label) {
  ds <- arrow::open_dataset(path, format = "parquet")
  cols <- names(ds)
  missing <- setdiff(need, cols)
  data.table::data.table(
    panel = label,
    path = path,
    n_columns = length(cols),
    required_columns = length(need),
    missing_columns = if (length(missing)) paste(missing, collapse = " | ") else "NONE",
    status = if (length(missing)) "FAIL" else "PASS"
  )
}

status <- data.table::rbindlist(list(
  inspect_schema(core, core_need, "core_v3"),
  inspect_schema(colleague, colleague_need, "colleague_v3"),
  data.table::data.table(
    panel = "density_crosswalk",
    path = crosswalk,
    n_columns = NA_integer_,
    required_columns = 2L,
    missing_columns = if (file.exists(crosswalk)) "NONE" else "missing file",
    status = if (file.exists(crosswalk)) "PASS" else "WARN"
  )
), fill = TRUE)

if (file.exists(crosswalk)) {
  cw <- tryCatch(readr::read_csv(crosswalk, n_max = 5, show_col_types = FALSE), error = function(e) NULL)
  if (is.null(cw)) {
    status[panel == "density_crosswalk", `:=`(status = "WARN", missing_columns = "file unreadable")]
  } else {
    need_cw <- c("revelio_metro_raw", "density_per_sqmi")
    miss_cw <- setdiff(need_cw, names(cw))
    if (length(miss_cw)) {
      status[panel == "density_crosswalk",
             `:=`(status = "WARN", missing_columns = paste(miss_cw, collapse = " | "))]
    }
  }
}

data.table::fwrite(status, file.path(out, "preflight_schema_status.csv"))

# A deliberately light data check: Arrow computes these aggregates without
# collecting the full panel to R.
sample_check <- function(path, label) {
  ds <- arrow::open_dataset(path, format = "parquet")
  ans <- tryCatch(
    ds |>
      dplyr::summarise(
        min_year = min(year, na.rm = TRUE),
        max_year = max(year, na.rm = TRUE),
        n_rows = dplyr::n()
      ) |>
      dplyr::collect(),
    error = function(e) data.frame(min_year = NA_integer_, max_year = NA_integer_, n_rows = NA_real_,
                                   error = conditionMessage(e))
  )
  data.table::as.data.table(ans)[, panel := label][]
}

v3_bootstrap(c("dplyr"))
coverage <- data.table::rbindlist(list(
  sample_check(core, "core_v3"),
  sample_check(colleague, "colleague_v3")
), fill = TRUE)
data.table::fwrite(coverage, file.path(out, "preflight_panel_coverage.csv"))

readr::write_lines(c(
  "# V3 preflight",
  "",
  paste0("- Core panel: `", core, "`"),
  paste0("- Colleague panel: `", colleague, "`"),
  paste0("- Density crosswalk: `", crosswalk, "`"),
  "",
  "The density crosswalk is required only when the optional density split is requested.",
  "All main analyses impose U.S. position-years and U.S. last-degree-country restrictions."
), file.path(out, "README.md"))

print(status)
print(coverage)

if (any(status$status == "FAIL")) stop("V3 preflight failed. Inspect output/preflight/preflight_schema_status.csv.")
message("[PASS] V3 preflight completed. Outputs: ", out)
