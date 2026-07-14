#!/usr/bin/env Rscript
# Reviewed colleague citation/collaboration descriptives.
# The script performs one filtered Arrow read, then creates all summaries
# in memory. It retains the U.S.-position and U.S.-educated sample exactly.

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))

v3_bootstrap(c("arrow", "data.table", "dplyr", "readr", "ggplot2", "scales"))
threads <- v3_configure_resources(FALSE)
set.seed(123)

INPUT <- v3_assert_dir(v3_env("REVELIO_V3_COLLEAGUE_INPUT"), "REVELIO_V3_COLLEAGUE_INPUT")
OUT <- v3_output_dir("colleague_links")
TAB <- file.path(OUT, "tables")
FIG <- file.path(OUT, "figures")
DIAG <- file.path(OUT, "diagnostics")
dir.create(TAB, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG, recursive = TRUE, showWarnings = FALSE)
dir.create(DIAG, recursive = TRUE, showWarnings = FALSE)

YEAR_MIN <- as.integer(v3_env("COLLEAGUE_YEAR_MIN", "1990"))
YEAR_MAX <- as.integer(v3_env("COLLEAGUE_YEAR_MAX", "2019"))
US <- v3_env("REVELIO_US_COUNTRY", "United States")
MAX_TENURE <- 50L

msg <- function(...) cat(sprintf("[%s] %s\n", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), paste(..., collapse = " ")))

id_cols <- c(
  "user_id", "year", "n_patents", "first_country", "last_university_country",
  "first_startdate_edu", "first_startdate_pos",
  "current_onet_code", "current_naics3", "current_role_k150_v2"
)

count_cols <- c(
  # Outgoing inventor-to-inventor citations.
  "n_cite_links_total",
  "n_cite_links_same_parent",
  "n_cite_links_same_parent_more_senior",
  "n_cite_links_same_parent_less_equal_senior",
  "n_cite_links_same_parent_metro",
  # Incoming inventor-to-inventor citations.
  "n_cite_links_received_total",
  "n_cite_links_received_same_parent",
  "n_cite_links_received_same_parent_from_more_senior",
  "n_cite_links_received_same_parent_from_less_senior",
  "n_cite_links_received_same_parent_from_equal_senior",
  "n_cite_links_received_same_parent_metro",
  # Incoming citing-patent cross-check.
  "n_citing_patents_received_total",
  "n_citing_patents_received_same_parent",
  "n_citing_patents_received_same_parent_from_more_senior",
  "n_citing_patents_received_same_parent_from_less_senior",
  "n_citing_patents_received_same_parent_from_equal_senior",
  # Co-inventor links and patent-level collaboration cross-check.
  "n_collab_links_total",
  "n_collab_links_same_parent",
  "n_collab_links_same_parent_more_senior",
  "n_collab_links_same_parent_less_equal_senior",
  "n_collab_links_same_parent_metro",
  "n_patents_with_any_coinventor",
  "n_patents_with_same_parent_collab",
  "n_patents_with_same_parent_more_senior_collab",
  "n_patents_with_same_parent_less_equal_senior_collab",
  "n_patents_with_same_parent_metro_collab"
)

ds <- arrow::open_dataset(INPUT, format = "parquet")
v3_require_cols(names(ds), c(id_cols, count_cols), "V3 colleague panel")

msg("Reading colleague panel once:", YEAR_MIN, "-", YEAR_MAX)
dt <- ds |>
  dplyr::filter(
    year >= YEAR_MIN, year <= YEAR_MAX,
    first_country == US, last_university_country == US
  ) |>
  dplyr::select(dplyr::all_of(c(id_cols, count_cols))) |>
  dplyr::collect() |>
  data.table::as.data.table()

if (!nrow(dt)) stop("No colleague observations after sample restriction.")
for (cc in intersect(c("user_id", "first_country", "last_university_country",
                       "first_startdate_edu", "first_startdate_pos",
                       "current_onet_code", "current_naics3", "current_role_k150_v2"), names(dt))) {
  data.table::set(dt, j = cc, value = v3_clean_chr(dt[[cc]]))
}
dt[, year := as.integer(year)]
for (cc in c("n_patents", count_cols)) {
  data.table::set(dt, j = cc, value = suppressWarnings(as.numeric(dt[[cc]])))
  dt[is.na(get(cc)), (cc) := 0]
}
data.table::setorder(dt, user_id, year)
v3_assert_unique_user_year(dt, file.path(DIAG, "duplicate_user_years.csv"))
v3_compute_tenure(dt, MAX_TENURE)
dt[, tenure_bin := v3_tenure_bin(tenure, MAX_TENURE, 5L)]

summarise_counts <- function(x, by = NULL) {
  base <- x[, c(
    list(
      n_inventor_years = .N,
      n_inventors = data.table::uniqueN(user_id),
      n_patenting_inventor_years = sum(n_patents > 0, na.rm = TRUE),
      total_patents = sum(n_patents, na.rm = TRUE),
      coverage_onet = mean(!is.na(current_onet_code), na.rm = TRUE),
      coverage_naics3 = mean(!is.na(current_naics3), na.rm = TRUE),
      coverage_role150 = mean(!is.na(current_role_k150_v2), na.rm = TRUE)
    ),
    stats::setNames(lapply(.SD, sum, na.rm = TRUE), paste0("total_", names(.SD)))
  ), by = by, .SDcols = count_cols]
  base
}

add_metrics <- function(x) {
  x[, `:=`(
    outgoing_same_parent_share = v3_safe_ratio(total_n_cite_links_same_parent, total_n_cite_links_total),
    outgoing_same_parent_metro_share = v3_safe_ratio(total_n_cite_links_same_parent_metro, total_n_cite_links_total),
    outgoing_to_senior_given_same_parent = v3_safe_ratio(
      total_n_cite_links_same_parent_more_senior, total_n_cite_links_same_parent
    ),
    outgoing_to_junior_equal_given_same_parent = v3_safe_ratio(
      total_n_cite_links_same_parent_less_equal_senior, total_n_cite_links_same_parent
    ),
    incoming_same_parent_share = v3_safe_ratio(
      total_n_cite_links_received_same_parent, total_n_cite_links_received_total
    ),
    incoming_same_parent_metro_share = v3_safe_ratio(
      total_n_cite_links_received_same_parent_metro, total_n_cite_links_received_total
    ),
    incoming_from_senior_given_same_parent = v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_more_senior,
      total_n_cite_links_received_same_parent
    ),
    incoming_from_junior_given_same_parent = v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_less_senior,
      total_n_cite_links_received_same_parent
    ),
    incoming_from_equal_given_same_parent = v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_equal_senior,
      total_n_cite_links_received_same_parent
    ),
    incoming_seniority_classification_rate = v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_more_senior +
        total_n_cite_links_received_same_parent_from_less_senior +
        total_n_cite_links_received_same_parent_from_equal_senior,
      total_n_cite_links_received_same_parent
    ),
    incoming_citations_from_senior_per_1000_iy = 1000 * v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_more_senior, n_inventor_years
    ),
    incoming_citations_from_junior_per_1000_iy = 1000 * v3_safe_ratio(
      total_n_cite_links_received_same_parent_from_less_senior, n_inventor_years
    ),
    collab_same_parent_share = v3_safe_ratio(total_n_collab_links_same_parent, total_n_collab_links_total),
    collab_same_parent_metro_share = v3_safe_ratio(
      total_n_collab_links_same_parent_metro, total_n_collab_links_total
    ),
    collab_with_senior_given_same_parent = v3_safe_ratio(
      total_n_collab_links_same_parent_more_senior, total_n_collab_links_same_parent
    ),
    collab_with_junior_equal_given_same_parent = v3_safe_ratio(
      total_n_collab_links_same_parent_less_equal_senior, total_n_collab_links_same_parent
    ),
    patent_same_parent_collab_share = v3_safe_ratio(
      total_n_patents_with_same_parent_collab, total_n_patents_with_any_coinventor
    ),
    incoming_patent_same_parent_share = v3_safe_ratio(
      total_n_citing_patents_received_same_parent, total_n_citing_patents_received_total
    ),
    incoming_patent_from_senior_given_same_parent = v3_safe_ratio(
      total_n_citing_patents_received_same_parent_from_more_senior,
      total_n_citing_patents_received_same_parent
    ),
    incoming_patent_from_junior_given_same_parent = v3_safe_ratio(
      total_n_citing_patents_received_same_parent_from_less_senior,
      total_n_citing_patents_received_same_parent
    ),
    incoming_patent_from_equal_given_same_parent = v3_safe_ratio(
      total_n_citing_patents_received_same_parent_from_equal_senior,
      total_n_citing_patents_received_same_parent
    )
  )]
  x[]
}

overall <- add_metrics(summarise_counts(dt))
yearly <- add_metrics(summarise_counts(dt, by = "year"))
tenure_bin <- add_metrics(summarise_counts(dt[!is.na(tenure_bin)], by = "tenure_bin"))
data.table::setorder(tenure_bin, tenure_bin)

# Data-integrity checks are aggregate and cheap. They flag, rather than silently
# discard, cells in which component counts exceed their natural denominators.
integrity <- dt[, .(
  n_rows = .N,
  outgoing_component_exceeds_same_parent = sum(
    n_cite_links_same_parent_more_senior + n_cite_links_same_parent_less_equal_senior >
      n_cite_links_same_parent, na.rm = TRUE
  ),
  incoming_component_exceeds_same_parent = sum(
    n_cite_links_received_same_parent_from_more_senior +
      n_cite_links_received_same_parent_from_less_senior +
      n_cite_links_received_same_parent_from_equal_senior >
      n_cite_links_received_same_parent, na.rm = TRUE
  ),
  collab_component_exceeds_same_parent = sum(
    n_collab_links_same_parent_more_senior + n_collab_links_same_parent_less_equal_senior >
      n_collab_links_same_parent, na.rm = TRUE
  ),
  incoming_patent_component_exceeds_same_parent = sum(
    n_citing_patents_received_same_parent_from_more_senior +
      n_citing_patents_received_same_parent_from_less_senior +
      n_citing_patents_received_same_parent_from_equal_senior >
      n_citing_patents_received_same_parent, na.rm = TRUE
  )
)]

data.table::fwrite(overall, file.path(TAB, "overall_summary.csv"))
data.table::fwrite(yearly, file.path(TAB, "yearly_summary.csv"))
data.table::fwrite(tenure_bin, file.path(TAB, "tenure_bin_summary.csv"))
data.table::fwrite(integrity, file.path(DIAG, "measure_integrity_checks.csv"))

# Compact main figures: one plot per empirical question, rather than dozens of
# near-duplicate series.
make_long_plot <- function(z, vars, labels, title, subtitle, ylab, file, percent = TRUE) {
  zz <- data.table::melt(
    z, id.vars = "tenure_bin", measure.vars = vars,
    variable.name = "metric", value.name = "value"
  )
  zz[, metric := factor(labels[metric], levels = unname(labels))]
  p <- ggplot2::ggplot(zz, ggplot2::aes(x = tenure_bin, y = value, group = metric, color = metric)) +
    ggplot2::geom_line(linewidth = 0.9, na.rm = TRUE) +
    ggplot2::geom_point(size = 1.4, na.rm = TRUE) +
    ggplot2::labs(title = title, subtitle = subtitle, x = "Tenure bin", y = ylab, color = NULL) +
    v3_plot_theme()
  if (percent) p <- p + ggplot2::scale_y_continuous(labels = scales::percent_format(accuracy = 0.1))
  ggplot2::ggsave(file.path(FIG, file), p, width = 10, height = 6, dpi = 250)
}

make_long_plot(
  tenure_bin,
  c("outgoing_same_parent_share", "outgoing_same_parent_metro_share"),
  c(outgoing_same_parent_share = "Same parent", outgoing_same_parent_metro_share = "Same parent and metro"),
  "Outgoing citations to coworkers over tenure",
  "Shares use all outgoing inventor-to-inventor citation links as the denominator.",
  "Share of outgoing citation links", "outgoing_coworker_citations_by_tenure.png"
)

make_long_plot(
  tenure_bin,
  c("incoming_from_junior_given_same_parent", "incoming_from_equal_given_same_parent",
    "incoming_from_senior_given_same_parent"),
  c(
    incoming_from_junior_given_same_parent = "From junior colleague",
    incoming_from_equal_given_same_parent = "From equal-seniority colleague",
    incoming_from_senior_given_same_parent = "From senior colleague"
  ),
  "Who cites the focal inventor inside the same parent?",
  "Shares condition on incoming same-parent citation links.",
  "Share of incoming same-parent citation links", "incoming_citation_seniority_by_tenure.png"
)

make_long_plot(
  tenure_bin,
  c("collab_same_parent_share", "collab_same_parent_metro_share"),
  c(collab_same_parent_share = "Same parent", collab_same_parent_metro_share = "Same parent and metro"),
  "Collaboration with coworkers over tenure",
  "Shares use all co-inventor links as the denominator.",
  "Share of collaboration links", "coworker_collaboration_by_tenure.png"
)

make_long_plot(
  tenure_bin,
  c("incoming_citations_from_junior_per_1000_iy", "incoming_citations_from_senior_per_1000_iy"),
  c(
    incoming_citations_from_junior_per_1000_iy = "From junior colleague",
    incoming_citations_from_senior_per_1000_iy = "From senior colleague"
  ),
  "Incoming same-parent citations per inventor-year",
  "Rates are not conditional on receiving a citation.",
  "Incoming citation links per 1,000 inventor-years",
  "incoming_citation_rates_by_tenure.png", percent = FALSE
)

manifest <- data.table::data.table(
  input = INPUT,
  output = OUT,
  year_min = YEAR_MIN,
  year_max = YEAR_MAX,
  sample_definition = "first_country == United States AND last_university_country == United States",
  n_obs = nrow(dt),
  n_users = data.table::uniqueN(dt$user_id),
  r_threads = threads,
  main_outcome_family = "outgoing citations; incoming citations; collaboration",
  note = "Detailed patent-level counts are retained in tables as cross-checks; figures prioritize pre-specified link-level outcomes."
)
data.table::fwrite(manifest, file.path(DIAG, "manifest.csv"))

readr::write_lines(c(
  "# Reviewed colleague-links analysis",
  "",
  "- Uses the Step-6-enriched V3 panel.",
  "- Uses one filtered data read rather than repeated Arrow scans.",
  "- Keeps U.S. position-years and U.S. last-degree-country restrictions.",
  "- Reports aggregate ratios, with denominator and classification diagnostics.",
  "- Treats incoming seniority shares as descriptive composition, not causal effects."
), file.path(OUT, "README.md"))

msg("[PASS] Colleague analysis completed. Outputs:", OUT)
