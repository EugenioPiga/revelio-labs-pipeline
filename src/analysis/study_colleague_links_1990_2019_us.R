#!/usr/bin/env Rscript
################################################################################
# study_colleague_links_1990_2019_us.R
#
# Focused diagnostics for Step 6 colleague citation/collaboration variables.
#
# Sample restriction:
#   1. year in 1990–2019
#   2. first_country == "United States"
#   3. last degree country == "United States"
#
# Input:
#   /labs/khanna/linkedin_202507/processed/inventor_year_merged_v2
#
# Output:
#   /home/epiga/revelio_labs/output/colleague_links_1990_2019_us/
################################################################################

suppressPackageStartupMessages({
  library(arrow)
  library(dplyr)
  library(tidyr)
  library(ggplot2)
  library(readr)
  library(scales)
  library(stringr)
  library(rlang)
})

DATA_PATH <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"

OUT_BASE <- "/home/epiga/revelio_labs/output/colleague_links_1990_2019_us"
TAB_DIR  <- file.path(OUT_BASE, "tables")
FIG_DIR  <- file.path(OUT_BASE, "figures")

dir.create(TAB_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(FIG_DIR, recursive = TRUE, showWarnings = FALSE)

safe_ratio <- function(num, den) {
  ifelse(!is.na(den) & den > 0, num / den, NA_real_)
}

theme_clean <- function() {
  theme_minimal(base_size = 12) +
    theme(
      plot.title = element_text(face = "bold"),
      plot.subtitle = element_text(color = "gray35"),
      legend.position = "bottom",
      panel.grid.minor = element_blank()
    )
}

save_plot <- function(p, filename, width = 10, height = 6) {
  out <- file.path(FIG_DIR, filename)
  ggsave(out, p, width = width, height = height, dpi = 220, bg = "white")
  message("[PLOT] ", out)
}

message("============================================================")
message("Colleague links, 1990–2019, US-position years, US last degree")
message("Input:  ", DATA_PATH)
message("Output: ", OUT_BASE)
message("============================================================")

ds <- open_dataset(DATA_PATH, format = "parquet")
schema_names <- names(ds)

# Flexible degree-country variable.
last_degree_country_col <- NULL
if ("last_university_country" %in% schema_names) {
  last_degree_country_col <- "last_university_country"
} else if ("edu_last_university_country" %in% schema_names) {
  last_degree_country_col <- "edu_last_university_country"
} else {
  stop("Could not find last degree country variable.")
}

if (!("first_country" %in% schema_names)) {
  stop("Could not find first_country. Needed for US-position-year restriction.")
}

required_vars <- c(
  "user_id", "year", "n_patents",
  "first_country", last_degree_country_col,
  "current_onet_code", "current_naics3", "current_role_k150_v2",
  "avg_seniority",

  "n_cite_links_total",
  "n_cite_links_same_parent",
  "n_cite_links_same_parent_more_senior",
  "n_cite_links_same_parent_less_equal_senior",
  "n_cite_links_same_parent_metro",
  "n_cite_links_same_parent_metro_more_senior",
  "n_cite_links_same_parent_metro_less_equal_senior",

  "n_cited_patents_total",
  "n_cited_patents_same_parent",
  "n_cited_patents_same_parent_more_senior",
  "n_cited_patents_same_parent_less_equal_senior",
  "n_cited_patents_same_parent_metro",
  "n_cited_patents_same_parent_metro_more_senior",
  "n_cited_patents_same_parent_metro_less_equal_senior",

  "n_collab_links_total",
  "n_collab_links_same_parent",
  "n_collab_links_same_parent_more_senior",
  "n_collab_links_same_parent_less_equal_senior",
  "n_collab_links_same_parent_metro",
  "n_collab_links_same_parent_metro_more_senior",
  "n_collab_links_same_parent_metro_less_equal_senior",

  "n_patents_with_any_coinventor",
  "n_patents_with_same_parent_collab",
  "n_patents_with_same_parent_more_senior_collab",
  "n_patents_with_same_parent_less_equal_senior_collab",
  "n_patents_with_same_parent_metro_collab",
  "n_patents_with_same_parent_metro_more_senior_collab",
  "n_patents_with_same_parent_metro_less_equal_senior_collab"
)

missing <- setdiff(required_vars, schema_names)
if (length(missing) > 0) {
  print(missing)
  stop("Some required variables are missing.")
}

US <- "United States"

df <- ds %>%
  mutate(last_degree_country_filter = !!sym(last_degree_country_col)) %>%
  filter(
    year >= 1990,
    year <= 2019,
    first_country == US,
    last_degree_country_filter == US
  )

# ==============================================================================
# 1. Decade summary
# ==============================================================================

message("[1/6] Creating 1990–2019 aggregate summary...")

decade <- df %>%
  summarise(
    n_inventor_years = n(),
    n_patenting_inventor_years = sum(if_else(n_patents > 0, 1, 0), na.rm = TRUE),
    total_patents = sum(n_patents, na.rm = TRUE),

    n_with_onet = sum(if_else(!is.na(current_onet_code), 1, 0), na.rm = TRUE),
    n_with_naics3 = sum(if_else(!is.na(current_naics3), 1, 0), na.rm = TRUE),
    n_with_role150 = sum(if_else(!is.na(current_role_k150_v2), 1, 0), na.rm = TRUE),

    total_cite_links = sum(n_cite_links_total, na.rm = TRUE),
    total_cite_links_same_parent = sum(n_cite_links_same_parent, na.rm = TRUE),
    total_cite_links_same_parent_more_senior = sum(n_cite_links_same_parent_more_senior, na.rm = TRUE),
    total_cite_links_same_parent_less_equal_senior = sum(n_cite_links_same_parent_less_equal_senior, na.rm = TRUE),
    total_cite_links_same_parent_metro = sum(n_cite_links_same_parent_metro, na.rm = TRUE),
    total_cite_links_same_parent_metro_more_senior = sum(n_cite_links_same_parent_metro_more_senior, na.rm = TRUE),
    total_cite_links_same_parent_metro_less_equal_senior = sum(n_cite_links_same_parent_metro_less_equal_senior, na.rm = TRUE),

    total_cited_patents = sum(n_cited_patents_total, na.rm = TRUE),
    total_cited_patents_same_parent = sum(n_cited_patents_same_parent, na.rm = TRUE),
    total_cited_patents_same_parent_more_senior = sum(n_cited_patents_same_parent_more_senior, na.rm = TRUE),
    total_cited_patents_same_parent_less_equal_senior = sum(n_cited_patents_same_parent_less_equal_senior, na.rm = TRUE),
    total_cited_patents_same_parent_metro = sum(n_cited_patents_same_parent_metro, na.rm = TRUE),
    total_cited_patents_same_parent_metro_more_senior = sum(n_cited_patents_same_parent_metro_more_senior, na.rm = TRUE),
    total_cited_patents_same_parent_metro_less_equal_senior = sum(n_cited_patents_same_parent_metro_less_equal_senior, na.rm = TRUE),

    total_collab_links = sum(n_collab_links_total, na.rm = TRUE),
    total_collab_links_same_parent = sum(n_collab_links_same_parent, na.rm = TRUE),
    total_collab_links_same_parent_more_senior = sum(n_collab_links_same_parent_more_senior, na.rm = TRUE),
    total_collab_links_same_parent_less_equal_senior = sum(n_collab_links_same_parent_less_equal_senior, na.rm = TRUE),
    total_collab_links_same_parent_metro = sum(n_collab_links_same_parent_metro, na.rm = TRUE),
    total_collab_links_same_parent_metro_more_senior = sum(n_collab_links_same_parent_metro_more_senior, na.rm = TRUE),
    total_collab_links_same_parent_metro_less_equal_senior = sum(n_collab_links_same_parent_metro_less_equal_senior, na.rm = TRUE),

    total_patents_with_any_coinventor = sum(n_patents_with_any_coinventor, na.rm = TRUE),
    total_patents_with_same_parent_collab = sum(n_patents_with_same_parent_collab, na.rm = TRUE),
    total_patents_with_same_parent_more_senior_collab = sum(n_patents_with_same_parent_more_senior_collab, na.rm = TRUE),
    total_patents_with_same_parent_less_equal_senior_collab = sum(n_patents_with_same_parent_less_equal_senior_collab, na.rm = TRUE),
    total_patents_with_same_parent_metro_collab = sum(n_patents_with_same_parent_metro_collab, na.rm = TRUE),
    total_patents_with_same_parent_metro_more_senior_collab = sum(n_patents_with_same_parent_metro_more_senior_collab, na.rm = TRUE),
    total_patents_with_same_parent_metro_less_equal_senior_collab = sum(n_patents_with_same_parent_metro_less_equal_senior_collab, na.rm = TRUE)
  ) %>%
  collect() %>%
  mutate(
    coverage_onet = safe_ratio(n_with_onet, n_inventor_years),
    coverage_naics3 = safe_ratio(n_with_naics3, n_inventor_years),
    coverage_role150 = safe_ratio(n_with_role150, n_inventor_years),

    share_cite_links_same_parent = safe_ratio(total_cite_links_same_parent, total_cite_links),
    share_cite_links_same_parent_metro = safe_ratio(total_cite_links_same_parent_metro, total_cite_links),
    share_cite_links_same_parent_more_senior = safe_ratio(total_cite_links_same_parent_more_senior, total_cite_links_same_parent),
    share_cite_links_same_parent_less_equal_senior = safe_ratio(total_cite_links_same_parent_less_equal_senior, total_cite_links_same_parent),

    share_cited_patents_same_parent = safe_ratio(total_cited_patents_same_parent, total_cited_patents),
    share_cited_patents_same_parent_metro = safe_ratio(total_cited_patents_same_parent_metro, total_cited_patents),
    share_cited_patents_same_parent_more_senior = safe_ratio(total_cited_patents_same_parent_more_senior, total_cited_patents_same_parent),
    share_cited_patents_same_parent_less_equal_senior = safe_ratio(total_cited_patents_same_parent_less_equal_senior, total_cited_patents_same_parent),

    share_collab_links_same_parent = safe_ratio(total_collab_links_same_parent, total_collab_links),
    share_collab_links_same_parent_metro = safe_ratio(total_collab_links_same_parent_metro, total_collab_links),
    share_collab_links_same_parent_more_senior = safe_ratio(total_collab_links_same_parent_more_senior, total_collab_links_same_parent),
    share_collab_links_same_parent_less_equal_senior = safe_ratio(total_collab_links_same_parent_less_equal_senior, total_collab_links_same_parent),

    share_patents_with_any_coinventor = safe_ratio(total_patents_with_any_coinventor, total_patents),
    share_patents_with_same_parent_collab = safe_ratio(total_patents_with_same_parent_collab, total_patents),
    share_patents_with_same_parent_metro_collab = safe_ratio(total_patents_with_same_parent_metro_collab, total_patents),
    share_patents_with_same_parent_more_senior_collab = safe_ratio(total_patents_with_same_parent_more_senior_collab, total_patents_with_same_parent_collab),
    share_patents_with_same_parent_less_equal_senior_collab = safe_ratio(total_patents_with_same_parent_less_equal_senior_collab, total_patents_with_same_parent_collab)
  )

write_csv(decade, file.path(TAB_DIR, "summary_1990_2019_us_lastdegree_us.csv"))

# ==============================================================================
# 2. Yearly summary
# ==============================================================================

message("[2/6] Creating yearly summary...")

yearly <- df %>%
  group_by(year) %>%
  summarise(
    n_inventor_years = n(),
    n_patenting_inventor_years = sum(if_else(n_patents > 0, 1, 0), na.rm = TRUE),
    total_patents = sum(n_patents, na.rm = TRUE),

    n_with_onet = sum(if_else(!is.na(current_onet_code), 1, 0), na.rm = TRUE),
    n_with_naics3 = sum(if_else(!is.na(current_naics3), 1, 0), na.rm = TRUE),
    n_with_role150 = sum(if_else(!is.na(current_role_k150_v2), 1, 0), na.rm = TRUE),

    total_cite_links = sum(n_cite_links_total, na.rm = TRUE),
    total_cite_links_same_parent = sum(n_cite_links_same_parent, na.rm = TRUE),
    total_cite_links_same_parent_more_senior = sum(n_cite_links_same_parent_more_senior, na.rm = TRUE),
    total_cite_links_same_parent_less_equal_senior = sum(n_cite_links_same_parent_less_equal_senior, na.rm = TRUE),
    total_cite_links_same_parent_metro = sum(n_cite_links_same_parent_metro, na.rm = TRUE),

    total_cited_patents = sum(n_cited_patents_total, na.rm = TRUE),
    total_cited_patents_same_parent = sum(n_cited_patents_same_parent, na.rm = TRUE),
    total_cited_patents_same_parent_more_senior = sum(n_cited_patents_same_parent_more_senior, na.rm = TRUE),
    total_cited_patents_same_parent_less_equal_senior = sum(n_cited_patents_same_parent_less_equal_senior, na.rm = TRUE),
    total_cited_patents_same_parent_metro = sum(n_cited_patents_same_parent_metro, na.rm = TRUE),

    total_collab_links = sum(n_collab_links_total, na.rm = TRUE),
    total_collab_links_same_parent = sum(n_collab_links_same_parent, na.rm = TRUE),
    total_collab_links_same_parent_more_senior = sum(n_collab_links_same_parent_more_senior, na.rm = TRUE),
    total_collab_links_same_parent_less_equal_senior = sum(n_collab_links_same_parent_less_equal_senior, na.rm = TRUE),
    total_collab_links_same_parent_metro = sum(n_collab_links_same_parent_metro, na.rm = TRUE),

    total_patents_with_any_coinventor = sum(n_patents_with_any_coinventor, na.rm = TRUE),
    total_patents_with_same_parent_collab = sum(n_patents_with_same_parent_collab, na.rm = TRUE),
    total_patents_with_same_parent_more_senior_collab = sum(n_patents_with_same_parent_more_senior_collab, na.rm = TRUE),
    total_patents_with_same_parent_less_equal_senior_collab = sum(n_patents_with_same_parent_less_equal_senior_collab, na.rm = TRUE),
    total_patents_with_same_parent_metro_collab = sum(n_patents_with_same_parent_metro_collab, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  collect() %>%
  arrange(year) %>%
  mutate(
    coverage_onet = safe_ratio(n_with_onet, n_inventor_years),
    coverage_naics3 = safe_ratio(n_with_naics3, n_inventor_years),
    coverage_role150 = safe_ratio(n_with_role150, n_inventor_years),

    share_cite_links_same_parent = safe_ratio(total_cite_links_same_parent, total_cite_links),
    share_cite_links_same_parent_metro = safe_ratio(total_cite_links_same_parent_metro, total_cite_links),
    share_cite_links_same_parent_more_senior = safe_ratio(total_cite_links_same_parent_more_senior, total_cite_links_same_parent),
    share_cite_links_same_parent_less_equal_senior = safe_ratio(total_cite_links_same_parent_less_equal_senior, total_cite_links_same_parent),

    share_cited_patents_same_parent = safe_ratio(total_cited_patents_same_parent, total_cited_patents),
    share_cited_patents_same_parent_metro = safe_ratio(total_cited_patents_same_parent_metro, total_cited_patents),
    share_cited_patents_same_parent_more_senior = safe_ratio(total_cited_patents_same_parent_more_senior, total_cited_patents_same_parent),
    share_cited_patents_same_parent_less_equal_senior = safe_ratio(total_cited_patents_same_parent_less_equal_senior, total_cited_patents_same_parent),

    share_collab_links_same_parent = safe_ratio(total_collab_links_same_parent, total_collab_links),
    share_collab_links_same_parent_metro = safe_ratio(total_collab_links_same_parent_metro, total_collab_links),
    share_collab_links_same_parent_more_senior = safe_ratio(total_collab_links_same_parent_more_senior, total_collab_links_same_parent),
    share_collab_links_same_parent_less_equal_senior = safe_ratio(total_collab_links_same_parent_less_equal_senior, total_collab_links_same_parent),

    share_patents_with_any_coinventor = safe_ratio(total_patents_with_any_coinventor, total_patents),
    share_patents_with_same_parent_collab = safe_ratio(total_patents_with_same_parent_collab, total_patents),
    share_patents_with_same_parent_metro_collab = safe_ratio(total_patents_with_same_parent_metro_collab, total_patents),
    share_patents_with_same_parent_more_senior_collab = safe_ratio(total_patents_with_same_parent_more_senior_collab, total_patents_with_same_parent_collab),
    share_patents_with_same_parent_less_equal_senior_collab = safe_ratio(total_patents_with_same_parent_less_equal_senior_collab, total_patents_with_same_parent_collab)
  )

write_csv(yearly, file.path(TAB_DIR, "yearly_1990_2019_us_lastdegree_us.csv"))

# Evolution table: 1990 vs 2019.
evolution_vars <- c(
  "coverage_onet", "coverage_naics3", "coverage_role150",
  "share_cite_links_same_parent", "share_cite_links_same_parent_metro",
  "share_cite_links_same_parent_more_senior", "share_cite_links_same_parent_less_equal_senior",
  "share_collab_links_same_parent", "share_collab_links_same_parent_metro",
  "share_collab_links_same_parent_more_senior", "share_collab_links_same_parent_less_equal_senior",
  "share_patents_with_any_coinventor",
  "share_patents_with_same_parent_collab", "share_patents_with_same_parent_metro_collab",
  "share_patents_with_same_parent_more_senior_collab",
  "share_patents_with_same_parent_less_equal_senior_collab"
)

evolution <- yearly %>%
  filter(year %in% c(1990, 2019)) %>%
  select(year, all_of(evolution_vars)) %>%
  pivot_longer(-year, names_to = "variable", values_to = "value") %>%
  pivot_wider(names_from = year, values_from = value, names_prefix = "y") %>%
  mutate(
    change_1990_2019 = y2019 - y1990,
    pct_change_1990_2019 = safe_ratio(change_1990_2019, y1990)
  )

write_csv(evolution, file.path(TAB_DIR, "evolution_1990_to_2019.csv"))

# ==============================================================================
# 3. Plots
# ==============================================================================

message("[3/6] Plotting yearly trends...")

p <- yearly %>%
  select(year, coverage_onet, coverage_naics3, coverage_role150) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Coverage of classification variables",
    subtitle = "1990–2019, first_country = US and last degree in US",
    x = NULL,
    y = "Share of inventor-years",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "01_coverage_onet_naics_role.png")

p <- yearly %>%
  select(year, share_cite_links_same_parent, share_cite_links_same_parent_metro) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Citation links to colleagues",
    subtitle = "Inventor-link version; same parent and same parent × metro",
    x = NULL,
    y = "Share of citation-inventor links",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "02_citation_links_same_parent_parent_metro.png")

p <- yearly %>%
  select(year, share_cited_patents_same_parent, share_cited_patents_same_parent_metro) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Cited patents containing colleagues",
    subtitle = "Cited-patent version; patent counted once if it contains a colleague",
    x = NULL,
    y = "Share of cited patents",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "03_cited_patents_same_parent_parent_metro.png")

p <- yearly %>%
  select(year, share_collab_links_same_parent, share_collab_links_same_parent_metro) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Collaboration links to colleagues",
    subtitle = "Co-inventor-link version; same parent and same parent × metro",
    x = NULL,
    y = "Share of co-inventor links",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "04_collab_links_same_parent_parent_metro.png")

p <- yearly %>%
  select(
    year,
    share_patents_with_any_coinventor,
    share_patents_with_same_parent_collab,
    share_patents_with_same_parent_metro_collab
  ) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Patent-level collaboration",
    subtitle = "Share of patents with any co-inventor, same-parent collaborator, and same-parent-metro collaborator",
    x = NULL,
    y = "Share of patents",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "05_patent_level_collaboration.png")

p <- yearly %>%
  select(
    year,
    share_cite_links_same_parent_more_senior,
    share_cite_links_same_parent_less_equal_senior
  ) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Seniority composition of same-parent citation links",
    subtitle = "More-senior vs less/equal-senior cited colleagues",
    x = NULL,
    y = "Share among same-parent citation links",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "06_citation_seniority_composition.png")

p <- yearly %>%
  select(
    year,
    share_collab_links_same_parent_more_senior,
    share_collab_links_same_parent_less_equal_senior
  ) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Seniority composition of same-parent collaboration links",
    subtitle = "More-senior vs less/equal-senior co-inventor colleagues",
    x = NULL,
    y = "Share among same-parent collaboration links",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "07_collaboration_seniority_composition.png")

p <- yearly %>%
  select(
    year,
    share_patents_with_same_parent_more_senior_collab,
    share_patents_with_same_parent_less_equal_senior_collab
  ) %>%
  pivot_longer(-year, names_to = "metric", values_to = "share") %>%
  ggplot(aes(year, share, color = metric)) +
  geom_line(linewidth = 1) +
  geom_point(size = 1.8) +
  scale_y_continuous(labels = percent) +
  labs(
    title = "Patent-level seniority composition of same-parent collaboration",
    subtitle = "Share among patents with same-parent collaborators",
    x = NULL,
    y = "Share",
    color = NULL
  ) +
  theme_clean()
save_plot(p, "08_patent_collaboration_seniority_composition.png")

# ==============================================================================
# 4. Text summary for coauthors
# ==============================================================================

message("[4/6] Writing coauthor text summary...")

d <- decade[1, ]

summary_lines <- c(
  "Colleague citation and collaboration measures, 1990–2019",
  "",
  "Sample restriction:",
  "  - year between 1990 and 2019",
  "  - first_country == United States",
  "  - last degree country == United States",
  "",
  "Construction:",
  "  Citation-to-colleague measures start from USPTO citation edges.",
  "  For each citing patent, we identify the focal inventor i and each inventor j on the cited patent.",
  "  We then compare i and j in the citing patent filing year.",
  "  Collaboration measures use co-inventor pairs on the same patent and compare i and j in the patent filing year.",
  "  Same-parent means i and j share the same parent firm. Same-parent-metro also requires the same metro.",
  "  More senior means floor(avg_seniority_j) > floor(avg_seniority_i). Less/equal senior means the opposite weak inequality.",
  "",
  sprintf("Inventor-years: %s", comma(d$n_inventor_years)),
  sprintf("Patents: %s", comma(d$total_patents)),
  "",
  sprintf("O*NET coverage: %.1f%%", 100 * d$coverage_onet),
  sprintf("NAICS3 coverage: %.1f%%", 100 * d$coverage_naics3),
  sprintf("role_k150 coverage: %.1f%%", 100 * d$coverage_role150),
  "",
  sprintf("Citation links to same-parent colleagues: %.1f%%", 100 * d$share_cite_links_same_parent),
  sprintf("Citation links to same-parent-metro colleagues: %.1f%%", 100 * d$share_cite_links_same_parent_metro),
  sprintf("Among same-parent citation links, share to more-senior colleagues: %.1f%%", 100 * d$share_cite_links_same_parent_more_senior),
  sprintf("Among same-parent citation links, share to less/equal-senior colleagues: %.1f%%", 100 * d$share_cite_links_same_parent_less_equal_senior),
  "",
  sprintf("Collaboration links to same-parent colleagues: %.1f%%", 100 * d$share_collab_links_same_parent),
  sprintf("Collaboration links to same-parent-metro colleagues: %.1f%%", 100 * d$share_collab_links_same_parent_metro),
  sprintf("Among same-parent collaboration links, share with more-senior colleagues: %.1f%%", 100 * d$share_collab_links_same_parent_more_senior),
  sprintf("Among same-parent collaboration links, share with less/equal-senior colleagues: %.1f%%", 100 * d$share_collab_links_same_parent_less_equal_senior),
  "",
  sprintf("Patents with any co-inventor: %.1f%%", 100 * d$share_patents_with_any_coinventor),
  sprintf("Patents with same-parent collaborators: %.1f%%", 100 * d$share_patents_with_same_parent_collab),
  sprintf("Patents with same-parent-metro collaborators: %.1f%%", 100 * d$share_patents_with_same_parent_metro_collab),
  sprintf("Among patents with same-parent collaborators, share involving more-senior colleagues: %.1f%%", 100 * d$share_patents_with_same_parent_more_senior_collab),
  sprintf("Among patents with same-parent collaborators, share involving less/equal-senior colleagues: %.1f%%", 100 * d$share_patents_with_same_parent_less_equal_senior_collab),
  "",
  "Interpretation:",
  "  These variables are descriptive, not causal.",
  "  More-senior collaboration can be interpreted as exposure to or collaboration with senior colleagues, but not necessarily causal help.",
  "  The key empirical distinction is between internal knowledge flows through citations and internal production through co-inventorship."
)

writeLines(summary_lines, file.path(OUT_BASE, "coauthor_summary_1990_2019_us.txt"))

message("[5/6] Done.")
message("Tables:  ", TAB_DIR)
message("Figures: ", FIG_DIR)
message("Summary: ", file.path(OUT_BASE, "coauthor_summary_1990_2019_us.txt"))
