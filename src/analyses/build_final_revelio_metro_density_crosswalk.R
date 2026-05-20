#!/usr/bin/env Rscript

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("readr", "dplyr", "stringr", "tidyr")

for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(readr)
library(dplyr)
library(stringr)
library(tidyr)

IN_DIR <- "/home/epiga/revelio_labs/output/metro_density_match_check/diagnostics"

AUTO_MATCH_FILE <- file.path(IN_DIR, "revelio_metro_to_cbsa_density_matches.csv")
MANUAL_FILE     <- file.path(IN_DIR, "manual_revelio_top100_to_cbsa_density_crosswalk.csv")
REVELIO_FILE    <- file.path(IN_DIR, "revelio_unique_metro_values.csv")
CENSUS_FILE     <- file.path(IN_DIR, "census_cbsa_population_land_density_2023.csv")

OUT_FINAL   <- file.path(IN_DIR, "final_revelio_metro_density_crosswalk.csv")
OUT_SUMMARY <- file.path(IN_DIR, "final_revelio_metro_density_match_summary.csv")
OUT_TOP_UNMATCHED <- file.path(IN_DIR, "final_top_unmatched_revelio_metros.csv")
OUT_TOP_DENSITY <- file.path(IN_DIR, "final_top_density_revelio_metros.csv")
OUT_TOP_SUPPORT <- file.path(IN_DIR, "final_top_supported_matched_metros.csv")

message("[INFO] Reading inputs.")

revelio <- read_csv(REVELIO_FILE, show_col_types = FALSE)
census  <- read_csv(CENSUS_FILE, show_col_types = FALSE)

auto <- read_csv(AUTO_MATCH_FILE, show_col_types = FALSE) %>%
  transmute(
    revelio_metro_raw,
    cbsa_geoid = as.character(cbsa_geoid),
    cbsa_name,
    population_2023_acs5,
    land_area_sqmi,
    land_area_sqkm,
    density_per_sqmi,
    density_per_sqkm,
    match_source = "automatic",
    match_confidence = "auto",
    match_note = match_method
  )

manual_raw <- read_csv(MANUAL_FILE, show_col_types = FALSE)

manual <- manual_raw %>%
  transmute(
    revelio_metro_raw,
    cbsa_geoid = as.character(manual_cbsa_geoid),
    cbsa_name = manual_cbsa_name,
    population_2023_acs5,
    land_area_sqmi,
    land_area_sqkm,
    density_per_sqmi,
    density_per_sqkm,
    match_source = ifelse(
      is.na(manual_cbsa_geoid),
      "manual_unmatched_or_nonmetro",
      "manual"
    ),
    match_confidence = manual_match_confidence,
    match_note = manual_match_note
  )

message("[INFO] Rows:")
message("  Revelio unique metros: ", nrow(revelio))
message("  Automatic matches:     ", nrow(auto))
message("  Manual crosswalk rows: ", nrow(manual))

# Manual crosswalk should override automatic if both exist.
combined_matches <- bind_rows(
  auto %>% mutate(priority = 2L),
  manual %>% mutate(priority = 1L)
) %>%
  group_by(revelio_metro_raw) %>%
  arrange(priority, .by_group = TRUE) %>%
  slice_head(n = 1) %>%
  ungroup() %>%
  select(-priority)

final_crosswalk <- revelio %>%
  left_join(combined_matches, by = "revelio_metro_raw") %>%
  mutate(
    cbsa_geoid = as.character(cbsa_geoid),
    has_cbsa_match = !is.na(cbsa_geoid) & cbsa_geoid != "",
    has_valid_density = has_cbsa_match &
      is.finite(density_per_sqmi) &
      density_per_sqmi > 0,
    log_density_per_sqmi = ifelse(
      has_valid_density,
      log(density_per_sqmi),
      NA_real_
    ),
    log_population_2023_acs5 = ifelse(
      has_cbsa_match & is.finite(population_2023_acs5) & population_2023_acs5 > 0,
      log(population_2023_acs5),
      NA_real_
    )
  ) %>%
  arrange(desc(n_inventor_years))

summary <- tibble(
  n_revelio_unique_metros = nrow(final_crosswalk),
  n_matched_unique_metros = sum(final_crosswalk$has_cbsa_match, na.rm = TRUE),
  n_unmatched_unique_metros = sum(!final_crosswalk$has_cbsa_match, na.rm = TRUE),
  unique_metro_match_rate = mean(final_crosswalk$has_cbsa_match, na.rm = TRUE),

  inventor_years_total = sum(final_crosswalk$n_inventor_years, na.rm = TRUE),
  inventor_years_matched = sum(final_crosswalk$n_inventor_years[final_crosswalk$has_cbsa_match], na.rm = TRUE),
  inventor_year_match_rate = inventor_years_matched / inventor_years_total,

  users_total_approx = sum(final_crosswalk$n_users, na.rm = TRUE),
  users_matched_approx = sum(final_crosswalk$n_users[final_crosswalk$has_cbsa_match], na.rm = TRUE),
  user_match_rate_approx = users_matched_approx / users_total_approx,

  total_patents_all = sum(final_crosswalk$total_patents, na.rm = TRUE),
  total_patents_matched = sum(final_crosswalk$total_patents[final_crosswalk$has_cbsa_match], na.rm = TRUE),
  patent_match_rate = total_patents_matched / total_patents_all,

  n_valid_density_unique_metros = sum(final_crosswalk$has_valid_density, na.rm = TRUE),
  inventor_years_valid_density = sum(final_crosswalk$n_inventor_years[final_crosswalk$has_valid_density], na.rm = TRUE),
  inventor_year_valid_density_rate = inventor_years_valid_density / inventor_years_total
)

by_source <- final_crosswalk %>%
  mutate(match_source = ifelse(is.na(match_source), "unmatched", match_source)) %>%
  group_by(match_source, match_confidence) %>%
  summarise(
    n_unique_metros = n(),
    n_inventor_years = sum(n_inventor_years, na.rm = TRUE),
    n_users = sum(n_users, na.rm = TRUE),
    total_patents = sum(total_patents, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  arrange(desc(n_inventor_years))

top_unmatched <- final_crosswalk %>%
  filter(!has_cbsa_match) %>%
  arrange(desc(n_inventor_years)) %>%
  slice_head(n = 100)

top_density <- final_crosswalk %>%
  filter(has_valid_density) %>%
  arrange(desc(density_per_sqmi)) %>%
  slice_head(n = 100)

top_supported <- final_crosswalk %>%
  filter(has_valid_density) %>%
  arrange(desc(n_inventor_years)) %>%
  slice_head(n = 100)

message("[INFO] Final match summary:")
print(summary)

message("[INFO] Match by source:")
print(by_source)

message("[INFO] Top 30 unmatched after manual crosswalk:")
print(top_unmatched %>% select(revelio_metro_raw, n_inventor_years, n_users, total_patents, match_note) %>% slice_head(n = 30))

message("[INFO] Top 30 matched by inventor-year support:")
print(top_supported %>% select(revelio_metro_raw, cbsa_geoid, cbsa_name, n_inventor_years, n_users, density_per_sqmi, match_source, match_confidence) %>% slice_head(n = 30))

message("[INFO] Top 30 matched by density:")
print(top_density %>% select(revelio_metro_raw, cbsa_geoid, cbsa_name, n_inventor_years, density_per_sqmi, match_source, match_confidence) %>% slice_head(n = 30))

write_csv(final_crosswalk, OUT_FINAL)
write_csv(summary, OUT_SUMMARY)
write_csv(top_unmatched, OUT_TOP_UNMATCHED)
write_csv(top_density, OUT_TOP_DENSITY)
write_csv(top_supported, OUT_TOP_SUPPORT)

message("[INFO] Saved:")
message("  ", OUT_FINAL)
message("  ", OUT_SUMMARY)
message("  ", OUT_TOP_UNMATCHED)
message("  ", OUT_TOP_DENSITY)
message("  ", OUT_TOP_SUPPORT)

message("[INFO] DONE.")
