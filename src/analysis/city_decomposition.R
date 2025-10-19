#!/usr/bin/env Rscript

# ============================
# Setup
# ============================

# Personal library
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("arrow", "fixest", "dplyr", "readr", "broom")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(arrow)
library(fixest)
library(dplyr)
library(readr)
library(broom)

# ============================
# Config
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
FE_DIR <- "/home/epiga/revelio_labs/output/regressions/ppml_patents_3fe_fe"
OUT_FILE <- "/home/epiga/revelio_labs/output/regressions/decomposition_joined.csv"

# ============================
# Load data
# ============================
cat("[INFO] Reading inventor-year parquet...\n")
df <- open_dataset(INPUT, format = "parquet") %>% collect()

df <- df %>%
  select(user_id, n_patents, first_rcid, first_city, year) %>%
  filter(!is.na(user_id) & !is.na(n_patents) &
         !is.na(first_rcid) & !is.na(first_city))

cat("[INFO] Data rows after filtering: ", nrow(df), "\n")

# ============================
# Load Fixed Effects
# ============================
cat("[INFO] Reading fixed effects...\n")
fe_user <- read_csv(file.path(FE_DIR, "fe_user_id.csv"))
fe_firm <- read_csv(file.path(FE_DIR, "fe_first_rcid.csv"))
fe_city <- read_csv(file.path(FE_DIR, "fe_first_city.csv"))
fe_year <- read_csv(file.path(FE_DIR, "fe_year.csv"))

# ============================
# Harmonize ID types
# ============================
df <- df %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_city = as.character(first_city),
    year = as.character(year)
  )

fe_user <- fe_user %>% mutate(level = as.character(level))
fe_firm <- fe_firm %>% mutate(level = as.character(level))
fe_city <- fe_city %>% mutate(level = as.character(level))
fe_year <- fe_year %>% mutate(level = as.character(level))

# ============================
# Merge with FE
# ============================
cat("[INFO] Joining with FE...\n")

df_joined <- df %>%
  left_join(fe_user, by = c("user_id" = "level"), multiple = "all") %>%
  rename(fe_user = fe) %>%
  left_join(fe_firm, by = c("first_rcid" = "level"), multiple = "all") %>%
  rename(fe_firm = fe) %>%
  left_join(fe_city, by = c("first_city" = "level"), multiple = "all") %>%
  rename(fe_city = fe) %>%
  left_join(fe_year, by = c("year" = "level"), multiple = "all") %>%
  rename(fe_year = fe)

cat("[INFO] Joined data rows: ", nrow(df_joined), "\n")

# ============================
# Save results
# ============================
write_csv(df_joined, OUT_FILE)
cat("[INFO] Decomposition dataset saved to: ", OUT_FILE, "\n")
