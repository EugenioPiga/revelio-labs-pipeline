#!/usr/bin/env Rscript

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("arrow", "fixest", "dplyr", "broom")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(arrow)
library(fixest)
library(dplyr)
library(broom)

# ============================
# Config
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
OUT_FILE <- file.path(OUT_DIR, "akm_patents_3fe.rds")
LOG_FILE <- file.path(OUT_DIR, "akm_patents_3fe_summary.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_city) %>%
  collect()

df <- df %>%
  filter(!is.na(user_id), !is.na(n_patents), !is.na(first_rcid), !is.na(first_city))

cat("[INFO] Data loaded: ", nrow(df), " rows\n")

# ============================
# Run AKM regression (Poisson FE as proxy)
# ============================
cat("[INFO] Running AKM-style regression with fixest...\n")
akm_model <- feols(
  n_patents ~ 1 | user_id + first_rcid + first_city,
  data = df,
  family = "poisson"
)

# ============================
# Save results
# ============================
saveRDS(akm_model, OUT_FILE)

sink(LOG_FILE)
print(summary(akm_model))
sink()

cat("[INFO] Regression finished. Results saved to:\n")
cat("  - model object:", OUT_FILE, "\n")
cat("  - summary:", LOG_FILE, "\n")
