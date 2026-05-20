#!/usr/bin/env Rscript

###############################################################################
# ppml_patents_covariates.R (penppml version)
# Estimate PPML with tenure controls and multi-dimensional FEs using penppml
# Author: Eugenio
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("arrow", "penppml", "dplyr", "readr", "broom", "lubridate")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(arrow)
library(dplyr)
library(readr)
library(broom)
library(lubridate)
library(penppml) 
cat("[DEBUG] penppml loaded:", "penppml" %in% loadedNamespaces(), "\n")

# ============================
# Config
# ============================
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
COV_FILE <- file.path(OUT_DIR, "ppml_cov_penppml.rds")
SUMMARY_FILE <- file.path(OUT_DIR, "ppml_cov_penppml_summary.txt")

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load data
# ============================
cat("[INFO] Reading parquet file...\n")
global_start <- Sys.time()

df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_rcid, first_city, year,
         first_startdate_edu, first_startdate_pos) %>%
  collect()

cat("[INFO] Data loaded:", nrow(df), "rows\n")

# ============================
# Create covariates (TENURE and TENURE^2)
# ============================
cat("[INFO] Creating tenure controls...\n")

df <- df %>%
  mutate(
    edu_start_year = as.numeric(substr(first_startdate_edu, 1, 4)),
    pos_start_year = as.numeric(substr(first_startdate_pos, 1, 4)),

    # Construct TENURE
    tenure = case_when(
      !is.na(pos_start_year) ~ year - pos_start_year,
      is.na(pos_start_year) & !is.na(edu_start_year) ~ year - edu_start_year + 3,
      TRUE ~ NA_real_
    ),
    tenure_sq = tenure^2
  ) %>%
  # remove pre-career years (negative tenure)
  filter(tenure >= 0)

cat("[INFO] Tenure controls created. Sample size after filtering:", nrow(df), "\n")

df %>%
  group_by(user_id) %>%
  summarise(
    var_tenure = var(tenure, na.rm = TRUE),
    var_tenure_sq = var(tenure_sq, na.rm = TRUE)
  ) %>%
  summarise(
    zero_var_share = mean(var_tenure == 0 | is.na(var_tenure))
  )

df <- df %>%
  filter(!is.na(n_patents), !is.na(user_id), !is.na(first_rcid),
         !is.na(first_city), !is.na(year), n_patents >= 0)

cat("[INFO] Sample after cleaning:", nrow(df), "rows\n")

summary(df$n_patents)
mean(df$n_patents == 0)


# ============================
# Run PPML (penhdfeppml) with tenure controls
# ============================
cat("[INFO] Running PPML with tenure controls using penhdfeppml...\n")
start_cov <- Sys.time()

df <- df %>%
  filter(!is.na(n_patents)) %>%
  mutate(n_patents = ifelse(n_patents < 0, 0, n_patents))


ppml_cov <- penhdfeppml(
  data = df,
  dep = "n_patents",
  indep = c("tenure", "tenure_sq"),
  fixed = c("User_id", "first_rcid", "first_city", "year"),
  penalty = "ridge",
  method = "plugin",
  tol = 1e-8,
  verbose = TRUE
)

end_cov <- Sys.time()
runtime_cov <- as.numeric(difftime(end_cov, start_cov, units = "secs"))
cat("[INFO] Covariate PPML runtime:", runtime_cov, "seconds\n")

# ============================
# Save model and summary
# ============================
saveRDS(ppml_cov, COV_FILE)

sink(SUMMARY_FILE)
cat("=== PPML (penppml) with Tenure Controls ===\n")
print(summary(ppml_cov))
cat("\nRuntime (seconds):", runtime_cov, "\n")
sink()
cat("[INFO] Summary saved to:", SUMMARY_FILE, "\n")

# ============================
# Extract and save Fixed Effects
# ============================
cat("[INFO] Extracting fixed effects and building decomposition file...\n")

FE_DIR <- file.path(OUT_DIR, "ppml_cov_penppml_fe")
dir.create(FE_DIR, showWarnings = FALSE, recursive = TRUE)

# Extract fixef from penppml object
fe_list <- compute_fes(ppml_cov)

for (fe_name in names(fe_list)) {
  fe_df <- tibble(level = names(fe_list[[fe_name]]), fe = as.numeric(fe_list[[fe_name]]))
  out_path <- file.path(FE_DIR, paste0("fe_", fe_name, ".csv"))
  write_csv(fe_df, out_path)
  cat("[INFO] Saved FE for", fe_name, "to", out_path, "\n")
}

# ----------------------------
# Merge fixed effects with base data
# ----------------------------
cat("[INFO] Merging fixed effects into a single decomposition dataset...\n")

df_base <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, first_rcid, first_city, year, n_patents) %>%
  collect()

read_fe <- function(name, key) {
  path <- file.path(FE_DIR, paste0("fe_", name, ".csv"))
  if (file.exists(path)) {
    read_csv(path, show_col_types = FALSE) %>%
      rename(!!key := level, !!paste0("fe_", key) := fe)
  } else {
    NULL
  }
}

fe_user  <- read_fe("user_id", "user_id")
fe_rcid  <- read_fe("first_rcid", "first_rcid")
fe_city  <- read_fe("first_city", "first_city")
fe_year  <- read_fe("year", "year")

# ----------------------------
# Harmonize ID types
# ----------------------------
cat("[INFO] Harmonizing ID types...\n")

df_base <- df_base %>%
  mutate(
    user_id = as.character(user_id),
    first_rcid = as.character(first_rcid),
    first_city = as.character(first_city),
    year = as.character(year)
  )

if (!is.null(fe_user)) fe_user <- fe_user %>% mutate(user_id = as.character(user_id))
if (!is.null(fe_rcid)) fe_rcid <- fe_rcid %>% mutate(first_rcid = as.character(first_rcid))
if (!is.null(fe_city)) fe_city <- fe_city %>% mutate(first_city = as.character(first_city))
if (!is.null(fe_year)) fe_year <- fe_year %>% mutate(year = as.character(year))

# ----------------------------
# Merge all FEs back to base data
# ----------------------------
cat("[INFO] Merging FEs with main data...\n")

decomp <- df_base
if (!is.null(fe_user))  decomp <- decomp %>% left_join(fe_user,  by = "user_id")
if (!is.null(fe_rcid))  decomp <- decomp %>% left_join(fe_rcid,  by = "first_rcid")
if (!is.null(fe_city))  decomp <- decomp %>% left_join(fe_city,  by = "first_city")
if (!is.null(fe_year))  decomp <- decomp %>% left_join(fe_year,  by = "year")

# ----------------------------
# Save decomposition file
# ----------------------------
out_decomp <- file.path(OUT_DIR, "decomposition_joined_penppml.csv")
write_csv(decomp, out_decomp)
cat("[INFO] Decomposition file saved to:", out_decomp, "\n")

# ----------------------------
# Runtime summary
# ----------------------------
global_end <- Sys.time()
cat("[INFO] Total runtime (seconds):", round(as.numeric(global_end - global_start), 2), "\n")
cat("[INFO] PPML (penppml) with tenure controls and decomposition completed successfully.\n")
