#!/usr/bin/env Rscript

###############################################################################
# read_decomposition_results_covariates_feglm.R
# Read and analyze decomposition_joined_covariates_feglm.csv
# from PPML (alpaca::feglm) regression with tenure controls
# Author: Eugenio — 2025-10-21
###############################################################################

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("readr", "dplyr", "broom")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(readr)
library(dplyr)
library(broom)

# ----------------------------
# Paths
# ----------------------------
OUT_DIR  <- "/home/epiga/revelio_labs/output/regressions/"
OUT_FILE <- file.path(OUT_DIR, "decomposition_joined_covariates_feglm.csv")

REG_RESULTS   <- file.path(OUT_DIR, "regression_decomposition_results_covariates_feglm.csv")
COVAR_RESULTS <- file.path(OUT_DIR, "covariate_decomposition_results_covariates_feglm.csv")

# ----------------------------
# Load data
# ----------------------------
cat("[INFO] Reading decomposition outputs...\n")

if (!file.exists(OUT_FILE)) {
  stop(paste0("[ERROR] File not found: ", OUT_FILE,
              "\nPlease ensure ppml_covariates_feglm.R finished successfully."))
}

decomp <- read_csv(OUT_FILE, show_col_types = FALSE)
cat("[INFO] Dataset loaded with", nrow(decomp), "rows and", ncol(decomp), "columns\n")
cat("[INFO] Columns:\n")
print(colnames(decomp))

# ----------------------------
# City-level summary
# ----------------------------
cat("\n[INFO] Example summary by city (top 10 by observations):\n")
city_summary <- decomp %>%
  group_by(first_city) %>%
  summarise(
    mean_fe_city  = mean(fe_first_city, na.rm = TRUE),
    mean_fe_firm  = mean(fe_first_rcid, na.rm = TRUE),
    mean_fe_user  = mean(fe_user_id, na.rm = TRUE),
    n_obs = n()
  ) %>%
  arrange(desc(n_obs))
print(head(city_summary, 10))

# ----------------------------
# Firm-level summary
# ----------------------------
cat("\n[INFO] Example summary by firm (top 10 by observations):\n")
firm_summary <- decomp %>%
  group_by(first_rcid) %>%
  summarise(
    mean_fe_firm = mean(fe_first_rcid, na.rm = TRUE),
    mean_fe_user = mean(fe_user_id, na.rm = TRUE),
    mean_fe_city = mean(fe_first_city, na.rm = TRUE),
    n_obs = n()
  ) %>%
  arrange(desc(n_obs))
print(head(firm_summary, 10))

# ----------------------------
# Save split CSVs
# ----------------------------
cat("\n[INFO] Saving decomposition splits by city and firm...\n")
write_csv(city_summary, file.path(OUT_DIR, "city_decomposition_summary_covariates_feglm.csv"))
write_csv(firm_summary, file.path(OUT_DIR, "firm_decomposition_summary_covariates_feglm.csv"))

cat("[INFO] Done. Results saved to:\n")
cat("  -", file.path(OUT_DIR, "city_decomposition_summary_covariates_feglm.csv"), "\n")
cat("  -", file.path(OUT_DIR, "firm_decomposition_summary_covariates_feglm.csv"), "\n")

# ----------------------------
# Regression-based decomposition
# ----------------------------
cat("\n[INFO] Running regression-based decomposition...\n")

city_means <- decomp %>%
  group_by(first_city) %>%
  summarise(mean_patents = mean(n_patents, na.rm = TRUE), .groups = "drop") %>%
  mutate(log_mean_patents = log1p(mean_patents))

city_data <- city_summary %>%
  left_join(city_means, by = "first_city")

reg_user <- lm(mean_fe_user ~ log_mean_patents, data = city_data)
reg_firm <- lm(mean_fe_firm ~ log_mean_patents, data = city_data)
reg_city <- lm(mean_fe_city ~ log_mean_patents, data = city_data)

results <- bind_rows(
  tidy(reg_user) %>% mutate(component = "Inventor_FE"),
  tidy(reg_firm) %>% mutate(component = "Firm_FE"),
  tidy(reg_city) %>% mutate(component = "City_FE")
)

results_clean <- results %>%
  filter(term == "log_mean_patents") %>%
  select(component, estimate, std.error, statistic, p.value) %>%
  rename(alpha = estimate)

alpha_sum <- sum(results_clean$alpha, na.rm = TRUE)
results_clean <- results_clean %>%
  mutate(alpha_norm = alpha / alpha_sum)

write_csv(results_clean, REG_RESULTS)

cat("[INFO] Regression-based decomposition results saved to:\n")
cat("  -", REG_RESULTS, "\n")
print(results_clean)

# ----------------------------
# Covariance / Correlation of FEs
# ----------------------------
cat("\n[INFO] Computing covariance and correlation of fixed effects...\n")

fe_data <- decomp %>%
  select(fe_user_id, fe_first_rcid, fe_first_city) %>%
  na.omit()

cov_matrix <- cov(fe_data)
cor_matrix <- cor(fe_data)

cat("\n[INFO] Covariance matrix:\n")
print(cov_matrix)
cat("\n[INFO] Correlation matrix:\n")
print(cor_matrix)

write_csv(as.data.frame(cov_matrix), file.path(OUT_DIR, "fe_covariance_matrix_covariates_feglm.csv"))
write_csv(as.data.frame(cor_matrix), file.path(OUT_DIR, "fe_correlation_matrix_covariates_feglm.csv"))

cat("[INFO] Saved covariance/correlation results to:\n")
cat("  -", file.path(OUT_DIR, "fe_covariance_matrix_covariates_feglm.csv"), "\n")
cat("  -", file.path(OUT_DIR, "fe_correlation_matrix_covariates_feglm.csv"), "\n")

# ----------------------------
# Covariate-based decomposition (optional)
# ----------------------------
cat("\n[INFO] Running covariate-based decomposition...\n")

city_inventors <- decomp %>%
  group_by(first_city) %>%
  summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
  mutate(log_inventors = log1p(n_inventors))

city_cov_data <- city_summary %>%
  left_join(city_inventors, by = "first_city")

cov_reg_total <- lm(log1p(mean_fe_city + mean_fe_firm + mean_fe_user) ~ log_inventors,
                    data = city_cov_data)
cov_reg_user  <- lm(mean_fe_user ~ log_inventors, data = city_cov_data)
cov_reg_firm  <- lm(mean_fe_firm ~ log_inventors, data = city_cov_data)
cov_reg_city  <- lm(mean_fe_city ~ log_inventors, data = city_cov_data)

cov_results <- bind_rows(
  tidy(cov_reg_total) %>% mutate(component = "Total_FE"),
  tidy(cov_reg_user)  %>% mutate(component = "Inventor_FE"),
  tidy(cov_reg_firm)  %>% mutate(component = "Firm_FE"),
  tidy(cov_reg_city)  %>% mutate(component = "City_FE")
)

cov_results_clean <- cov_results %>%
  filter(term == "log_inventors") %>%
  select(component, estimate, std.error, statistic, p.value) %>%
  rename(beta = estimate)

write_csv(cov_results_clean, COVAR_RESULTS)

cat("[INFO] Covariate-based decomposition results saved to:\n")
cat("  -", COVAR_RESULTS, "\n")
print(cov_results_clean)

cat("\n[INFO] All done ✅\n")
