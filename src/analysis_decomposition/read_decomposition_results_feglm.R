#!/usr/bin/env Rscript
###############################################################################
# read_decomposition_results_all_feglm.R
# Run decomposition analysis (regression, covariance, correlation)
# for all FEGLM models: baseline, top10, tenure, tenure-top10
# Author: Eugenio — 2025-10-24
###############################################################################

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("readr", "dplyr", "broom", "ggplot2")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))

# ----------------------------
# Config: list of decomposition files
# ----------------------------
OUT_DIR <- "/home/gps-yuhei/revelio_labs/output/regressions"

versions <- c(
  "baseline_feglm"#,
#   "baseline_top10_feglm",
#   "covariates_feglm",
#   "covariates_feglm_top10"
)

# pick specific year for decomposition
BASE_YEAR <- 2010

# ----------------------------
# Loop through versions
# ----------------------------
for (v in versions) {
  cat("\n============================================\n")
  cat("[INFO] Processing decomposition for:", v, "\n")
  cat("============================================\n")

  OUT_FILE <- file.path(OUT_DIR, paste0("decomposition_joined_", v, ".csv"))
  REG_RESULTS <- file.path(OUT_DIR, paste0("regression_decomposition_results_", v, ".csv"))
  COVAR_RESULTS <- file.path(OUT_DIR, paste0("covariate_decomposition_results_", v, ".csv"))

  if (!file.exists(OUT_FILE)) {
    cat("[WARN] File not found, skipping:", OUT_FILE, "\n")
    next
  }

  # ----------------------------
  # Load data
  # ----------------------------
  cat("[INFO] Reading decomposition file...\n")
  decomp <- read_csv(OUT_FILE, show_col_types = FALSE)
  cat("[INFO] Dataset loaded with", nrow(decomp), "rows and", ncol(decomp), "columns\n")
  cat("[INFO] Columns found:\n")
  print(colnames(decomp))

  # ----------------------------
  # City-level summary
  # ----------------------------
  cat("[INFO] Creating city-level summary...\n")

  city_summary <- decomp %>%
    filter(year == BASE_YEAR) %>%
    group_by(first_city) %>%
    summarise(
      mean_fe_city  = mean(fe_first_city, na.rm = TRUE),
      mean_fe_firm  = mean(fe_first_rcid, na.rm = TRUE),
      mean_fe_user  = mean(fe_user_id, na.rm = TRUE),
      n_obs = n(),
      .groups = "drop"
    ) %>%
    arrange(desc(n_obs))
  write_csv(city_summary, file.path(OUT_DIR, paste0("city_decomposition_summary_", v, ".csv")))

  # ----------------------------
  # Firm-level summary
  # ----------------------------
  cat("[INFO] Creating firm-level summary...\n")

  firm_summary <- decomp %>%
    filter(year == BASE_YEAR) %>%
    group_by(first_rcid) %>%
    summarise(
      mean_fe_firm = mean(fe_first_rcid, na.rm = TRUE),
      mean_fe_user = mean(fe_user_id, na.rm = TRUE),
      mean_fe_city = mean(fe_first_city, na.rm = TRUE),
      n_obs = n(),
      .groups = "drop"
    ) %>%
    arrange(desc(n_obs))
  write_csv(firm_summary, file.path(OUT_DIR, paste0("firm_decomposition_summary_", v, ".csv")))

  # ----------------------------
  # Regression-based decomposition (α)
  # ----------------------------
  cat("[INFO] Running regression-based decomposition (α)...\n")

  city_means <- decomp %>%
    filter(year == BASE_YEAR) %>%
    group_by(first_city) %>%
    summarise(mean_patents = mean(n_patents, na.rm = TRUE), .groups = "drop") %>%
    mutate(log_mean_patents = log(mean_patents))

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
  results_clean <- results_clean %>% mutate(alpha_norm = alpha / alpha_sum)

  write_csv(results_clean, REG_RESULTS)
  cat("[INFO] Regression-based decomposition saved:", REG_RESULTS, "\n\n")
  cat("[RESULTS - α Regression Coefficients]\n")
  print(results_clean)
  cat("\n--------------------------------------------\n")

  # ----------------------------
  # Covariance / Correlation of FEs
  # ----------------------------
  cat("[INFO] Computing covariance/correlation of FEs...\n")

  fe_data <- decomp %>% select(fe_user_id, fe_first_rcid, fe_first_city) %>% na.omit()
  cov_matrix <- cov(fe_data)
  cor_matrix <- cor(fe_data)

  write_csv(as.data.frame(cov_matrix),
            file.path(OUT_DIR, paste0("fe_covariance_matrix_", v, ".csv")))
  write_csv(as.data.frame(cor_matrix),
            file.path(OUT_DIR, paste0("fe_correlation_matrix_", v, ".csv")))

  cat("[INFO] Covariance matrix:\n")
  print(round(cov_matrix, 4))
  cat("\n[INFO] Correlation matrix:\n")
  print(round(cor_matrix, 4))
  cat("\n[INFO] Saved covariance/correlation matrices for", v, "\n")
  cat("--------------------------------------------\n")

  # ----------------------------
  # Covariate-based decomposition (β)
  # ----------------------------
  cat("[INFO] Running covariate-based decomposition (β)...\n")

  city_inventors <- decomp %>%
    filter(year == BASE_YEAR) %>%
    group_by(first_city) %>%
    summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
    mutate(log_inventors = log(n_inventors))

  city_cov_data <- city_summary %>%
    left_join(city_means, by = "first_city") %>%
    left_join(city_inventors, by = "first_city")

  city_cov_data <- city_cov_data %>%
   mutate(
     log_mean_patents = ifelse(is.infinite(log_mean_patents), NA, log_mean_patents),
     log_inventors = ifelse(is.infinite(log_inventors), NA, log_inventors),

     sum_fe = (mean_fe_city + mean_fe_firm + mean_fe_user)
    )

  # sanity check: correlation between log_mean_patents and sum_fe
  qplot(log_mean_patents, sum_fe, data = city_cov_data)

  cov_reg_original <- lm(log_mean_patents ~ log_inventors,
                      data = city_cov_data)
  cov_reg_total <- lm(sum_fe ~ log_inventors,
                      data = city_cov_data)
  cov_reg_user  <- lm(mean_fe_user ~ log_inventors, data = city_cov_data)
  cov_reg_firm  <- lm(mean_fe_firm ~ log_inventors, data = city_cov_data)
  cov_reg_city  <- lm(mean_fe_city ~ log_inventors, data = city_cov_data)

  cov_results <- bind_rows(
    tidy(cov_reg_original) %>% mutate(component = "Original"),
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
  cat("[INFO] Covariate-based decomposition saved:", COVAR_RESULTS, "\n\n")
  cat("[RESULTS - β Regression Coefficients]\n")
  print(cov_results_clean)
  cat("\n============================================\n")
  cat("[INFO] ✅ Completed decomposition for:", v, "\n")
  cat("============================================\n")
}

cat("\n[INFO] All decompositions completed successfully.\n")
