#!/usr/bin/env Rscript
###############################################################################
# read_decomposition_results_all_feglm.R
# Decomposition analysis (α, β, covariance/correlation)
# for baseline and covariates FEGLM models
# Author: Eugenio — updated 2025-10-30
###############################################################################

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("readr", "dplyr", "broom", "ggplot2", "ggrepel")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))

# ----------------------------
# Config: list of decomposition files
# ----------------------------
#OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"
OUT_DIR <- "/home/gps-yuhei/revelio_labs/output/regressions"
OUT_FIG <- "/home/gps-yuhei/code/revelio-labs-pipeline/output/figures/decomposition"

versions <- c(
  "baseline_feglm",
  "baseline_top10_feglm",
  "covariates_feglm",
  "covariates_feglm_top10"
)

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
  # Focus on one specific year
  # ----------------------------
  target_year <- "2018"
  if ("year" %in% colnames(decomp)) {
    decomp <- decomp %>% filter(year == target_year)
    cat("[INFO] Filtering decomposition to year =", target_year, "\n")
    cat("[INFO] Rows after year filter:", nrow(decomp), "\n")
  }

  # ----------------------------
  # Aggregation
  # ----------------------------
  cat("[INFO] Aggregating to city-year level...\n")
  agg_city <- decomp %>%
    group_by(first_city, year) %>%
    summarise(
      mean_patents = mean(n_patents, na.rm = TRUE),
      mean_patents_nonzero = mean(n_patents[n_patents > 0], na.rm = TRUE),
      num_inventors = n_distinct(user_id),
      E_user = mean(fe_user_id, na.rm = TRUE),
      E_firm = mean(fe_first_rcid, na.rm = TRUE),
      E_city = mean(fe_first_city, na.rm = TRUE),
      E_year = mean(fe_year, na.rm = TRUE),
      mean_tenure = if ("tenure" %in% names(.)) mean(tenure, na.rm = TRUE) else NA,
      mean_tenure_sq = if ("tenure_sq" %in% names(.)) mean(tenure_sq, na.rm = TRUE) else NA,
      .groups = "drop"
    ) %>%
    filter(mean_patents > 0) %>%
    mutate(log_E_patents = log(mean_patents))

  # sanity check: correlation between num_inventors and mean_patents
  inventors_patents_plot <- ggplot(
    agg_city,
    aes(x = log(num_inventors), y = log(mean_patents), label = first_city),
    #aes(x = log(num_inventors), y = log(mean_patents_nonzero), label = first_city)
  ) +
    geom_point(aes(size = num_inventors), alpha = 0.6) +
    ggrepel::geom_text_repel(size = 3, max.overlaps = Inf) +
    labs(
      title = "Inventors vs. Patents",
      x = "log(Number of inventors)",
      y = "log(Mean patents)",
      size = "Number of inventors"
    ) +
    scale_size_continuous(range = c(2, 8)) +
    theme_minimal()
  ggsave(
    file.path(OUT_FIG, paste0("inventors_vs_patents_", v, ".png")),
    inventors_patents_plot,
    width = 8,
    height = 6
  )

#  # sanity check: correlation between num_inventors and city FE
#   inventors_city_plot <- ggplot(
#     agg_city,
#     aes(x = log(num_inventors), y = E_city, label = first_city)
#   ) +
#     geom_point(aes(size = num_inventors), alpha = 0.6) +
#     ggrepel::geom_text_repel(size = 3, max.overlaps = Inf) +
#     labs(
#       title = "Inventors vs. City FE",
#       x = "log(Number of inventors)",
#       y = "City fixed effect (E_city)",
#       size = "Number of inventors"
#     ) +
#     scale_size_continuous(range = c(2, 8)) +
#     theme_minimal()
#   ggsave(
#     file.path(OUT_FIG, paste0("inventors_vs_city_fe_", v, ".png")),
#     inventors_city_plot,
#     width = 8,
#     height = 6
#   )

  # ----------------------------
  # (1) Regression-Based Decomposition (α)
  # ----------------------------
  cat("[INFO] Running regression-based decomposition (α)...\n")

  has_tenure <- all(c("tenure", "tenure_sq") %in% colnames(decomp))
  if (grepl("covariates", v) && has_tenure) {
    cat("[INFO] Tenure variables found — including them in α-decomposition.\n")
    reg_user <- lm(E_user ~ log_E_patents + mean_tenure + mean_tenure_sq, data = agg_city)
    reg_firm <- lm(E_firm ~ log_E_patents + mean_tenure + mean_tenure_sq, data = agg_city)
    reg_city <- lm(E_city ~ log_E_patents + mean_tenure + mean_tenure_sq, data = agg_city)
    reg_year <- lm(E_year ~ log_E_patents + mean_tenure + mean_tenure_sq, data = agg_city)
  } else {
    cat("[WARN] Tenure variables not found — running baseline α-decomposition.\n")
    reg_user <- lm(E_user ~ log_E_patents, data = agg_city)
    reg_firm <- lm(E_firm ~ log_E_patents, data = agg_city)
    reg_city <- lm(E_city ~ log_E_patents, data = agg_city)
    reg_year <- lm(E_year ~ log_E_patents, data = agg_city)
  }

  alpha_user <- coef(reg_user)["log_E_patents"]
  alpha_firm <- coef(reg_firm)["log_E_patents"]
  alpha_city <- coef(reg_city)["log_E_patents"]
  alpha_year <- coef(reg_year)["log_E_patents"]

  alpha_results <- data.frame(
    component = c("Inventor_FE", "Firm_FE", "City_FE", "Year_FE"),
    alpha = c(alpha_user, alpha_firm, alpha_city, alpha_year)
  )

  # Add tenure components if covariates version AND tenure exists
  if (grepl("covariates", v) && has_tenure) {
    alpha_tenure <- mean(coef(reg_user)["mean_tenure"], na.rm = TRUE)
    alpha_tenure_sq <- mean(coef(reg_user)["mean_tenure_sq"], na.rm = TRUE)
    alpha_results <- rbind(
      alpha_results,
      data.frame(component = "Tenure", alpha = alpha_tenure),
      data.frame(component = "Tenure_Sq", alpha = alpha_tenure_sq)
    )
  }

  alpha_results$alpha_sum <- sum(alpha_results$alpha, na.rm = TRUE)
  alpha_results <- alpha_results %>%
    mutate(alpha_norm = alpha / alpha_sum)

  write_csv(alpha_results, REG_RESULTS)
  cat("[RESULTS - α Decomposition]\n")
  print(alpha_results)

  # ----------------------------
  # (2) Covariate-Based Decomposition (β)
  # ----------------------------
  cat("[INFO] Running covariate-based decomposition (β)...\n")

  city_inventors <- decomp %>%
    group_by(first_city, year) %>%
    summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
    mutate(X_c = log(n_inventors))

  city_cov_data <- agg_city %>%
    left_join(city_inventors, by = c("first_city", "year")) %>%
    mutate(Total_FE = E_user + E_firm + E_city)

  if (grepl("covariates", v) && has_tenure) {
    cat("[INFO] Tenure variables found — including them in β-decomposition.\n")
    cov_reg_total <- lm(log_E_patents ~ X_c + mean_tenure + mean_tenure_sq, data = city_cov_data)
    cov_reg_user  <- lm(E_user ~ X_c + mean_tenure + mean_tenure_sq, data = city_cov_data)
    cov_reg_firm  <- lm(E_firm ~ X_c + mean_tenure + mean_tenure_sq, data = city_cov_data)
    cov_reg_city  <- lm(E_city ~ X_c + mean_tenure + mean_tenure_sq, data = city_cov_data)
  } else {
    cat("[WARN] Tenure variables not found — running baseline β-decomposition.\n")
    cov_reg_total <- lm(log_E_patents ~ X_c, data = city_cov_data)
    cov_reg_user  <- lm(E_user ~ X_c, data = city_cov_data)
    cov_reg_firm  <- lm(E_firm ~ X_c, data = city_cov_data)
    cov_reg_city  <- lm(E_city ~ X_c, data = city_cov_data)
  }

  beta_total <- coef(cov_reg_total)["X_c"]
  beta_user  <- coef(cov_reg_user)["X_c"]
  beta_firm  <- coef(cov_reg_firm)["X_c"]
  beta_city  <- coef(cov_reg_city)["X_c"]

  beta_results <- data.frame(
    component = c("Total_FE", "Inventor_FE", "Firm_FE", "City_FE"),
    beta = c(beta_total, beta_user, beta_firm, beta_city)
  )

  # Add tenure components only if covariates & tenure present
  if (grepl("covariates", v) && has_tenure) {
    beta_tenure <- mean(coef(cov_reg_user)["mean_tenure"], na.rm = TRUE)
    beta_tenure_sq <- mean(coef(cov_reg_user)["mean_tenure_sq"], na.rm = TRUE)
    beta_results <- rbind(
      beta_results,
      data.frame(component = "Tenure", beta = beta_tenure),
      data.frame(component = "Tenure_Sq", beta = beta_tenure_sq)
    )
  }

  beta_results$beta_sum <- sum(beta_results$beta[beta_results$component != "Total_FE"], na.rm = TRUE)

  write_csv(beta_results, COVAR_RESULTS)
  cat("[RESULTS - β Decomposition]\n")
  print(beta_results)
  cat("\n-------------------------------------------------\n")

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

  cat("[INFO] Covariance and correlation saved for", v, "\n")
}
cat("\n[INFO] All decompositions completed successfully.\n")
