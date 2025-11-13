#!/usr/bin/env Rscript
###############################################################################
# read_decomposition_results_feglm_yhat_nofirmFE.R
# Full decomposition (α, β, covariance/correlation)
# Saves Estimate, Std.Error, t.Value, and p.Value
# Prints results in console for immediate inspection
# Author: Eugenio — updated 2025-11-10
###############################################################################

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("readr", "dplyr", "broom", "knitr")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))

# ----------------------------
# Config
# ----------------------------
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions"

versions <- c(
  "covariates_feglm_top10_nofirmFE_state",
  "covariates_feglm_top10_nofirmFE_city",
  "covariates_feglm_top10_nofirmFE_metro_area"
)

# ----------------------------
# Helper: Capitalize first letter
# ----------------------------
ucfirst <- function(x) paste0(toupper(substr(x, 1, 1)), substr(x, 2, nchar(x)))

# ----------------------------
# Helper: Extract regression stats
# ----------------------------
extract_stats <- function(model, var_name) {
  s <- summary(model)
  coef_tbl <- as.data.frame(s$coefficients)
  if (!(var_name %in% rownames(coef_tbl))) return(data.frame())
  row <- coef_tbl[var_name, ]
  data.frame(
    Estimate = as.numeric(row["Estimate"]),
    Std.Error = as.numeric(row["Std. Error"]),
    t.Value = as.numeric(row["t value"]),
    p.Value = as.numeric(row["Pr(>|t|)"])
  )
}

# ----------------------------
# Helper: Print regression table cleanly
# ----------------------------
print_table <- function(title, df) {
  cat("\n-----------------------------\n")
  cat(title, "\n")
  cat("-----------------------------\n")
  print(knitr::kable(df, digits = 4))
  cat("\n")
}

# ----------------------------
# Core function
# ----------------------------
run_decomposition <- function(decomp, loc_var, v, OUT_DIR, suffix) {

  cat("\n[INFO] Starting decomposition for:", v, "(", suffix, ")\n")

  target_year <- "2018"
  if ("year" %in% colnames(decomp)) {
    decomp <- decomp %>% filter(year == target_year)
    cat("[INFO] Filtered to year =", target_year, "rows:", nrow(decomp), "\n")
  }

  mean_var <- if (suffix == "yhat") "y_hat" else "n_patents"

  agg_state <- decomp %>%
    group_by(.data[[loc_var]], year) %>%
    summarise(
      mean_val = mean(.data[[mean_var]], na.rm = TRUE),
      E_user = mean(fe_user_id, na.rm = TRUE),
      E_loc = mean(.data[[paste0('fe_', loc_var)]], na.rm = TRUE),
      E_year = mean(fe_year, na.rm = TRUE),
      mean_tenure = if ("tenure" %in% names(.)) mean(tenure, na.rm = TRUE) else NA,
      mean_tenure_sq = if ("tenure_sq" %in% names(.)) mean(tenure_sq, na.rm = TRUE) else NA,
      .groups = "drop"
    ) %>%
    filter(mean_val > 0) %>%
    mutate(log_E_yhat = log(mean_val))

  has_tenure <- all(c("tenure", "tenure_sq") %in% colnames(decomp))
  if (has_tenure) {
     cat("\n[INFO] Controlling for tenure in all analyses for:", v, "(", suffix, ")\n")
  } else {
     cat("\n[INFO] Tenure not found for:", v, "(", suffix, ")\n")
  }

  # ----------------------------
  # α Decomposition
  # ----------------------------
  cat("[INFO] Running α-decomposition (Regression-based)...\n")

  if (grepl("covariates", v) && has_tenure) {
    reg_user <- lm(E_user ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg_state)
    reg_loc  <- lm(E_loc  ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg_state)
    reg_year <- lm(E_year ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg_state)
  } else {
    reg_user <- lm(E_user ~ log_E_yhat, data = agg_state)
    reg_loc  <- lm(E_loc  ~ log_E_yhat, data = agg_state)
    reg_year <- lm(E_year ~ log_E_yhat, data = agg_state)
  }

  alpha_results <- bind_rows(
    cbind(Component = "Inventor_FE", extract_stats(reg_user, "log_E_yhat")),
    cbind(Component = paste0(ucfirst(loc_var), "_FE"), extract_stats(reg_loc, "log_E_yhat")),
    cbind(Component = "Year_FE", extract_stats(reg_year, "log_E_yhat"))
  )

  out_alpha <- file.path(
    OUT_DIR, paste0("alpha_full_", suffix, "_", loc_var, "_", gsub("covariates_", "", v), ".csv")
  )
  write_csv(alpha_results, out_alpha)
  print_table(paste0("α-Decomposition (", loc_var, ", ", suffix, ")"), alpha_results)

  # ----------------------------
  # β Decomposition
  # ----------------------------
  cat("[INFO] Running β-decomposition (Covariate-based)...\n")

  state_inventors <- decomp %>%
    group_by(.data[[loc_var]], year) %>%
    summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
    mutate(X_c = log(n_inventors))

  state_cov_data <- agg_state %>%
    left_join(state_inventors, by = c(loc_var, "year")) %>%
    mutate(Total_FE = E_user + E_loc)

  if (grepl("covariates", v) && has_tenure) {
    cov_reg_total <- lm(log_E_yhat ~ X_c + mean_tenure + mean_tenure_sq, data = state_cov_data)
    cov_reg_user  <- lm(E_user ~ X_c + mean_tenure + mean_tenure_sq, data = state_cov_data)
    cov_reg_loc   <- lm(E_loc  ~ X_c + mean_tenure + mean_tenure_sq, data = state_cov_data)
  } else {
    cov_reg_total <- lm(log_E_yhat ~ X_c, data = state_cov_data)
    cov_reg_user  <- lm(E_user ~ X_c, data = state_cov_data)
    cov_reg_loc   <- lm(E_loc  ~ X_c, data = state_cov_data)
  }

  beta_results <- bind_rows(
    cbind(Component = "Total_FE", extract_stats(cov_reg_total, "X_c")),
    cbind(Component = "Inventor_FE", extract_stats(cov_reg_user, "X_c")),
    cbind(Component = paste0(ucfirst(loc_var), "_FE"), extract_stats(cov_reg_loc, "X_c"))
  )

  out_beta <- file.path(
    OUT_DIR, paste0("beta_full_", suffix, "_", loc_var, "_", gsub("covariates_", "", v), ".csv")
  )
  write_csv(beta_results, out_beta)
  print_table(paste0("β-Decomposition (", loc_var, ", ", suffix, ")"), beta_results)

  # ----------------------------
  # Covariance/Correlation
  # ----------------------------
  fe_data <- decomp %>% select(fe_user_id, .data[[paste0('fe_', loc_var)]]) %>% na.omit()
  cov_matrix <- cov(fe_data)
  cor_matrix <- cor(fe_data)

  write_csv(as.data.frame(cov_matrix),
            file.path(OUT_DIR, paste0("fe_covariance_", suffix, "_", loc_var, "_", gsub("covariates_", "", v), ".csv")))
  write_csv(as.data.frame(cor_matrix),
            file.path(OUT_DIR, paste0("fe_correlation_", suffix, "_", loc_var, "_", gsub("covariates_", "", v), ".csv")))

  cat("[INFO] ✅ Finished:", v, "(", suffix, ")\n")
}

# ----------------------------
# Main Loop
# ----------------------------
for (v in versions) {
  cat("\n============================================\n")
  cat("[INFO] Processing version:", v, "\n")
  cat("============================================\n")

  OUT_FILE <- file.path(OUT_DIR, paste0("decomposition_joined_", v, ".csv"))
  if (!file.exists(OUT_FILE)) {
    cat("[WARN] File not found, skipping:", OUT_FILE, "\n")
    next
  }

  loc_var <- if (grepl("city", v)) {
    "first_city"
  } else if (grepl("metro", v)) {
    "first_metro_area"
  } else if (grepl("state", v)) {
    "first_state"
  }

  decomp <- read_csv(OUT_FILE, show_col_types = FALSE)
  cat("[INFO] Loaded", nrow(decomp), "rows.\n")

  run_decomposition(decomp, loc_var, v, OUT_DIR, "yhat")
  run_decomposition(decomp, loc_var, v, OUT_DIR, "mean")
}

cat("\n[INFO] ✅ All decompositions completed successfully.\n")
