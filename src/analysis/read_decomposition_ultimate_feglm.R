#!/usr/bin/env Rscript
###############################################################################
# read_decomposition_ultimate_feglm.R
# Full decomposition (α, β, covariance/correlation)
# Works for all 9 PPML specs:
# (state/city/metro) × (rcid/parent/nofirm)
# Author: Eugenio — updated 2025-11-10
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("readr", "dplyr", "broom", "knitr")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))

# ============================
# Config
# ============================
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions/ppml_loop_runs"

geos  <- c("state", "city", "metro")
firms <- c("rcid", "parent", "nofirm")

# ============================
# Helper functions
# ============================
ucfirst <- function(x) paste0(toupper(substr(x, 1, 1)), substr(x, 2, nchar(x)))

extract_stats <- function(model, var_name) {
  s <- summary(model)
  tbl <- as.data.frame(s$coefficients)
  if (!(var_name %in% rownames(tbl))) return(data.frame())
  row <- tbl[var_name, ]
  data.frame(
    Estimate = as.numeric(row["Estimate"]),
    Std.Error = as.numeric(row["Std. Error"]),
    t.Value = as.numeric(row["t value"]),
    p.Value = as.numeric(row["Pr(>|t|)"])
  )
}

print_table <- function(title, df) {
  cat("\n-----------------------------\n")
  cat(title, "\n")
  cat("-----------------------------\n")
  print(knitr::kable(df, digits = 4))
  cat("\n")
}

# ============================
# Core Decomposition Function
# ============================
run_decomp <- function(decomp, loc_var, firm_fe_name, tag, OUT_DIR, suffix) {

  cat("\n[INFO] Starting decomposition for:", tag, "-", suffix, "\n")

  # ----------------------------
  # Filter Year
  # ----------------------------
  target_year <- "2018"
  decomp <- decomp %>% filter(year == target_year)

  if (nrow(decomp) == 0) {
    cat("[WARN] No rows for year 2018 in", tag, "→ skipping\n")
    return(NULL)
  }

  cat("[INFO] Filtered to year 2018, rows:", nrow(decomp), "\n")

  # ----------------------------
  # Mean variable selection
  # ----------------------------
  mean_var <- ifelse(suffix == "yhat", "yhat", "n_patents")

  # ----------------------------
  # Aggregation by location × year
  # ----------------------------
  agg <- decomp %>%
    group_by(.data[[loc_var]], year) %>%
    summarise(
      mean_val = mean(.data[[mean_var]], na.rm = TRUE),
      E_user   = mean(fe_user_id, na.rm = TRUE),
      E_loc    = mean(.data[[paste0("fe_", loc_var)]], na.rm = TRUE),
      E_year   = mean(fe_year, na.rm = TRUE),
      E_firm   = if (!is.null(firm_fe_name))
               mean(.data[[firm_fe_name]], na.rm = TRUE)
             else NA_real_,
      mean_tenure = mean(tenure, na.rm = TRUE),
      mean_tenure_sq = mean(tenure_sq, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(mean_val > 0) %>%
    mutate(log_E_yhat = log(mean_val))

  # ============================================================
  # α-DECOMPOSITION (user FE, location FE, year FE)
  # ============================================================
  cat("[INFO] Running α-decomposition...\n")

  reg_user <- lm(E_user ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg)
  reg_loc  <- lm(E_loc  ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg)
  reg_year <- lm(E_year ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg)
  if (!is.null(firm_fe_name)) {
    reg_firm <- lm(E_firm ~ log_E_yhat + mean_tenure + mean_tenure_sq, data = agg)
  }
  alpha <- bind_rows(
    cbind(Component = "Inventor_FE", extract_stats(reg_user, "log_E_yhat")),
    if (!is.null(firm_fe_name))
      cbind(Component = "Firm_FE", extract_stats(reg_firm, "log_E_yhat")),
    cbind(Component = paste0(ucfirst(loc_var), "_FE"), extract_stats(reg_loc, "log_E_yhat")),
    cbind(Component = "Year_FE", extract_stats(reg_year, "log_E_yhat"))
  )

  out_alpha <- file.path(OUT_DIR, paste0("alpha_", tag, "_", suffix, ".csv"))
  write_csv(alpha, out_alpha)
  print_table(paste0("α-Decomposition (", tag, ", ", suffix, ")"), alpha)

  # ============================================================
  # β-DECOMPOSITION (covariate: size = log #inventors)
  # ============================================================
  cat("[INFO] Running β-decomposition...\n")

  state_size <- decomp %>%
    group_by(.data[[loc_var]], year) %>%
    summarise(n_inventors = n_distinct(user_id), .groups = "drop") %>%
    mutate(X_c = log(n_inventors))

  cov_data <- agg %>%
    left_join(state_size, by = c(loc_var, "year")) %>%
    mutate(
      Total_FE = E_user +
                ifelse(is.na(E_firm), 0, E_firm) +
                E_loc
    )

  reg_total <- lm(log_E_yhat ~ X_c + mean_tenure + mean_tenure_sq, data = cov_data)
  reg_user2 <- lm(E_user      ~ X_c + mean_tenure + mean_tenure_sq, data = cov_data)
  reg_loc2  <- lm(E_loc       ~ X_c + mean_tenure + mean_tenure_sq, data = cov_data)
  if (!is.null(firm_fe_name)) {
    reg_firm2 <- lm(E_firm ~ X_c + mean_tenure + mean_tenure_sq, data = cov_data)
  }

  beta <- bind_rows(
     cbind(Component = "Total_FE", extract_stats(reg_total, "X_c")),
     cbind(Component = "Inventor_FE", extract_stats(reg_user2, "X_c")),
     if (!is.null(firm_fe_name))
       cbind(Component = "Firm_FE", extract_stats(reg_firm2, "X_c")),
     cbind(Component = paste0(ucfirst(loc_var), "_FE"), extract_stats(reg_loc2, "X_c"))
   )

  out_beta <- file.path(OUT_DIR, paste0("beta_", tag, "_", suffix, ".csv"))
  write_csv(beta, out_beta)
  print_table(paste0("β-Decomposition (", tag, ", ", suffix, ")"), beta)

  # ============================================================
  # Covariance / Correlation
  # ============================================================
  fe_data <- decomp %>% 
    select(fe_user_id, .data[[paste0("fe_", loc_var)]]) %>% 
    na.omit()

  cov_mat <- cov(fe_data)
  cor_mat <- cor(fe_data)

  write_csv(as.data.frame(cov_mat),
            file.path(OUT_DIR, paste0("covariance_", tag, "_", suffix, ".csv")))
  write_csv(as.data.frame(cor_mat),
            file.path(OUT_DIR, paste0("correlation_", tag, "_", suffix, ".csv")))

  cat("[INFO] Finished decomposition for", tag, "-", suffix, "\n")
}

# ============================
# MAIN LOOP — all 9 specs
# ============================

for (g in geos) {
  for (f in firms) {

    tag <- paste(g, f, sep = "_")

    # correct location variable
    loc_var <- dplyr::case_when(
      g == "state" ~ "first_state",
      g == "city"  ~ "first_city",
      g == "metro" ~ "first_metro_area"
    )

    # Load decomposition dataset
    decomp_file <- file.path(
      OUT_DIR,
      paste0("ppml_top10_", tag, "_decomposition.csv")
    )

    if (!file.exists(decomp_file)) {
      cat("[WARN] Missing:", decomp_file, "\n")
      next
    }

    decomp <- read_csv(decomp_file, show_col_types = FALSE)
    cat("\n============================================\n")
    cat("[INFO] Loaded:", tag, "-", nrow(decomp), "rows\n")
    cat("============================================\n")

    # Identify correct firm FE column name
    firm_fe_name <- if (f == "nofirm") NULL else
      paste0("fe_", ifelse(f == "rcid", "first_rcid", "first_parent_rcid"))

    # Run both yhat + mean decomposition
    run_decomp(decomp, loc_var, firm_fe_name, tag, OUT_DIR, "yhat")
    run_decomp(decomp, loc_var, firm_fe_name, tag, OUT_DIR, "mean")
  }
}

cat("\n[INFO] ALL DECOMPOSITIONS COMPLETED SUCCESSFULLY.\n")
