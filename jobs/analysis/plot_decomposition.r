###############################################################################
# Local Decomposition + Regression + Plot Script
# Author: Eugenio — flexible windows (5y, full sample)
# α (weighted only):
#   - log_yhat (weighted)
#   - log_mean_patents (weighted)
# β (weighted only), combined table:
#   - Observed_log_mean_patent
#   - Total_FE (log_yhat)
#   - Inventor_FE
#   - Location_FE
#   - Tenure (b1*E[tenure] + b2*E[tenure^2])
###############################################################################

library(readr)
library(dplyr)
library(ggplot2)
library(ggrepel)
library(knitr)

setwd("C:/Users/eugen/OneDrive - UC San Diego/Desktop/Research/LinkedIn Innovation/Local Analyses/01_Data")

# ==========================================================
# Helper: clean theme
# ==========================================================
theme_clean <- function() {
  theme_minimal(base_size = 13) +
    theme(
      panel.background = element_rect(fill = "white", color = NA),
      plot.background = element_rect(fill = "white", color = NA),
      panel.grid.minor = element_blank(),
      panel.grid.major.x = element_blank(),
      panel.grid.major.y = element_line(color = "gray70", linewidth = 0.3),
      axis.text  = element_text(color = "black"),
      axis.title = element_text(color = "black"),
      plot.title = element_text(face = "bold", hjust = 0.5, size = 12, color = "black")
    )
}

ucfirst <- function(x) paste0(toupper(substr(x, 1, 1)), substr(x, 2, nchar(x)))

# ==========================================================
# Regression helper functions
# ==========================================================
extract_stats <- function(model, var) {
  s   <- summary(model)
  tbl <- as.data.frame(s$coefficients)
  if (!(var %in% rownames(tbl))) return(data.frame())
  row <- tbl[var, ]
  data.frame(
    Estimate  = round(row["Estimate"],    4),
    Std.Error = round(row["Std. Error"],  4),
    t.Value   = round(row["t value"],     2),
    p.Value   = round(row["Pr(>|t|)"],    3)
  )
}

print_table <- function(title, df) {
  cat("\n---------------------------------\n")
  cat(title, "\n")
  cat("---------------------------------\n")
  print(knitr::kable(df, digits = 4))
  cat("\n")
}

# ==========================================================
# GLOBAL STORAGE FOR ALL REGRESSION OUTPUT
# ==========================================================
all_regression_results <- list()

# ==========================================================
# Load PPML model to extract tenure coefficients
# (metro-area model RDS placed in this directory)
# ==========================================================
ppml_model <- readRDS("ppml_covariates_feglm_top10_nofirmFE_metro_area.rds")

coef_tenure    <- coef(ppml_model)["tenure"]
coef_tenure_sq <- coef(ppml_model)["tenure_sq"]

cat("[INFO] Loaded tenure coefficients from RDS:\n")
cat("  tenure     =", coef_tenure, "\n")
cat("  tenure_sq  =", coef_tenure_sq, "\n")

# ==========================================================
# Main decomposition + plot function
# ==========================================================
plot_decomp <- function(df, loc_var, title_prefix, out_prefix, label_locations = TRUE) {

  # --------------------------------------------------------
  # Aggregate: location-year level
  # --------------------------------------------------------
  df_summary <- df %>%
    group_by(across(all_of(c(loc_var, "year")))) %>%
    summarise(
      n_inventors   = n_distinct(user_id),
      mean_patents  = mean(n_patents, na.rm = TRUE),
      mean_log_yhat = mean(log(y_hat), na.rm = TRUE),

      # tenure contributions using the true estimated coefficients
      mean_tenure    = coef_tenure    * mean(tenure,    na.rm = TRUE),
      mean_tenure_sq = coef_tenure_sq * mean(tenure_sq, na.rm = TRUE),

      fe_loc  = mean(.data[[paste0("fe_", loc_var)]], na.rm = TRUE),
      fe_user = mean(fe_user_id, na.rm = TRUE),
      fe_time = mean(fe_year, na.rm=TRUE),
      .groups = "drop"
    ) %>%
    # keep well-defined observations
    filter(
      !is.na(fe_loc),
      n_inventors > 0,
      mean_patents > 0,
      !is.na(mean_log_yhat)
    ) %>%
    mutate(
      log_n_inv   = log(n_inventors),
      log_yhat    = mean_log_yhat,
      log_mean_y  = log(mean_patents),
      # total tenure component (b1*E[tenure] + b2*E[tenure²])
      tenure_contrib = mean_tenure + mean_tenure_sq
    )

  # --------------------------------------------------------
  # α-DECOMPOSITION: y_hat (weighted only)
  #   fe_user, fe_loc ~ log_yhat
  # --------------------------------------------------------
  reg_user_yhat_w <- lm(fe_user ~ log_yhat, data = df_summary, weights = n_inventors)
  reg_loc_yhat_w  <- lm(fe_loc  ~ log_yhat, data = df_summary, weights = n_inventors)
  reg_time_yhat_w  <- lm(fe_time  ~ log_yhat, data = df_summary, weights = n_inventors)
 

  alpha_yhat_w <- bind_rows(
    cbind(
      Component = "Inventor_FE (weighted)",
      extract_stats(reg_user_yhat_w, "log_yhat")
    ),
    cbind(
      Component = paste0(ucfirst(gsub("first_", "", loc_var)), "_FE (weighted)"),
      extract_stats(reg_loc_yhat_w, "log_yhat")
    ),
    cbind(
      Component = paste0("Time_FE (weighted)"),
      extract_stats(reg_time_yhat_w, "log_yhat")
    )
  )

  write_csv(alpha_yhat_w, paste0(out_prefix, "_alpha_yhat_weighted.csv"))
  print_table(paste0(title_prefix, " – α Decomposition (log_yhat, weighted)"),
              alpha_yhat_w)

  # --------------------------------------------------------
  # α-DECOMPOSITION: mean_patents (weighted only)
  #   fe_user, fe_loc ~ log_mean_y
  # --------------------------------------------------------
  reg_user_ymean_w <- lm(fe_user ~ log_mean_y, data = df_summary, weights = n_inventors)
  reg_loc_ymean_w  <- lm(fe_loc  ~ log_mean_y, data = df_summary, weights = n_inventors)
  reg_time_ymean_w  <- lm(fe_time  ~ log_mean_y, data = df_summary, weights = n_inventors)


  alpha_ymean_w <- bind_rows(
    cbind(
      Component = "Inventor_FE (weighted)",
      extract_stats(reg_user_ymean_w, "log_mean_y")
    ),
    cbind(
      Component = paste0(ucfirst(gsub("first_", "", loc_var)), "_FE (weighted)"),
      extract_stats(reg_loc_ymean_w, "log_mean_y")
    )
    ,
    cbind(
      Component = paste0("Time_FE (weighted)"),
      extract_stats(reg_time_ymean_w, "log_mean_y")
    )
  )

  write_csv(alpha_ymean_w, paste0(out_prefix, "_alpha_meanY_weighted.csv"))
  print_table(paste0(title_prefix, " – α Decomposition (log_mean_patents, weighted)"),
              alpha_ymean_w)

  # --------------------------------------------------------
  # β-DECOMPOSITION: combined table (weighted only)
  #   All at location-year level
  # --------------------------------------------------------
  cat("[INFO] Running β-decomposition (combined, weighted)...\n")

  # Weighted regressions
  reg_obs_log_mean   <- lm(log_mean_y     ~ log_n_inv, data = df_summary, weights = n_inventors)
  reg_total_yhat_w   <- lm(log_yhat       ~ log_n_inv, data = df_summary, weights = n_inventors)
  reg_user_yhat_w    <- lm(fe_user        ~ log_n_inv, data = df_summary, weights = n_inventors)
  reg_loc_yhat_w     <- lm(fe_loc         ~ log_n_inv, data = df_summary, weights = n_inventors)
  reg_time_yhat_w    <- lm(fe_time         ~ log_n_inv, data = df_summary, weights = n_inventors)
  reg_tenure_w       <- lm(tenure_contrib ~ log_n_inv, data = df_summary, weights = n_inventors)

  # Location label in the Component column
  loc_component_name <- if (loc_var == "first_metro_area") {
    "Metro_area"
  } else {
    "State_FE"
  }

  beta_combined_w <- bind_rows(
    cbind(
      Component = "Observed_log_mean_patent",
      extract_stats(reg_obs_log_mean, "log_n_inv")
    ),
    cbind(
      Component = "Total_FE",
      extract_stats(reg_total_yhat_w, "log_n_inv")
    ),
    cbind(
      Component = "Inventor_FE",
      extract_stats(reg_user_yhat_w, "log_n_inv")
    ),
    cbind(
      Component = loc_component_name,
      extract_stats(reg_loc_yhat_w, "log_n_inv")
    ),
    cbind(
      Component = "Time_FE",
      extract_stats(reg_time_yhat_w, "log_n_inv")
    ),
    cbind(
      Component = "Tenure",
      extract_stats(reg_tenure_w, "log_n_inv")
    )
  )

  write_csv(beta_combined_w, paste0(out_prefix, "_beta_combined_weighted.csv"))
  print_table(paste0(title_prefix, " – β Decomposition (combined, weighted)"),
              beta_combined_w)

  # --------------------------------------------------------
  # Save α and β tables into global list for TXT export
  # --------------------------------------------------------
  all_regression_results[[out_prefix]] <<- list(
    alpha_yhat_w    = alpha_yhat_w,
    alpha_ymean_w   = alpha_ymean_w,
    beta_combined_w = beta_combined_w
  )

  # ======================================================
  # FE plots — LOCATION FE vs log_n_inv
  # ======================================================
  top_labels <- df_summary %>%
    arrange(desc(n_inventors)) %>%
    slice_head(n = 15) %>%
    pull(all_of(loc_var))

  make_FE_plot <- function(weighted = FALSE) {
    label_data <- df_summary %>% filter(.data[[loc_var]] %in% top_labels)

    ggplot(df_summary, aes(x = log_n_inv, y = fe_loc, size = n_inventors)) +
      geom_point(color = "black", fill = "grey70", alpha = 0.9,
                 shape = 21, stroke = 0.5) +
      geom_smooth(
        method = "lm", se = FALSE, color = "black", linewidth = 0.9,
        aes(weight = n_inventors)
      ) +
      geom_text_repel(
        data = label_data,
        aes_string(label = loc_var),
        size = 3.2,
        max.overlaps = 15
      ) +
      scale_size(range = c(2.5, 9)) +
      labs(
        title = paste0(
          title_prefix,
          " — Location FE vs Inventors (Weighted)"
        ),
        x = "log(Number of inventors)",
        y = paste0("Location FE (", gsub("first_", "", loc_var), ")")
      ) +
      theme_clean()
  }

  fe_w <- make_FE_plot(TRUE)

  ggsave(paste0(out_prefix, "_FE_location_weighted.png"), fe_w,
         width = 7.5, height = 5, dpi = 400)

  # ======================================================
  # FE plots — INVENTOR FE vs log_n_inv
  # ======================================================
  make_FEinventor_plot <- function(weighted = FALSE) {
    label_data <- df_summary %>% filter(.data[[loc_var]] %in% top_labels)

    ggplot(df_summary, aes(x = log_n_inv, y = fe_user, size = n_inventors)) +
      geom_point(color = "black", fill = "grey70", alpha = 0.9,
                 shape = 21, stroke = 0.5) +
      geom_smooth(
        method = "lm", se = FALSE, color = "black", linewidth = 0.9,
        aes(weight = n_inventors)
      ) +
      geom_text_repel(
        data = label_data,
        aes_string(label = loc_var),
        size = 3.2,
        max.overlaps = 15
      ) +
      scale_size(range = c(2.5, 9)) +
      labs(
        title = paste0(
          title_prefix,
          " — Inventor FE vs Inventors (Weighted)"
        ),
        x = "log(Number of inventors)",
        y = "Inventor FE"
      ) +
      theme_clean()
  }

  fe_inv_w <- make_FEinventor_plot(TRUE)

  ggsave(paste0(out_prefix, "_FEinventor_weighted.png"), fe_inv_w,
         width = 7.5, height = 5, dpi = 400)

  cat("[INFO] Saved plots and tables for", out_prefix, "\n")
}

# ==========================================================
# TIME WINDOW DEFINITIONS
# ==========================================================
time_windows_5y <- list(
  c(1990, 1995),
  c(1995, 2000),
  c(2000, 2005),
  c(2005, 2010),
  c(2010, 2015),
  c(2015, 2020),
  c(2020, 2024)
)

# full sample + 5-year windows (no single-year windows)
time_windows_all <- c(list(c(1990, 2024)), time_windows_5y)

# ==========================================================
# MAIN EXECUTION — MSA (metro) FIRST, THEN STATE
# ==========================================================
files <- list(
  metro = "decomposition_metro_yhat.csv",
  state = "decomposition_state_yhat.csv"
)

for (name in names(files)) {

  df <- read_csv(files[[name]], show_col_types = FALSE)
  loc_var <- ifelse(name == "state", "first_state", "first_metro_area")

  for (tw in time_windows_all) {

    start_year <- tw[1]
    end_year   <- tw[2]

    df_window <- df %>% filter(year >= start_year & year <= end_year)
    if (nrow(df_window) == 0) next

    out_suffix   <- paste0(name, "_", start_year, "_", end_year)
    title_prefix <- paste0(ucfirst(name), " Level ", start_year, "–", end_year)

    plot_decomp(df_window, loc_var, title_prefix, out_suffix, label_locations = TRUE)
  }
}

cat("\n\n✅ All STATE & MSA plots + tables saved to C:/Users/eugen/OneDrive - UC San Diego/Desktop/Research/LinkedIn Innovation/Local Analyses/03_Output/decomposition/\n")

# ==========================================================
# FINAL TXT EXPORT OF ALL REGRESSION RESULTS (PRETTY FORMAT)
# ==========================================================
txt_path <- "C:/Users/eugen/OneDrive - UC San Diego/Desktop/Research/LinkedIn Innovation/Local Analyses/03_Output/decomposition/all_regression_results.txt"
sink(txt_path)

cat("============================================================\n")
cat("             FULL REGRESSION OUTPUT — ALL WINDOWS           \n")
cat("============================================================\n\n")

print_block <- function(title_text, df) {
  cat("---------------------------------\n")
  cat(title_text, "\n")
  cat("---------------------------------\n\n")
  print(knitr::kable(df, digits = 4))
  cat("\n")
}

for (name in names(all_regression_results)) {

  parts <- strsplit(name, "_")[[1]]
  level_raw  <- parts[1]
  start_year <- parts[2]
  end_year   <- parts[3]

  level_title  <- ucfirst(level_raw)
  window_title <- paste0(level_title, " Level ", start_year, "–", end_year)

  res <- all_regression_results[[name]]

  # α — yhat (weighted)
  print_block(paste0(window_title, " – α (log_yhat, weighted)"),
              res$alpha_yhat_w)

  # α — mean_patents (weighted)
  print_block(paste0(window_title, " – α (log_mean_patents, weighted)"),
              res$alpha_ymean_w)

  # β — combined (weighted)
  print_block(paste0(window_title, " – β (combined, weighted)"),
              res$beta_combined_w)

  cat("\n\n")
}

sink()

cat("\n📄 TXT file saved to:", txt_path, "\n")
