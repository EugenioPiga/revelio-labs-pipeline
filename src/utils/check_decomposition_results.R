#!/usr/bin/env Rscript

# ============================================================
# Diagnostic plots for FE scaling and tenure effects
# ============================================================

user_lib <- "~/R/library"
.libPaths(c(user_lib, .libPaths()))
packages <- c("readr", "dplyr", "ggplot2", "scales")
invisible(lapply(packages, function(p) if(!requireNamespace(p, quietly=TRUE)) install.packages(p, lib=user_lib)))

library(readr)
library(dplyr)
library(ggplot2)
library(scales)

# -------------------------------
# Load data (adjust path if needed)
# -------------------------------
OUT_DIR  <- "/home/epiga/revelio_labs/output/regressions/"
FILE     <- file.path(OUT_DIR, "decomposition_joined_covariates_feglm.csv")

cat("[INFO] Reading:", FILE, "\n")
df <- read_csv(FILE, show_col_types = FALSE)
cat("[INFO] Rows:", nrow(df), "Cols:", ncol(df), "\n")

# -------------------------------
# Basic summary stats
# -------------------------------
fe_stats <- df %>%
  summarise(
    var_user  = var(fe_user_id, na.rm = TRUE),
    var_firm  = var(fe_first_rcid, na.rm = TRUE),
    var_city  = var(fe_first_city, na.rm = TRUE),
    mean_user = mean(fe_user_id, na.rm = TRUE),
    mean_firm = mean(fe_first_rcid, na.rm = TRUE),
    mean_city = mean(fe_first_city, na.rm = TRUE)
  )

print(fe_stats)

# ============================================================
# FE Distributions
# ============================================================

plot_hist <- function(var, name) {
  ggplot(df, aes(x = .data[[var]])) +
    geom_histogram(bins = 100, fill = "steelblue", color = "white") +
    scale_x_continuous(labels = comma) +
    labs(
      title = paste("Distribution of", name, "Fixed Effects"),
      x = name,
      y = "Count"
    ) +
    theme_minimal(base_size = 13)
}

ggsave(file.path(OUT_DIR, "hist_fe_user.png"),  plot_hist("fe_user_id", "Inventor"))
ggsave(file.path(OUT_DIR, "hist_fe_firm.png"),  plot_hist("fe_first_rcid", "Firm"))
ggsave(file.path(OUT_DIR, "hist_fe_city.png"),  plot_hist("fe_first_city", "City"))

cat("[INFO] Histograms saved to:", OUT_DIR, "\n")

# ============================================================
# Log-scale comparison
# ============================================================

df_long <- df %>%
  select(fe_user_id, fe_first_rcid, fe_first_city) %>%
  tidyr::pivot_longer(everything(), names_to = "FE_type", values_to = "value")

ggplot(df_long, aes(x = value, fill = FE_type)) +
  geom_density(alpha = 0.5) +
  scale_x_log10(labels = comma) +
  labs(title = "Fixed Effect Distributions (log scale)",
       x = "Fixed Effect (absolute value, log10)",
       y = "Density") +
  theme_minimal(base_size = 13)

ggsave(file.path(OUT_DIR, "density_fe_logscale.png"), width = 8, height = 5)

# ============================================================
# FE vs. Tenure (if tenure is available)
# ============================================================

if ("tenure" %in% colnames(df)) {
  ggplot(df %>% sample_n(1e5), aes(x = tenure, y = fe_user_id)) +
    geom_hex(bins = 50) +
    scale_fill_viridis_c() +
    labs(
      title = "Inventor Fixed Effects vs. Tenure",
      x = "Tenure (raw units)",
      y = "Inventor FE (ψᵢ)"
    ) +
    theme_minimal(base_size = 13)

  ggsave(file.path(OUT_DIR, "scatter_feuser_tenure.png"), width = 8, height = 5)
} else {
  cat("[WARN] No `tenure` column found in decomposition file.\n")
}

# ============================================================
# Simple check of scaling range
# ============================================================

fe_ranges <- df %>%
  summarise(
    min_user = min(fe_user_id, na.rm = TRUE),
    max_user = max(fe_user_id, na.rm = TRUE),
    min_firm = min(fe_first_rcid, na.rm = TRUE),
    max_firm = max(fe_first_rcid, na.rm = TRUE),
    min_city = min(fe_first_city, na.rm = TRUE),
    max_city = max(fe_first_city, na.rm = TRUE)
  )

print(fe_ranges)
cat("[INFO] Done. Check output figures in:", OUT_DIR, "\n")
