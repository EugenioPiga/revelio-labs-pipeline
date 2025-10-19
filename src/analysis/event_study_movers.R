#!/usr/bin/env Rscript

# ============================
# Setup
# ============================

# Use non-interactive PNG device on HPC (fixes X11 error)
options(bitmapType = "cairo")

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

packages <- c("arrow", "dplyr", "fixest", "ggplot2", "broom", "readr")
installed <- rownames(installed.packages())
for (pkg in packages) {
  if (!pkg %in% installed) {
    install.packages(pkg, repos = "https://cloud.r-project.org", lib = user_lib)
  }
}

library(arrow)
library(dplyr)
library(fixest)
library(ggplot2)
library(broom)
library(readr)

# ============================
# Config
# ============================

INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/eventstudy"
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

# ============================
# Load Data
# ============================

cat("[INFO] Reading inventor-year data...\n")
df <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, year, first_rcid, first_city, n_patents) %>%
  collect()

cat("[INFO] Loaded", nrow(df), "rows\n")

# Restrict to 1980 onwards
df <- df %>% filter(year >= 1980)
cat("[INFO] Sample restricted to 1980 onwards. Remaining rows:", nrow(df), "\n")

# ============================
# Define Movers
# ============================

df <- df %>%
  arrange(user_id, year) %>%
  group_by(user_id) %>%
  mutate(
    lag_city = lag(first_city),
    lag_firm = lag(first_rcid),
    firm_mover = first_rcid != lag_firm,
    city_mover = first_city != lag_city,
    firm_within_city_mover = (first_rcid != lag_firm) & (first_city == lag_city)
  ) %>%
  ungroup()

# ============================
# Relative Year to Move
# ============================

define_event_study <- function(data, mover_var) {
  movers <- data %>%
    filter(!!sym(mover_var)) %>%
    group_by(user_id) %>%
    summarise(move_year = min(year, na.rm = TRUE), .groups = "drop")

  df_es <- data %>%
    left_join(movers, by = "user_id") %>%
    mutate(rel_year = year - move_year) %>%
    filter(!is.na(move_year)) %>%
    mutate(
      rel_year_bin = case_when(
        rel_year <= -8 ~ -8,
        rel_year >= 8 ~ 8,
        TRUE ~ rel_year
      ),
      log_patents = ifelse(is.na(n_patents) | n_patents < 0, NA, log1p(n_patents))
    ) %>%
    filter(!is.na(log_patents))

  cat("[INFO]", sum(is.na(df_es$log_patents)), "NAs dropped in log_patents for", mover_var, "\n")
  return(df_es)
}

# ============================
# Event Study Function
# ============================

run_event_study <- function(df_es, mover_type, OUT_DIR) {
  cat("[INFO] Running event study for:", mover_type, "\n")

  model <- feols(
    log_patents ~ i(rel_year_bin, ref = -1) | user_id + year,
    data = df_es,
    cluster = "user_id"
  )

  results <- broom::tidy(model, conf.int = TRUE) %>%
    filter(grepl("rel_year_bin", term)) %>%
    mutate(
      mover_type = mover_type,
      rel_year = as.numeric(gsub("rel_year_bin::", "", term))
    )

  # Save coefficients
  readr::write_csv(results, file.path(OUT_DIR, paste0("eventstudy_", mover_type, "_coefficients.csv")))

  # Plot
  p <- ggplot(results, aes(x = rel_year, y = estimate)) +
    geom_line(color = "black") +
    geom_point(color = "darkred") +
    geom_ribbon(aes(ymin = conf.low, ymax = conf.high), fill = "grey80", alpha = 0.4) +
    geom_vline(xintercept = 0, linetype = "dashed", color = "grey40") +
    geom_hline(yintercept = 0, color = "black", linetype = "solid") +
    labs(
      title = paste("Event Study: Inventor", mover_type, "Movers"),
      x = "Year Relative to Move",
      y = "Event Study Coefficient"
    ) +
    theme_minimal(base_size = 14)

  plot_file <- file.path(OUT_DIR, paste0("eventstudy_", mover_type, ".png"))
  ggsave(filename = plot_file, plot = p, device = "png", width = 8, height = 5, dpi = 300)

  cat("[INFO] Saved plot to:", plot_file, "\n")

  return(results)
}

# ============================
# Run for Each Mover Type
# ============================

df_firm  <- define_event_study(df, "firm_mover")
df_city  <- define_event_study(df, "city_mover")
df_fwc   <- define_event_study(df, "firm_within_city_mover")

cat("[INFO] Running event study for firm movers...\n")
results_firm <- run_event_study(df_firm, "firm", OUT_DIR)
rm(df_firm); gc()

cat("[INFO] Running event study for city movers...\n")
results_city <- run_event_study(df_city, "city", OUT_DIR)
rm(df_city); gc()

cat("[INFO] Running event study for firm-within-city movers...\n")
results_fwc <- run_event_study(df_fwc, "firm_within_city", OUT_DIR)
rm(df_fwc); gc()

# ============================
# Save Combined Output
# ============================

combined <- bind_rows(results_firm, results_city, results_fwc)
readr::write_csv(combined, file.path(OUT_DIR, "eventstudy_all_results.csv"))

cat("[INFO] All event study results saved to:", OUT_DIR, "\n")

