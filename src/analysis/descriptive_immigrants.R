#!/usr/bin/env Rscript
###############################################################################
# Descriptives — Immigrant share + patents vs cluster size
# - Scatter 1: immigrant share vs cluster size
# - Scatter 2: avg patents (immig vs nonimmig) vs cluster size
#
# Defaults:
#   YEAR = 2018 (change below if desired)
#   Cluster = geo (state/city/metro)
#   Cluster size = number of inventor-year observations in that geo-year
#
# Outputs (png + csv):
#   /home/epiga/revelio_labs/output/descriptives_immigrants
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow", "dplyr", "tidyr", "readr", "ggplot2")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}

library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(ggplot2)

# ============================
# Paths + knobs
# ============================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

TARGET_YEAR <- 2018     # <- change to whatever (or set NULL to pool all years)
MIN_CLUSTER <- 50       # <- drop tiny clusters for cleaner scatter

geos <- list(
  state = "first_state",
  city  = "first_city",
  metro = "first_metro_area"
)

immig_defs <- c(
  "immig_job_first_nonUS",
  "immig_deg_first_nonUS",
  "immig_first_deg_or_job_nonUS"
)

# ============================
# Helper: build cluster summary (geo × year)
# ============================
build_cluster_summary <- function(ds, geo_var, immig_var, target_year = 2018, min_cluster = 50) {

  # Filter + keep only needed cols (Arrow-friendly)
  base <- ds %>%
    select(user_id, year, n_patents, first_country,
           all_of(geo_var), all_of(immig_var)) %>%
    filter(first_country == "United States") %>%
    filter(!is.na(.data[[geo_var]]))

  if (!is.null(target_year)) {
    base <- base %>% filter(year == target_year)
  }

  # Clean immigrant flag to strict {0,1}
  base <- base %>%
    mutate(immig_flag = as.integer(.data[[immig_var]])) %>%
    filter(!is.na(immig_flag), immig_flag %in% c(0L, 1L))

  # Cluster size + immigrant share
  # NOTE: cluster_size is # inventor-year obs in the cluster-year.
  # If your data is truly unique by (user_id, year), this equals # inventors in that cluster-year.
  size_tbl <- base %>%
    group_by(across(all_of(geo_var))) %>%
    summarise(
      cluster_size = n(),
      immig_share  = mean(immig_flag),
      .groups = "drop"
    )

  # Avg patents by group inside each cluster
  pat_tbl <- base %>%
    group_by(across(all_of(geo_var)), immig_flag) %>%
    summarise(
      avg_patents = mean(n_patents, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    collect() %>%
    mutate(immig_flag = ifelse(immig_flag == 1L, "immigrant", "nonimmigrant")) %>%
    pivot_wider(names_from = immig_flag, values_from = avg_patents)

  # Collect size table and merge
  out <- size_tbl %>% collect() %>%
    left_join(pat_tbl, by = geo_var) %>%
    filter(cluster_size >= min_cluster)

  out
}

# ============================
# Helper: plotting
# ============================
plot_immig_share <- function(df, geo_name, immig_def, target_year) {
  ttl_year <- ifelse(is.null(target_year), "All years", as.character(target_year))

  ggplot(df, aes(x = cluster_size, y = immig_share)) +
    geom_point(alpha = 0.35) +
    geom_smooth(method = "loess", se = FALSE) +
    scale_x_log10() +
    labs(
      title = paste0("Immigrant share vs cluster size (", geo_name, ", ", immig_def, ", ", ttl_year, ")"),
      x = "Cluster size (log scale)",
      y = "Immigrant share"
    ) +
    theme_minimal()
}

plot_avg_patents <- function(df, geo_name, immig_def, target_year) {
  ttl_year <- ifelse(is.null(target_year), "All years", as.character(target_year))

  long <- df %>%
    select(cluster_size, immigrant, nonimmigrant) %>%
    pivot_longer(
      cols = c(immigrant, nonimmigrant),
      names_to = "group",
      values_to = "avg_patents"
    ) %>%
    mutate(group = factor(group, levels = c("immigrant", "nonimmigrant")))

  ggplot(long, aes(x = cluster_size, y = avg_patents, color = group)) +
    geom_point(alpha = 0.45) +
    geom_smooth(method = "lm", se = FALSE) +   # regression line per group (because color=group)
    scale_x_log10() +
    labs(
      title = paste0("Avg patents vs cluster size (", geo_name, ", ", immig_def, ", ", ttl_year, ")"),
      x = "Cluster size (log scale)",
      y = "Average patents per inventor-year",
      color = "Group"
    ) +
    theme_minimal()
}

# ============================
# Run all combinations
# ============================
cat("\n[INFO] Opening Arrow dataset...\n")
ds <- open_dataset(INPUT, format = "parquet")

for (immig_def in immig_defs) {
  for (geo_name in names(geos)) {

    geo_var <- geos[[geo_name]]
    cat("\n========================================\n")
    cat("[INFO] immig_def:", immig_def, "| geo:", geo_name, "(", geo_var, ")\n")
    cat("========================================\n")

    summ <- build_cluster_summary(
      ds = ds,
      geo_var = geo_var,
      immig_var = immig_def,
      target_year = TARGET_YEAR,
      min_cluster = MIN_CLUSTER
    )

    if (nrow(summ) == 0) {
      cat("[WARN] No data after filters; skipping.\n")
      next
    }

    # Save summary table
    tag_year <- ifelse(is.null(TARGET_YEAR), "allyears", paste0("y", TARGET_YEAR))
    tag <- paste(immig_def, geo_name, tag_year, sep = "__")

    csv_path <- file.path(OUT_DIR, paste0("cluster_summary__", tag, ".csv"))
    write_csv(summ, csv_path)
    cat("[INFO] Wrote:", csv_path, "\n")

    # Plot 1: immigrant share vs cluster size
    p1 <- plot_immig_share(summ, geo_name, immig_def, TARGET_YEAR)
    p1_path <- file.path(OUT_DIR, paste0("scatter_immig_share__", tag, ".png"))
    ggsave(p1_path, p1, width = 8, height = 5, dpi = 200)
    cat("[INFO] Wrote:", p1_path, "\n")

    # Plot 2: avg patents (immig vs nonimmig) vs cluster size
    p2 <- plot_avg_patents(summ, geo_name, immig_def, TARGET_YEAR)
    p2_path <- file.path(OUT_DIR, paste0("scatter_avg_patents__", tag, ".png"))
    ggsave(p2_path, p2, width = 8, height = 5, dpi = 200)
    cat("[INFO] Wrote:", p2_path, "\n")
  }
}

cat("\n[DONE] All descriptives complete.\n\n")
