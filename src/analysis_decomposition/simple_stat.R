#!/usr/bin/env Rscript

###############################################################################
# simple_stat.R
# Scatter plot of average inventors vs patents per inventor by state (2018)
# Author: Codex assistant
###############################################################################

# ----------------------------
# Setup
# ----------------------------
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow", "dplyr", "ggplot2", "ggrepel", "readr")
for (p in pkgs) {
  if (!requireNamespace(p, quietly = TRUE)) {
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
  }
  library(p, character.only = TRUE)
}

# ----------------------------
# Config
# ----------------------------
INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_FIG <- "/home/gps-yuhei/code/revelio-labs-pipeline/output/figures/decomposition"
dir.create(OUT_FIG, showWarnings = FALSE, recursive = TRUE)

# ----------------------------
# Load and prepare data
# ----------------------------
cat("[INFO] Loading 2018 inventor data...\n")
dataset <- open_dataset(INPUT, format = "parquet") %>%
  select(user_id, n_patents, first_state, first_country, year) %>%
  filter(
    year == 2018,
    first_country == "United States",
    !is.na(first_state),
    !is.na(user_id)
  ) %>%
  collect()

cat("[INFO] Rows in 2018 sample:", nrow(dataset), "\n")

state_summary <- dataset %>%
  group_by(first_state) %>%
  summarise(
    avg_inventors = n_distinct(user_id),
    avg_patents_per_inventor = mean(n_patents, na.rm = TRUE),
    .groups = "drop"
  ) %>%
  filter(avg_inventors > 0, avg_patents_per_inventor > 0)

cat("[INFO] States after aggregation:", nrow(state_summary), "\n")

# ----------------------------
# Plot
# ----------------------------
inventors_state_plot <- ggplot(
  state_summary,
  aes(x = avg_inventors, y = avg_patents_per_inventor, label = first_state)
) +
  geom_point(alpha = 0.7) +
  ggrepel::geom_text_repel(size = 3, max.overlaps = Inf) +
  labs(
    title = "Average Inventors vs. Patents per Inventor by State (2018)",
    x = "Average number of inventors (log scale)",
    y = "Average patents per inventor (log scale)",
    caption = "Source: inventor_year_merged dataset"
  ) +
  scale_x_log10() +
  scale_y_log10() +
  theme_minimal()

output_path <- file.path(OUT_FIG, "inventors_vs_patents_state_2018.png")
ggsave(output_path, inventors_state_plot, width = 9, height = 6)
cat("[INFO] Plot saved to:", output_path, "\n")
