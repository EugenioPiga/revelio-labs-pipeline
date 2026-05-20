#!/usr/bin/env Rscript

###############################################################################
# simple_stat.R
# Scatter plots of patents per inventor vs inventors by geography (2018)
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
  select(
    user_id,
    n_patents,
    first_state,
    first_city,
    first_metro_area,
    first_country,
    year
  ) %>%
  filter(
    year == 2018,
    first_country == "United States",
    !is.na(user_id)
  ) %>%
  collect()

cat("[INFO] Rows in 2018 sample:", nrow(dataset), "\n")

geographies <- list(
  list(
    level = "state",
    column = "first_state",
    title = "Average Inventors vs. Patents per Inventor by State (2018)",
    filename = "inventors_vs_patents_state_2018.png",
    label_n = NULL
  ),
  list(
    level = "city",
    column = "first_city",
    title = "Average Inventors vs. Patents per Inventor by City (2018)",
    filename = "inventors_vs_patents_city_2018.png",
    label_n = 30
  ),
  list(
    level = "msa",
    column = "first_metro_area",
    title = "Average Inventors vs. Patents per Inventor by Metro Area (2018)",
    filename = "inventors_vs_patents_metro_2018.png",
    label_n = 40
  )
)

for (geo in geographies) {
  group_col <- geo$column

  summary_df <- dataset %>%
    filter(
      !is.na(.data[[group_col]]),
      .data[[group_col]] != "",
      tolower(.data[[group_col]]) != "empty"
    ) %>%
    group_by(level = .data[[group_col]]) %>%
    summarise(
      num_inventors = n_distinct(user_id),
      avg_patents_per_inventor = mean(n_patents, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    filter(num_inventors > 0, avg_patents_per_inventor > 0)

  cat(
    sprintf(
      "[INFO] %s groups after aggregation: %d\n",
      geo$level,
      nrow(summary_df)
    )
  )

  label_data <- summary_df
  if (!is.null(geo$label_n)) {
    label_data <- summary_df %>%
      arrange(desc(num_inventors)) %>%
      slice_head(n = geo$label_n)
  }

  plot_obj <- ggplot(
    summary_df,
    aes(x = num_inventors, y = avg_patents_per_inventor)
  ) +
    geom_point(alpha = 0.7) +
    ggrepel::geom_text_repel(
      data = label_data,
      aes(
        x = num_inventors,
        y = avg_patents_per_inventor,
        label = level
      ),
      size = 3,
      max.overlaps = Inf
    ) +
    labs(
      title = geo$title,
      x = "Number of inventors (log scale)",
      y = "Average patents per inventor (log scale)",
      caption = "Source: inventor_year_merged dataset"
    ) +
    scale_x_log10() +
    scale_y_log10() +
    theme_minimal()

  output_path <- file.path(OUT_FIG, geo$filename)
  ggsave(output_path, plot_obj, width = 9, height = 6)
  cat("[INFO] Plot saved to:", output_path, "\n")
}
