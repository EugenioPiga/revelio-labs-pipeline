#!/usr/bin/env Rscript
###############################################################################
# combine_event_study_shards_OLS_t1.R
# Combine OLS Δ-scaled event-study coefficients (±1 sample) across shards and plot results
# Author: Eugenio — 2025-10-12
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("data.table", "dplyr", "ggplot2", "stringr", "readr", "broom")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))
data.table::setDTthreads(4)

###############################################################################
# CONFIGURATION
###############################################################################

IN_DIR  <- "/home/epiga/revelio_labs/output/eventstudy_ols_physician"
OUT_DIR <- file.path(IN_DIR, "aggregated_t1")
dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

mover_types <- c("city", "firm", "firm_within_city")
T_max <- 8

###############################################################################
# FUNCTION
###############################################################################

combine_and_plot <- function(mover_type) {
  cat("\n[INFO] Combining OLS (t1) coefficients for mover type:", mover_type, "\n")

  # --- Load all shards once ---
  files <- list.files(IN_DIR, pattern = paste0("coefficients_", mover_type, "_t1.*\\.csv$"), full.names = TRUE)
  if (length(files) == 0L) {
    cat("[WARN] No OLS t1 files found for mover type:", mover_type, "\n")
    return(NULL)
  }

  df <- rbindlist(lapply(files, fread), fill = TRUE)
  df[, event_time := as.numeric(event_time)]

  # --- Helper function to aggregate & plot ---
  aggregate_and_plot <- function(sub_df, color, out_suffix, ylab, title_prefix) {
    agg_df <- sub_df %>%
      group_by(event_time) %>%
      summarise(
        estimate = mean(estimate, na.rm = TRUE),
        std.error = sqrt(mean(std.error^2, na.rm = TRUE)),
        .groups = "drop"
      ) %>%
      mutate(
        ci_low = estimate - 1.96 * std.error,
        ci_high = estimate + 1.96 * std.error
      ) %>%
      arrange(event_time)

    # Add normalization point (-1, 0)
    agg_df <- rbind(
      agg_df,
      data.frame(event_time = -1, estimate = 0, std.error = 0, ci_low = 0, ci_high = 0)
    )
    agg_df <- agg_df[order(agg_df$event_time), ]

    # --- Save combined CSV
    out_csv <- file.path(OUT_DIR, paste0("combined_", mover_type, "_ols_t1_", out_suffix, "_coefficients.csv"))
    fwrite(agg_df, out_csv)
    cat("[INFO] Saved aggregated coefficients to:", out_csv, "\n")

    # --- Plot
    p <- ggplot(agg_df, aes(x = as.numeric(event_time), y = estimate)) +
      geom_line(color = color, linewidth = 1.1) +
      geom_point(color = color, size = 2) +
      geom_errorbar(aes(ymin = ci_low, ymax = ci_high), width = 0.25, color = "gray40") +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "red") +
      scale_x_continuous(breaks = seq(-T_max, T_max, by = 1)) +
      labs(
        title = paste0(title_prefix, " (OLS, ±1 Sample): ", toupper(mover_type), " Movers"),
        x = "Years relative to move (event time)",
        y = ylab
      ) +
      theme_minimal(base_size = 15) +
      theme(plot.title = element_text(face = "bold", size = 16),
            panel.grid.minor = element_blank())

    out_png <- file.path(OUT_DIR, paste0("event_study_", mover_type, "_ols_t1_", out_suffix, ".png"))
    ggsave(out_png, p, width = 8, height = 5)
    cat("[INFO] Saved plot to:", out_png, "\n")
  }

  # --- γ (Delta_scaled)
  df_gamma <- df[component == "Delta_scaled"]
  aggregate_and_plot(df_gamma, "#E69F00", "gamma",
                     ylab = "Estimated Δ-Scaled Effect (γ)",
                     title_prefix = "Δ-Scaled Event Study")

  # --- β (Mover_main)
  df_beta <- df[component == "Mover_main"]
  aggregate_and_plot(df_beta, "#56B4E9", "beta",
                     ylab = "Estimated Event-Time Effect (β)",
                     title_prefix = "Baseline Event Study")

  cat("[INFO] Finished combining & plotting for", mover_type, "\n")
}

###############################################################################
# MAIN
###############################################################################

for (mover_type in mover_types) {
  combine_and_plot(mover_type)
}

cat("\n[INFO] All done ✅ OLS (t1) plots and CSVs saved in:", OUT_DIR, "\n")





