#!/usr/bin/env Rscript
###############################################################################
# plot_event_study_feglm_t2.R
# Combine & plot event-study coefficients (feglm)
# Baseline + Tenure versions
# Author: Eugenio — 2025-10-24
###############################################################################

# =========================
# Setup
# =========================
options(bitmapType = "cairo")

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("data.table", "dplyr", "ggplot2", "stringr", "readr")
for (p in pkgs)
  if (!requireNamespace(p, quietly = TRUE))
    install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
invisible(lapply(pkgs, library, character.only = TRUE))
data.table::setDTthreads(4)

###############################################################################
# CONFIGURATION
###############################################################################

BASE_DIR   <- "/home/epiga/revelio_labs/output/eventstudy_ppml_feglm"
TENURE_DIR <- "/home/epiga/revelio_labs/output/eventstudy_tenure_ppml_feglm"
OUT_DIR    <- "/home/epiga/revelio_labs/output/eventstudy_feglm_combined/aggregated"

dir.create(OUT_DIR, showWarnings = FALSE, recursive = TRUE)

mover_types <- c("city", "firm", "firm_within_city")
T_max <- 8

###############################################################################
# FUNCTION
###############################################################################

plot_event_study <- function(mover_type, version_label, input_dir, color_main, color_delta) {
  cat("\n[INFO] Plotting", version_label, "results for mover type:", mover_type, "\n")

  suffix <- ifelse(version_label == "Tenure", "_tenure", "")
  file_path <- file.path(input_dir, paste0("coefficients_", mover_type, suffix, ".csv"))

  if (!file.exists(file_path)) {
    cat("[WARN] File not found:", file_path, "\n")
    return(NULL)
  }

  df <- fread(file_path)

  # Ensure event_time & component
  if (!"component" %in% names(df)) {
    df$component <- ifelse(grepl("_delta$", df$term), "Delta_scaled", "Mover_main")
  }
  if (!"event_time" %in% names(df)) {
    df$event_time <- as.numeric(stringr::str_extract(df$term, "-?\\d+"))
  }

  df <- df[!is.na(event_time)]
  df$event_time <- as.numeric(df$event_time)

  # Compute CI if std.error exists
  if ("std.error" %in% names(df) && !all(is.na(df$std.error))) {
    df <- df %>%
      mutate(
        ci_low = estimate - 1.96 * std.error,
        ci_high = estimate + 1.96 * std.error
      )
  } else {
    cat("[WARN] No std.error column found, skipping CIs.\n")
    df$ci_low <- NA
    df$ci_high <- NA
  }

  make_plot <- function(sub_df, color, out_suffix, ylab, title_prefix) {
    if (nrow(sub_df) == 0) return(NULL)

    # ✅ Add reference period (-1) safely with all missing columns filled
    ref_row <- as.list(rep(NA, ncol(sub_df)))
    names(ref_row) <- names(sub_df)
    ref_row$event_time <- -1
    ref_row$estimate <- 0
    if ("std.error" %in% names(sub_df)) ref_row$std.error <- 0
    if ("ci_low" %in% names(sub_df)) ref_row$ci_low <- 0
    if ("ci_high" %in% names(sub_df)) ref_row$ci_high <- 0

    sub_df <- rbind(sub_df, as.data.frame(ref_row), fill = TRUE)
    sub_df <- sub_df[order(sub_df$event_time), ]

    # Save CSV
    out_csv <- file.path(OUT_DIR, paste0("combined_", mover_type, "_", version_label, "_", out_suffix, ".csv"))
    fwrite(sub_df, out_csv)
    cat("[INFO] Saved combined coefficients to:", out_csv, "\n")

    # Plot
    p <- ggplot(sub_df, aes(x = event_time, y = estimate)) +
      geom_ribbon(aes(ymin = ci_low, ymax = ci_high), fill = color, alpha = 0.25, na.rm = TRUE) +
      geom_line(color = color, linewidth = 1.1, na.rm = TRUE) +
      geom_point(color = color, size = 2, na.rm = TRUE) +
      geom_vline(xintercept = -0.5, linetype = "dashed", color = "red") +
      scale_x_continuous(breaks = seq(-T_max, T_max, 1)) +
      labs(
        title = paste0(title_prefix, " (FEGLM, ", version_label, "): ", toupper(mover_type), " Movers"),
        x = "Years relative to move (event time)",
        y = ylab
      ) +
      theme_minimal(base_size = 15) +
      theme(
        plot.title = element_text(face = "bold", size = 16),
        panel.grid.minor = element_blank()
      )

    out_png <- file.path(OUT_DIR, paste0("event_study_", mover_type, "_", version_label, "_", out_suffix, ".png"))
    ggsave(out_png, p, width = 8, height = 5, dpi = 300)
    cat("[INFO] Saved plot to:", out_png, "\n")
  }

  df_gamma <- df[df$component == "Delta_scaled", ]
  df_beta  <- df[df$component == "Mover_main", ]

  make_plot(df_gamma, color_delta, "gamma", "Estimated Δ-Scaled Effect (γ)", "Δ-Scaled Event Study")
  make_plot(df_beta,  color_main,  "beta",  "Estimated Event-Time Effect (β)", "Baseline Event Study")

  cat("[INFO] ✅ Finished plotting", version_label, "for", mover_type, "\n")
}

###############################################################################
# MAIN LOOP
###############################################################################

for (mover_type in mover_types) {
  plot_event_study(mover_type, "Baseline", BASE_DIR, "#009E73", "#0072B2")
  plot_event_study(mover_type, "Tenure",   TENURE_DIR, "#CC79A7", "#D55E00")
}

cat("\n[INFO] ✅ All done — baseline and tenure FEGLM plots saved with 95% CIs in:", OUT_DIR, "\n")
