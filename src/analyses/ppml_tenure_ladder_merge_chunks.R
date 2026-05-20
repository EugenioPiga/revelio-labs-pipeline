#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(tibble)
})

OUT_DIR <- "/home/epiga/revelio_labs/output/ppml_tenure_ladder_extended"
CHUNK_TABLE_DIR <- file.path(OUT_DIR, "chunk_outputs", "tables")
CHUNK_DIAG_DIR  <- file.path(OUT_DIR, "chunk_outputs", "diagnostics")
FINAL_TABLE_DIR <- file.path(OUT_DIR, "tables")
FINAL_DIAG_DIR  <- file.path(OUT_DIR, "diagnostics")

dir.create(FINAL_TABLE_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(FINAL_DIAG_DIR,  recursive = TRUE, showWarnings = FALSE)

ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

safe_read_csv <- function(path) {
  tryCatch(readr::read_csv(path, show_col_types = FALSE), error = function(e) NULL)
}

bind_matching <- function(pattern) {
  files <- list.files(CHUNK_TABLE_DIR, pattern = pattern, full.names = TRUE)
  if (length(files) == 0) return(tibble())
  tabs <- lapply(files, safe_read_csv)
  tabs <- tabs[!vapply(tabs, is.null, logical(1))]
  if (length(tabs) == 0) return(tibble())
  bind_rows(tabs)
}

parse_tenure_terms <- function(tab) {
  if (nrow(tab) == 0 || !all(c("term", "estimate", "se", "p") %in% names(tab))) return(tibble())

  levs <- c("0-5", "6-10", "11-15", "16-20", "21-25", "26-30", "31-35", "36-40", "41-45", "46-50")

  tab %>%
    filter(grepl("^tenure_bin::", term) | grepl("^tenure_bin = ", term) | grepl("^tenure_bin", term)) %>%
    mutate(
      tenure_bin = dplyr::case_when(
        grepl("::", term) ~ sub("^.*::", "", term),
        grepl(" = ", term) ~ sub("^.* = ", "", term),
        TRUE ~ sub("^tenure_bin", "", term)
      ),
      tenure_bin = gsub("^\\s+|\\s+$", "", tenure_bin),
      tenure_bin = gsub("`", "", tenure_bin),
      tenure_bin = factor(tenure_bin, levels = levs, ordered = TRUE),
      conf_low  = estimate - 1.96 * se,
      conf_high = estimate + 1.96 * se,
      p_label   = ifelse(is.finite(p), sprintf("p=%.3f", p), NA_character_)
    ) %>%
    filter(!is.na(tenure_bin))
}

ts_msg("Merging chunk outputs from:", CHUNK_TABLE_DIR)

coef_all      <- bind_matching("__coef_all\\.csv$")
means_0_5_all <- bind_matching("__means_0_5\\.csv$")
fe_decomp_all <- bind_matching("__fe_decomp_all\\.csv$")
group_diff_all <- bind_matching("__group_diff_all\\.csv$")
manifest_all  <- {
  files <- list.files(CHUNK_DIAG_DIR, pattern = "__manifest\\.csv$", full.names = TRUE)
  if (length(files) == 0) tibble() else bind_rows(lapply(files, safe_read_csv))
}

write_csv(coef_all,      file.path(FINAL_TABLE_DIR, "ALL__ppml_tenure_ladder_extended__all_terms.csv"))
write_csv(means_0_5_all, file.path(FINAL_TABLE_DIR, "ALL__mean_patents_0_5.csv"))
write_csv(fe_decomp_all, file.path(FINAL_TABLE_DIR, "ALL__mp_fe_decomposition_ols__all_terms.csv"))
write_csv(group_diff_all, file.path(FINAL_TABLE_DIR, "ALL__group_difference_interactions.csv"))

coef_focus <- parse_tenure_terms(coef_all)
fe_focus   <- parse_tenure_terms(fe_decomp_all)

write_csv(coef_focus, file.path(FINAL_TABLE_DIR, "ALL__ppml_tenure_ladder_extended__tenure_bin_focus.csv"))
write_csv(fe_focus,   file.path(FINAL_TABLE_DIR, "ALL__mp_fe_decomposition_ols__tenure_bin_focus.csv"))

if (nrow(group_diff_all) > 0) {
  group_diff_summary_by_contrast <- group_diff_all %>%
    group_by(group_var, group_ref, group_alt, fe_name) %>%
    summarise(
      n_bins_p_lt_0_10 = sum(is.finite(p) & p < 0.10, na.rm = TRUE),
      n_bins_p_lt_0_05 = sum(is.finite(p) & p < 0.05, na.rm = TRUE),
      avg_abs_diff     = mean(abs(estimate), na.rm = TRUE),
      .groups = "drop"
    )

  group_diff_summary_collapsed <- group_diff_all %>%
    group_by(group_var, fe_name) %>%
    summarise(
      n_bins_p_lt_0_10 = sum(is.finite(p) & p < 0.10, na.rm = TRUE),
      n_bins_p_lt_0_05 = sum(is.finite(p) & p < 0.05, na.rm = TRUE),
      avg_abs_diff     = mean(abs(estimate), na.rm = TRUE),
      .groups = "drop"
    )
} else {
  group_diff_summary_by_contrast <- tibble()
  group_diff_summary_collapsed <- tibble()
}

write_csv(group_diff_summary_by_contrast, file.path(FINAL_TABLE_DIR, "ALL__group_difference_summary_by_contrast.csv"))
write_csv(group_diff_summary_collapsed,   file.path(FINAL_TABLE_DIR, "ALL__group_difference_summary.csv"))
write_csv(manifest_all,                   file.path(FINAL_DIAG_DIR, "combined_chunk_manifests.csv"))

ts_msg("DONE merge. Final outputs in:", FINAL_TABLE_DIR)
