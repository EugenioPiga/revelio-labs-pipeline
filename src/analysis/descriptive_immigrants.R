#!/usr/bin/env Rscript
###############################################################################
# Descriptives — WEIGHTED SCATTERS (Immigrants vs Natives)
#
# All plots use WEIGHTS = CLUSTER SIZE (overall # inventors in cluster-year).
#
# Plots produced for each (year, geo, immig_def):
# A1) Immigrant share vs log(cluster size)
# A2) Immigrant share vs avg cluster productivity (avg_prod = mean patents in cluster-year)
#
# B1) Avg patents (immig vs native) vs log(cluster size) [y = group mean patents; x = log cluster size]
# B2) Avg patents (immig vs native) vs avg cluster productivity (overall avg_prod)
# B3) Avg patents (immig vs native) vs overall immigrant share
#
# C) Field (career_modal_section): immigrant share within field vs avg_prod AND vs log_cluster
#    - top 9 fields (by inventor counts in that slice); color legend; lm lines weighted by cluster_size
#
# D) Tenure bins (0-5,5-10,...): immigrant share within bin vs avg_prod AND vs log_cluster
#    - tenure computed from first_startdate_edu/first_startdate_pos; color legend; weighted lines
#
# E) Origin (first_university_country ONLY): share of inventors who are immigrants from top 6 origins
#      (i) origin = first_university_country if not U.S., else first_pos_country   
#    - share_origin = n_immigrants_from_origin / cluster_size
#    - plots vs avg_prod AND vs log_cluster; weighted lines
#
# G) Plot distribution of first degree type (Bachelor/Master/PhD/Other)
#    - overall
#    - by immigrant status
#
# H) Print examples of "first position" (overall + immig + nonimmig) to text file
#
# Output:
#   /home/epiga/revelio_labs/output/descriptives_immigrants/plots/
###############################################################################


# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","tidyr","readr","ggplot2","stringr")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
library(arrow)
library(dplyr)
library(tidyr)
library(readr)
library(ggplot2)
library(stringr)

set.seed(123)

# ============================
# Paths + knobs
# ============================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants"

PLOTS_DIR <- file.path(OUT_DIR, "plots")
TEXT_DIR  <- file.path(OUT_DIR, "text")
dir.create(PLOTS_DIR, recursive = TRUE, showWarnings = FALSE)
dir.create(TEXT_DIR,  recursive = TRUE, showWarnings = FALSE)

MIN_CLUSTER <- 10
USE_LAST_N_YEARS <- 16

# top categories in plots
TOP_FIELDS_N    <- 9
TOP_COUNTRIES_N <- 12

TENURE_BIN_WIDTH <- 5
TENURE_MAX <- 50

TOP_ORIGINS_N <- 12
TOP_FIELDS_COMP_N <- 9  # keep 9 for fields (you had TOP_FIELDS_N=9)
TOP_TENURE_COMP_N <- 9  # choose how many tenure bins to show

geos <- list(
  state = "first_state",
  metro = "first_metro_area"
)

immig_defs <- c(
#  "immig_job_first_nonUS",
#  "immig_deg_first_nonUS",
  "immig_first_deg_or_job_nonUS"
)

# ============================
# Helpers
# ============================
need_cols <- function(ds, cols) {
  nm <- names(ds)
  missing <- setdiff(cols, nm)
  if (length(missing) > 0) {
    stop(paste0("[ERROR] Missing required columns: ", paste(missing, collapse = ", ")))
  }
}

save_scatter <- function(df, x, y, color = NULL, w = "cluster_size",
                         title, xlab, ylab, filename,
                         point_alpha = 0.35,
                         size_range = c(0.8, 6),
                         monotone_color = FALSE) {
  if (nrow(df) == 0) return(invisible(NULL))

  subtitle_txt <- paste0(
    "Line: weighted OLS (weights = ", w, "). ",
    "Dot size ∝ ", w, "."
  )

  if (is.null(color)) {

    p <- ggplot(df, aes(x = .data[[x]], y = .data[[y]], size = .data[[w]])) +
      geom_point(alpha = point_alpha) +
      geom_smooth(method = "lm", aes(weight = .data[[w]]), se = FALSE) +
      labs(title = title, subtitle = subtitle_txt, x = xlab, y = ylab) +
      theme_minimal() +
      scale_size_continuous(range = size_range, guide = "none")

  } else {

    p <- ggplot(df, aes(x = .data[[x]], y = .data[[y]],
                        color = .data[[color]], size = .data[[w]])) +
      geom_point(alpha = point_alpha) +
      geom_smooth(method = "lm", aes(weight = .data[[w]]), se = FALSE) +
      labs(title = title, subtitle = subtitle_txt, x = xlab, y = ylab, color = color) +
      theme_minimal() +
      scale_size_continuous(range = size_range, guide = "none")

    # Optional: monotone (sequential) color scale when color is numeric or ordered bins
    if (monotone_color) {
      # If color is numeric -> continuous monotone
      if (is.numeric(df[[color]])) {
        p <- p + scale_color_viridis_c(option = "C", direction = 1)
      } else {
        p <- p + scale_color_viridis_d(option = "C", direction = 1)
      }
    }
  }

  ggsave(file.path(PLOTS_DIR, filename), p, width = 8.8, height = 5.6, dpi = 200)
  invisible(NULL)
}

# ---- Tenure (your exact definition) ----
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = as.numeric(substr(first_startdate_edu, 1, 4)),
      pos_year = as.numeric(substr(first_startdate_pos, 1, 4)),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > 50, year - pos_year, tenure),
      tenure   = ifelse(tenure > 50 | tenure < 0, NA, tenure)
    ) %>%
    filter(!is.na(tenure))
}

make_tenure_bins <- function(tenure) {
  breaks <- seq(0, TENURE_MAX, by = TENURE_BIN_WIDTH)
  cut(tenure, breaks = breaks, right = FALSE, include.lowest = TRUE)
}

# ============================
# (1) Open dataset + Top10 inventors filter
# ============================
cat("\n[INFO] Opening Arrow dataset...\n")
ds0 <- open_dataset(INPUT, format = "parquet")

# required for top10
need_cols(ds0, c("user_id","n_patents","year"))

cat("[INFO] Computing top 10% inventors by total patents...\n")
inv_tot <- ds0 %>%
  select(user_id, n_patents) %>%
  group_by(user_id) %>%
  summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop") %>%
  collect()

p90   <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)
cat("[INFO] Top10 inventors:", length(top10), "\n")

ds <- ds0 %>% filter(user_id %in% top10)
rm(ds0, inv_tot)

# years
years_all <- ds %>%
  select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
if (length(years_all) == 0) stop("[ERROR] No years found in dataset.")
years_to_run <- tail(years_all, USE_LAST_N_YEARS)

cat("[INFO] Running years:", paste(years_to_run, collapse = ", "), "\n")

# ============================
# (2) Compute first_pos_country in R (for top10 inventors)
#     pos_country_y = coalesce(last_country, first_country)
#     first_pos_country = pos_country_y at earliest year with non-missing
# ============================
cat("\n[INFO] Computing first_pos_country mapping (robust, in-memory)...\n")

# Pull only the columns needed, and only once
pos_df <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(pos_country_y = dplyr::coalesce(last_country, first_country)) %>%
  filter(!is.na(pos_country_y), pos_country_y != "") %>%
  distinct(user_id, year, pos_country_y) %>%
  collect()

# For each inventor, take the earliest year we observe pos_country_y
first_pos_map <- pos_df %>%
  group_by(user_id) %>%
  slice_min(order_by = year, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  transmute(
    user_id,
    first_pos_year = year,
    first_pos_country = pos_country_y
  )

cat("[INFO] first_pos_map rows:", nrow(first_pos_map), "\n")

# ============================
# Build panels for one (year, geo, immig_def)
# ============================
build_panels <- function(ds, year_val, geo_var, immig_var, min_cluster = 50, first_pos_map) {

  base_cols <- c("user_id","year","n_patents","first_country", geo_var, immig_var,
                 "career_modal_section","first_startdate_edu","first_startdate_pos",
                 "first_university_country", "career_modal_class")

  need_cols(ds, c("user_id","year","n_patents","first_country", geo_var, immig_var))

  base <- ds %>%
    select(any_of(base_cols)) %>%
    filter(first_country == "United States") %>%
    filter(year == year_val) %>%
    filter(!is.na(.data[[geo_var]])) %>%
    mutate(immig_flag = as.integer(.data[[immig_var]])) %>%
    filter(!is.na(immig_flag), immig_flag %in% c(0L, 1L)) %>%
    collect()

  if (nrow(base) == 0) {
    return(list(
      core = tibble(),
      pat_long = tibble(),
      field_comp_long = tibble(),
      tenure_comp_long = tibble(),
      origin_long = tibble(),
      h_comp_long = tibble(),
      g_comp_long = tibble(), 
      base_year = tibble()
    ))
  }

  # attach first_pos_country
  base <- base %>%
    left_join(first_pos_map, by = "user_id") %>%
    mutate(
      geo_level = as.character(.data[[geo_var]]),
      group = factor(ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
                     levels = c("nonimmigrant","immigrant","overall"))
    )

  # total immigrants in the cluster-year (denominator for "within-immigrant" shares)
  immig_counts <- base %>%
    filter(immig_flag == 1L) %>%
    group_by(geo_level) %>%
    summarise(n_immig = n_distinct(user_id), .groups = "drop")


  # cluster stats
  cluster_stats <- base %>%
    group_by(geo_level) %>%
    summarise(
      cluster_size = n_distinct(user_id),
      avg_prod     = mean(n_patents, na.rm = TRUE),  # overall mean patents
      n_immigrant  = n_distinct(user_id[immig_flag == 1L]),
      immig_share  = ifelse(cluster_size > 0, n_immigrant / cluster_size, NA_real_),
      .groups = "drop"
    ) %>%
    mutate(log_cluster = log(cluster_size)) %>%
    filter(cluster_size >= min_cluster)

  if (nrow(cluster_stats) == 0) {
    return(list(
      core = tibble(),
      pat_long = tibble(),
      field_comp_long = tibble(),
      tenure_comp_long = tibble(),
      origin_long = tibble(),
      g_comp_long = tibble(),
      h_comp_long = tibble(),
      base_year = base
    ))
  }

  # -------------------------
  # (B) avg patents by group + OVERALL
  # -------------------------
  pat_by_group <- base %>%
    group_by(geo_level, group) %>%
    summarise(avg_patents = mean(n_patents, na.rm = TRUE), .groups = "drop")

  pat_overall <- base %>%
    group_by(geo_level) %>%
    summarise(avg_patents = mean(n_patents, na.rm = TRUE), .groups = "drop") %>%
    mutate(group = factor("overall", levels = c("nonimmigrant","immigrant","overall")))

  pat_long <- bind_rows(pat_by_group, pat_overall) %>%
    left_join(cluster_stats, by = "geo_level") %>%
    filter(!is.na(avg_patents))

  core <- cluster_stats %>%
    left_join(
      pat_long %>%
        select(geo_level, group, avg_patents) %>%
        pivot_wider(names_from = group, values_from = avg_patents, names_prefix = "avg_"),
      by = "geo_level"
   )

  # ==========================================================
  #   share_cat = (# immigrants in category) / cluster_size
  # ==========================================================

  # -------------------------
  # (C-comp) Field composition among immigrants
  # -------------------------
field_comp_long <- tibble()
if ("career_modal_section" %in% names(base)) {

  immf <- base %>%
    filter(immig_flag == 1L,
           !is.na(career_modal_section), career_modal_section != "")

  if (nrow(immf) > 0) {

    top_fields <- immf %>%
      group_by(career_modal_section) %>%
      summarise(total = n_distinct(user_id), .groups="drop") %>%
      arrange(desc(total)) %>%
      slice_head(n = TOP_FIELDS_N) %>%
      pull(career_modal_section)

    field_counts <- immf %>%
      filter(career_modal_section %in% top_fields) %>%
      group_by(geo_level, field = career_modal_section) %>%
      summarise(n_field_imm = n_distinct(user_id), .groups="drop")

    field_comp_long <- tidyr::crossing(
        geo_level = unique(immig_counts$geo_level),
        field     = top_fields
      ) %>%
      left_join(field_counts, by = c("geo_level","field")) %>%
      mutate(n_field_imm = dplyr::coalesce(n_field_imm, 0L)) %>%
      left_join(immig_counts, by = "geo_level") %>%
      left_join(cluster_stats, by = "geo_level") %>%
      mutate(
        share_cat = ifelse(n_immig > 0, n_field_imm / n_immig, NA_real_),
        field = factor(field)
      ) %>%
      filter(!is.na(share_cat), is.finite(log_cluster))
  }
}

  # -------------------------
  # (D-comp) Tenure composition among immigrants
  # -------------------------
tenure_comp_long <- tibble()
if (all(c("first_startdate_edu","first_startdate_pos") %in% names(base))) {

  immt <- compute_tenure(base) %>%
    filter(immig_flag == 1L) %>%
    mutate(tenure_bin = make_tenure_bins(tenure)) %>%
    filter(!is.na(tenure_bin))

  if (nrow(immt) > 0) {

    tenure_bins_all <- sort(unique(as.character(immt$tenure_bin)))

    tenure_counts <- immt %>%
      group_by(geo_level, tenure_bin) %>%
      summarise(n_bin_imm = n_distinct(user_id), .groups="drop") %>%
      mutate(tenure_bin = as.character(tenure_bin))

    tenure_comp_long <- tidyr::crossing(
        geo_level  = unique(immig_counts$geo_level),
        tenure_bin = tenure_bins_all
      ) %>%
      left_join(tenure_counts, by = c("geo_level","tenure_bin")) %>%
      mutate(n_bin_imm = dplyr::coalesce(n_bin_imm, 0L)) %>%
      left_join(immig_counts, by = "geo_level") %>%
      left_join(cluster_stats, by = "geo_level") %>%
      mutate(
        share_cat = ifelse(n_immig > 0, n_bin_imm / n_immig, NA_real_),
        tenure_bin = factor(tenure_bin, levels = tenure_bins_all)
      ) %>%
      filter(!is.na(share_cat), is.finite(log_cluster))
  }
}

# -------------------------
# (E-comp) Unified immigrant origin among immigrants:
# origin = first_university_country if non-US, else first_pos_country if non-US
# share_origin = n_origin / cluster_size
# -------------------------
origin_long <- tibble()
if (all(c("first_university_country","first_pos_country") %in% names(base))) {

  imm <- base %>%
    filter(immig_flag == 1L) %>%
    mutate(
      edu_c = as.character(first_university_country),
      pos_c = as.character(first_pos_country),

      edu_missing = is.na(edu_c) | edu_c %in% c("", "empty"),
      pos_missing = is.na(pos_c) | pos_c %in% c("", "empty"),
      edu_is_us = !edu_missing & edu_c == "United States",
      pos_is_us = !pos_missing & pos_c == "United States",

      edu_nonUS = !edu_missing & !edu_is_us,
      pos_nonUS = !pos_missing & !pos_is_us,

      origin = dplyr::case_when(
        edu_nonUS ~ edu_c,
        !edu_nonUS & pos_nonUS ~ pos_c,
        TRUE ~ NA_character_
      )
    ) %>%
    filter(!is.na(origin), origin != "")

  if (nrow(imm) > 0) {

    # top origins overall
    top_c <- imm %>%
      group_by(origin) %>%
      summarise(total = n_distinct(user_id), .groups="drop") %>%
      arrange(desc(total)) %>%
      slice_head(n = TOP_ORIGINS_N) %>%
      pull(origin)

    # counts by geo × origin (only where present)
    origin_counts <- imm %>%
      filter(origin %in% top_c) %>%
      group_by(geo_level, origin) %>%
      summarise(n_origin = n_distinct(user_id), .groups="drop")

    # FULL grid geo × origin, fill missing with zero
    origin_long <- tidyr::crossing(
        geo_level = unique(immig_counts$geo_level),
        origin    = top_c
      ) %>%
      left_join(origin_counts, by = c("geo_level","origin")) %>%
      mutate(n_origin = dplyr::coalesce(n_origin, 0L)) %>%
      left_join(immig_counts, by = "geo_level") %>%
      left_join(cluster_stats, by = "geo_level") %>%
      mutate(
        share_cat = ifelse(n_immig > 0, n_origin / n_immig, NA_real_),
        origin = factor(origin)
      ) %>%
      filter(!is.na(share_cat), is.finite(log_cluster))
  }
}

  # -------------------------
  # (F-comp) Subfield composition among immigrants
  # -------------------------

make_within_section_subclass_long <- function(base,
                                             section_letter = c("G","H"),
                                             subclass_var,
                                             top_n,
                                             cluster_stats) {
  section_letter <- match.arg(section_letter)

  if (!("career_modal_section" %in% names(base))) return(tibble())
  if (!(subclass_var %in% names(base))) return(tibble())

  # immigrants in this SECTION (big bucket)
  immS <- base %>%
  filter(
    immig_flag == 1L,
    !is.na(.data[[subclass_var]]),
    .data[[subclass_var]] != "",
    .data[[subclass_var]] != "empty",
    substr(.data[[subclass_var]], 1, 1) == section_letter
  ) %>%
  mutate(subcat = as.character(.data[[subclass_var]]))

  if (nrow(immS) == 0) return(tibble())

  # denom: immigrants in this SECTION by cluster
  denom <- immS %>%
    group_by(geo_level) %>%
    summarise(n_S = n_distinct(user_id), .groups = "drop")

  # top subclasses overall (within section)
  top_sub <- immS %>%
    group_by(subcat) %>%
    summarise(total = n_distinct(user_id), .groups="drop") %>%
    arrange(desc(total)) %>%
    slice_head(n = top_n) %>%
    pull(subcat)

  counts <- immS %>%
    filter(subcat %in% top_sub) %>%
    group_by(geo_level, subcat) %>%
    summarise(n_sub = n_distinct(user_id), .groups="drop")

  out <- tidyr::crossing(
    geo_level = unique(cluster_stats$geo_level),
    subcat    = top_sub
  ) %>%
    left_join(counts, by = c("geo_level","subcat")) %>%
    mutate(n_sub = dplyr::coalesce(n_sub, 0L)) %>%
    left_join(denom, by = "geo_level") %>%
    left_join(cluster_stats, by = "geo_level") %>%
    mutate(
      share_cat = ifelse(!is.na(n_S) & n_S > 0, n_sub / n_S, NA_real_),
      subcat = factor(subcat, levels = top_sub)
    ) %>%
    filter(!is.na(share_cat), is.finite(log_cluster))

  out
}

# CHANGE THIS to your real subclass variable:
SUBCLASS_VAR <- "career_modal_class"

g_comp_long <- make_within_section_subclass_long(
  base = base,
  section_letter = "G",
  subclass_var = SUBCLASS_VAR,
  top_n = TOP_FIELDS_N,
  cluster_stats = cluster_stats
)

h_comp_long <- make_within_section_subclass_long(
  base = base,
  section_letter = "H",
  subclass_var = SUBCLASS_VAR,
  top_n = TOP_FIELDS_N,
  cluster_stats = cluster_stats
)

return(list(
  core = core,
  pat_long = pat_long,
  field_comp_long = field_comp_long,
  tenure_comp_long = tenure_comp_long,
  origin_long = origin_long,
  g_comp_long = g_comp_long,
  h_comp_long = h_comp_long,
  base_year = base
))
}  # closes build_panels()

# ============================
# Plot bundle for one combination
# ============================
plot_one <- function(pan, year_val, geo_name, immig_def) {

  core <- pan$core
  pat_long <- pan$pat_long
  field_comp_long <- pan$field_comp_long
  tenure_comp_long <- pan$tenure_comp_long
  origin_long <- pan$origin_long
  g_comp_long <- pan$g_comp_long
  h_comp_long <- pan$h_comp_long


  if (nrow(core) < 10) return(invisible(NULL))

  tag <- paste0("y", year_val, "__", geo_name, "__", immig_def)

  # -------------------------
  # A) overall immigrant share vs size and vs productivity
  # -------------------------
  save_scatter(
    df = core, x = "log_cluster", y = "immig_share", w = "cluster_size",
    title = paste0("Immigrant share vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
    xlab = "log(cluster size)  [cluster size = # inventors]",
    ylab = "Immigrant share",
    filename = paste0("scatter_share_vs_size__", tag, ".png")
  )
  save_scatter(
    df = core, x = "avg_prod", y = "immig_share", w = "cluster_size",
    title = paste0("Immigrant share vs avg cluster productivity (", year_val, ", ", geo_name, ", ", immig_def, ")"),
    xlab = "Average cluster productivity (mean patents per inventor-year)",
    ylab = "Immigrant share",
    filename = paste0("scatter_share_vs_prod__", tag, ".png")
  )

  # -------------------------
  # B) avg patents by group vs size/prod/share
  # includes 3 groups (nonimmigrant, immigrant, overall)
  # -------------------------
  if (n_distinct(pat_long$group) >= 2) {

    save_scatter(
      df = pat_long, x = "log_cluster", y = "avg_patents", color = "group", w = "cluster_size",
      title = paste0("Avg patents vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "log(cluster size)  [cluster size = # inventors]",
      ylab = "Average patents per inventor-year (group mean)",
      filename = paste0("scatter_avg_patents_vs_size__", tag, ".png")
    )

    save_scatter(
      df = pat_long, x = "avg_prod", y = "avg_patents", color = "group", w = "cluster_size",
      title = paste0("Avg patents vs avg cluster productivity (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "Average cluster productivity (mean patents per inventor-year)",
      ylab = "Average patents per inventor-year (group mean)",
      filename = paste0("scatter_avg_patents_vs_prod__", tag, ".png")
    )

    save_scatter(
      df = pat_long, x = "immig_share", y = "avg_patents", color = "group", w = "cluster_size",
      title = paste0("Avg patents vs overall immigrant share (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "Overall immigrant share in cluster-year",
      ylab = "Average patents per inventor-year (group mean)",
      filename = paste0("scatter_avg_patents_vs_immig_share__", tag, ".png")
    )
  }

  # (1) Origin composition: share of cluster = n_origin / cluster_size
  if (nrow(origin_long) > 0) {
    save_scatter(
      df = origin_long, x = "log_cluster", y = "share_cat", color = "origin", w = "cluster_size",
      title = paste0("Origin composition vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "log(cluster size)",
      ylab = "Share of cluster that are immigrants from origin",
      filename = paste0("composition_origin_vs_size__", tag, ".png"),
      point_alpha = 0.25
    )
  }

  # (3) Field composition among immigrants: n_immig_field / cluster_size
  if (nrow(field_comp_long) > 0) {
    save_scatter(
      df = field_comp_long, x = "log_cluster", y = "share_cat", color = "field", w = "cluster_size",
      title = paste0("Field composition (immigrants) vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "log(cluster size)",
      ylab = "Share of cluster that are immigrant inventors in field",
      filename = paste0("composition_field_vs_size__", tag, ".png"),
      point_alpha = 0.25
    )
  }

  # (4) Tenure composition among immigrants: n_immig_bin / cluster_size
  if (nrow(tenure_comp_long) > 0) {
    save_scatter(
      df = tenure_comp_long, x = "log_cluster", y = "share_cat", color = "tenure_bin", w = "cluster_size",
      title = paste0("Tenure-bin composition (immigrants) vs log cluster size (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      xlab = "log(cluster size)",
      ylab = "Share of cluster that are immigrant inventors in tenure bin",
      filename = paste0("composition_tenure_vs_size__", tag, ".png"),
      monotone_color = TRUE
    )
  }

  # (5)  Field based on class composition among immigrants: n_immig_field / cluster_size

if (nrow(g_comp_long) > 0) {
  save_scatter(
    df = g_comp_long, x = "log_cluster", y = "share_cat", color = "subcat", w = "cluster_size",
    title = paste0("Within class G: subclass composition among immigrants vs log cluster size (",
                   year_val, ", ", geo_name, ", ", immig_def, ")"),
    xlab = "log(cluster size)",
    ylab = "Share within G-immigrants (denom = # immigrants in class G in cluster)",
    filename = paste0("composition_G_subclass_vs_size__", tag, ".png"),
    point_alpha = 0.20
  )
}

if (nrow(h_comp_long) > 0) {
  save_scatter(
    df = h_comp_long, x = "log_cluster", y = "share_cat", color = "subcat", w = "cluster_size",
    title = paste0("Within class H: subclass composition among immigrants vs log cluster size (",
                   year_val, ", ", geo_name, ", ", immig_def, ")"),
    xlab = "log(cluster size)",
    ylab = "Share within H-immigrants (denom = # immigrants in class H in cluster)",
    filename = paste0("composition_H_subclass_vs_size__", tag, ".png"),
    point_alpha = 0.20
  )
}

}
# ============================
# (4) Degree distribution plots
# ============================

plot_degree_distribution <- function(ds, immig_def_for_degree, out_prefix) {

  deg_col <- "first_degree"

  df <- ds %>%
    select(user_id, first_country, all_of(deg_col), all_of(immig_def_for_degree)) %>%
    filter(first_country == "United States") %>%
    distinct() %>%
    collect() %>%
    mutate(
      immig_flag = as.integer(.data[[immig_def_for_degree]]),
      degree_std = .data[[deg_col]],   # <-- no standardization needed
      group = factor(ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
                     levels = c("nonimmigrant", "immigrant"))
    ) %>%
    filter(!is.na(degree_std), degree_std != "")

  if (nrow(df) == 0) return(invisible(NULL))

  # overall
  p1 <- ggplot(df, aes(x = degree_std)) +
    geom_bar() +
    labs(
      title = paste0("First degree distribution (all) — degree col: ", deg_col),
      x = "First degree",
      y = "# inventors"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 25, hjust = 1))
  ggsave(
    file.path(PLOTS_DIR, paste0(out_prefix, "__degree_dist_all.png")),
    p1, width = 8.5, height = 5.2, dpi = 200
  )

  # by immigrant status (dodged)
  p2 <- ggplot(df, aes(x = degree_std, fill = group)) +
    geom_bar(position = "dodge") +
    labs(
      title = paste0("First degree distribution by immigrant status — ", immig_def_for_degree),
      x = "First degree",
      y = "# inventors",
      fill = "Group"
    ) +
    theme_minimal() +
    theme(axis.text.x = element_text(angle = 25, hjust = 1))
  ggsave(
    file.path(PLOTS_DIR, paste0(out_prefix, "__degree_dist_by_group.png")),
    p2, width = 8.8, height = 5.2, dpi = 200
  )

  invisible(NULL)
}



# ============================
# MAIN LOOP: plots
# ============================
for (immig_def in immig_defs) {
  for (geo_name in names(geos)) {

    geo_var <- geos[[geo_name]]

    for (yy in years_to_run) {
      cat("\n====================================================\n")
      cat("[INFO] Year:", yy, "| Geo:", geo_name, "| Immig def:", immig_def, "\n")
      cat("====================================================\n")

      pan <- build_panels(
        ds = ds,
        year_val = yy,
        geo_var = geo_var,
        immig_var = immig_def,
        min_cluster = MIN_CLUSTER,
        first_pos_map = first_pos_map
      )

      if (nrow(pan$core) < 10) {
        cat("[WARN] Too few clusters after filters. Skipping.\n")
        next
      }

      plot_one(pan, yy, geo_name, immig_def)
      cat("[INFO] Done plots for:", "y", yy, geo_name, immig_def, "\n")
    }
  }
}


# ============================================================
# Robustness wrt cluster productivity + labeled plots
# ============================================================

# ---- packages ----
pkgs2 <- c("ggrepel", "scales")
for (p in pkgs2) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
library(ggrepel)
library(scales)

# -----------------------------------------
# robust coefs helper (HC1)
# -----------------------------------------
robust_vcov <- function(model) sandwich::vcovHC(model, type = "HC1")

# -----------------------------------------
# slope stats for interaction model:
# y ~ x * group  (weights = cluster_size)
# returns slope_native, slope_immig, slope_diff + SEs and p-values
# -----------------------------------------
slope_stats_interaction <- function(df, xvar, yvar = "avg_patents", wvar = "cluster_size") {
  # require both groups
  if (n_distinct(df$group) < 2) return(NULL)

  fml <- as.formula(paste0(yvar, " ~ ", xvar, " * group"))
  m <- lm(fml, data = df, weights = df[[wvar]])
  V <- robust_vcov(m)
  b <- coef(m)

  # names depend on factor coding; with levels nonimmigrant, immigrant,
  # interaction is xvar:groupimmigrant
  term_x  <- xvar
  term_ix <- paste0(xvar, ":groupimmigrant")
  if (!(term_ix %in% names(b))) {
    # fallback find
    term_ix <- grep(paste0("^", xvar, ":group"), names(b), value = TRUE)[1]
  }
  if (is.na(term_ix) || !(term_x %in% names(b))) return(NULL)

  slope_native <- unname(b[term_x])
  se_native    <- sqrt(V[term_x, term_x])

  slope_immig  <- unname(b[term_x] + b[term_ix])
  se_immig     <- sqrt(V[term_x, term_x] + V[term_ix, term_ix] + 2*V[term_x, term_ix])

  diff_slope <- unname(b[term_ix])
  se_diff    <- sqrt(V[term_ix, term_ix])

  z_native <- slope_native / se_native
  z_immig  <- slope_immig / se_immig
  z_diff   <- diff_slope / se_diff

  tibble(
    slope_native = slope_native,
    se_native = se_native,
    p_native = 2*pnorm(abs(z_native), lower.tail = FALSE),

    slope_immigrant = slope_immig,
    se_immigrant = se_immig,
    p_immigrant = 2*pnorm(abs(z_immig), lower.tail = FALSE),

    slope_diff = diff_slope,
    se_diff = se_diff,
    p_diff = 2*pnorm(abs(z_diff), lower.tail = FALSE)
  )
}

# -----------------------------------------
# pick which geo_levels to label
# (labels: top by cluster_size + extremes of x)
# -----------------------------------------
pick_labels <- function(df_onepoint, xvar, k_size = 12, k_x = 12) {
  # df_onepoint: one row per geo_level
  a <- df_onepoint %>% arrange(desc(cluster_size)) %>% slice_head(n = min(k_size, n()))
  b <- df_onepoint %>% arrange(desc(.data[[xvar]])) %>% slice_head(n = min(k_x, n()))
  c <- df_onepoint %>% arrange(.data[[xvar]]) %>% slice_head(n = min(k_x, n()))
  bind_rows(a, b, c) %>% distinct(geo_level, .keep_all = TRUE)
}

# -----------------------------------------
# labeled cluster-level plot: avg_prod vs size (or any y vs x)
# -----------------------------------------
save_labeled_cluster_plot <- function(core, xvar, yvar, year_val, geo_name, immig_def,
                                      fname_prefix, k_size = 12, k_x = 12) {

  core1 <- core %>%
    select(geo_level, cluster_size, log_cluster, avg_prod, all_of(yvar)) %>%
    filter(!is.na(.data[[xvar]]), !is.na(.data[[yvar]]))

  if (nrow(core1) == 0) return(invisible(NULL))

  lab <- pick_labels(core1, xvar = xvar, k_size = k_size, k_x = k_x)

  p <- ggplot(core1, aes(x = .data[[xvar]], y = .data[[yvar]])) +
    geom_point(alpha = 0.30) +
    geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
    ggrepel::geom_text_repel(
      data = lab,
      aes(label = geo_level),
      size = 3,
      max.overlaps = Inf,
      box.padding = 0.4,
      point.padding = 0.2,
      min.segment.length = 0
    ) +
    labs(
      title = paste0(fname_prefix, " (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      x = xvar,
      y = yvar
    ) +
    theme_minimal()

  ggsave(
    file.path(PLOTS_DIR, paste0(fname_prefix, "__y", year_val, "__", geo_name, "__", immig_def, "__labeled.png")),
    p, width = 10.5, height = 6.5, dpi = 220
  )

  invisible(NULL)
}

# -----------------------------------------
# labeled patents plots (two points per city: immigrant/nonimmigrant)
# label only ONCE per city by putting label on immigrant rows
# -----------------------------------------
save_labeled_patents_plot <- function(pat_long, xvar, year_val, geo_name, immig_def,
                                     fname_prefix, k_size = 12, k_x = 12) {

  df <- pat_long %>%
    filter(!is.na(.data[[xvar]]), !is.na(avg_patents)) %>%
    mutate(label_city = ifelse(group == "immigrant", geo_level, NA_character_))

  if (nrow(df) == 0) return(invisible(NULL))

  # build one-row-per-city frame to choose which cities to label
  one <- df %>%
    group_by(geo_level) %>%
    summarise(
      cluster_size = first(cluster_size),
      x = first(.data[[xvar]]),
      .groups = "drop"
    )
  lab_cities <- pick_labels(one %>% rename(!!xvar := x), xvar = xvar, k_size = k_size, k_x = k_x) %>%
    pull(geo_level)

  df <- df %>% mutate(label_city = ifelse(label_city %in% lab_cities, label_city, NA_character_))

  p <- ggplot(df, aes(x = .data[[xvar]], y = avg_patents, color = group)) +
    geom_point(alpha = 0.35) +
    geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
    ggrepel::geom_text_repel(
      aes(label = label_city),
      size = 3,
      max.overlaps = Inf,
      box.padding = 0.35,
      point.padding = 0.2,
      min.segment.length = 0,
      na.rm = TRUE
    ) +
    labs(
      title = paste0(fname_prefix, " (", year_val, ", ", geo_name, ", ", immig_def, ")"),
      x = xvar,
      y = "Average patents per inventor-year (group mean)",
      color = "Group"
    ) +
    theme_minimal()

  ggsave(
    file.path(PLOTS_DIR, paste0(fname_prefix, "__y", year_val, "__", geo_name, "__", immig_def, "__labeled.png")),
    p, width = 10.5, height = 6.5, dpi = 220
  )

  invisible(NULL)
}

cat("[DONE] Plots (selected years) in:\n  ", PLOTS_DIR, "\n\n")
