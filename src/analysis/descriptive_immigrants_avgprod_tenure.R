#!/usr/bin/env Rscript
###############################################################################
# THREE GRAPHS (PATENTS, not citations)
# 1) Avg patents of immigrants vs nonimmigrants over tenure
# 1b) SAME AS (1) but for specific origins vs natives
# 2) Avg patents vs cluster size after controlling for tenure (residualized)
# 3) Same as (2) but focusing on immigrants from particular origins (vs natives)
#
# Uses Arrow dataset: inventor_year_merged
# Top10 inventors defined by total patents (as in your pipeline)
###############################################################################

user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow","dplyr","tidyr","readr","ggplot2","stringr")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(tidyr); library(readr)
  library(ggplot2); library(stringr)
})

set.seed(123)

# =========================
# Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/descriptives_immigrants/tenure_patents"

PLOTS_DIR  <- file.path(OUT_DIR, "plots_patents")
TABLES_DIR <- file.path(OUT_DIR, "tables_patents")
dir.create(PLOTS_DIR,  recursive = TRUE, showWarnings = FALSE)
dir.create(TABLES_DIR, recursive = TRUE, showWarnings = FALSE)

# clustering definition
geos <- list(
  state = "first_state",
  metro = "first_metro_area"
)

# immigrant definition
IMMIG_VAR <- "immig_first_deg_or_job_nonUS"

# years
USE_LAST_N_YEARS <- 10
SKIP_YEARS <- c(2025)

# cluster filter
MIN_CLUSTER <- 10

# tenure bins
TENURE_MAX <- 50
TENURE_BIN_WIDTH <- 5  # for smooth tenure profile plot

# origins you care about (edit freely)
ORIGINS_KEEP <- c("India","China","United Kingdom","Japan","South Korea")

# =========================
# Helpers
# =========================
need_cols <- function(ds, cols) {
  nm <- names(ds)
  missing <- setdiff(cols, nm)
  if (length(missing) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(missing, collapse=", ")))
}

compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure)
    )
}

make_tenure_bins <- function(tenure, width = TENURE_BIN_WIDTH, maxv = TENURE_MAX) {
  br <- seq(0, maxv, by = width)
  cut(tenure, breaks = br, right = FALSE, include.lowest = TRUE)
}

# origin = first_university_country if non-US, else first_pos_country if non-US
make_origin <- function(df) {
  df %>%
    mutate(
      edu_c = str_trim(as.character(first_university_country)),
      pos_c = str_trim(as.character(first_pos_country)),
      edu_missing = is.na(edu_c) | edu_c %in% c("", "empty"),
      pos_missing = is.na(pos_c) | pos_c %in% c("", "empty"),
      edu_is_us = !edu_missing & edu_c == "United States",
      pos_is_us = !pos_missing & pos_c == "United States",
      edu_nonUS = !edu_missing & !edu_is_us,
      pos_nonUS = !pos_missing & !pos_is_us,
      origin = case_when(
        edu_nonUS ~ edu_c,
        !edu_nonUS & pos_nonUS ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# =========================
# (1) Open dataset + Top10 inventors by total patents
# =========================
cat("\n[INFO] Opening Arrow dataset...\n")
ds0 <- open_dataset(INPUT, format = "parquet")

need_cols(ds0, c("user_id","year","n_patents"))
cat("[INFO] Computing top 10% inventors by total patents...\n")

inv_tot <- ds0 %>%
  select(user_id, n_patents) %>%
  group_by(user_id) %>%
  summarise(total_pat = sum(n_patents, na.rm = TRUE), .groups="drop") %>%
  collect()

p90 <- quantile(inv_tot$total_pat, 0.90, na.rm = TRUE)
top10 <- inv_tot %>% filter(total_pat >= p90) %>% pull(user_id)
cat("[INFO] Top10 inventors:", length(top10), "\n")

ds <- ds0 %>% filter(user_id %in% top10)
rm(ds0, inv_tot)

# years_to_run
years_all <- ds %>% select(year) %>% distinct() %>% collect() %>% pull(year) %>% sort()
years_to_run <- tail(years_all, USE_LAST_N_YEARS)
years_to_run <- setdiff(years_to_run, SKIP_YEARS)
cat("[INFO] Running years:", paste(years_to_run, collapse=", "), "\n")

# =========================
# (2) Compute first_pos_country map once
# =========================
need_cols(ds, c("first_country","last_country"))
cat("[INFO] Computing first_pos_country map...\n")

pos_df <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(pos_country_y = coalesce(last_country, first_country)) %>%
  filter(!is.na(pos_country_y), pos_country_y != "") %>%
  distinct(user_id, year, pos_country_y) %>%
  collect()

first_pos_map <- pos_df %>%
  group_by(user_id) %>%
  slice_min(order_by = year, n = 1, with_ties = FALSE) %>%
  ungroup() %>%
  transmute(user_id, first_pos_country = pos_country_y)

rm(pos_df)

# =========================
# Build a base slice (year x geo) with patents outcome
# =========================
build_base <- function(ds, year_val, geo_var) {
  req <- c("user_id","year","n_patents","first_country", geo_var, IMMIG_VAR,
           "first_startdate_edu","first_startdate_pos",
           "first_university_country")
  need_cols(ds, req)

  base <- ds %>%
    select(any_of(req)) %>%
    filter(year == year_val) %>%
    filter(first_country == "United States") %>%
    filter(!is.na(.data[[geo_var]])) %>%
    mutate(immig_flag = as.integer(.data[[IMMIG_VAR]])) %>%
    filter(immig_flag %in% c(0L, 1L)) %>%
    collect()

  if (nrow(base) == 0) return(tibble())

  base %>%
    left_join(first_pos_map, by = "user_id") %>%
    mutate(
      geo_level = as.character(.data[[geo_var]]),
      n_patents = as.numeric(n_patents)
    ) %>%
    filter(!is.na(geo_level), geo_level != "")
}

# cluster stats from *patents* (overall cluster size, not subgroup size)
compute_cluster_stats <- function(base, min_cluster = MIN_CLUSTER) {
  base %>%
    group_by(geo_level) %>%
    summarise(
      cluster_size = n_distinct(user_id),
      avg_prod     = mean(n_patents, na.rm = TRUE),
      n_immigrant  = n_distinct(user_id[immig_flag == 1L]),
      immig_share  = ifelse(cluster_size > 0, n_immigrant / cluster_size, NA_real_),
      .groups="drop"
    ) %>%
    mutate(log_cluster = log(cluster_size)) %>%
    filter(cluster_size >= min_cluster) %>%
    filter(is.finite(log_cluster), is.finite(avg_prod))
}

# =========================
# Containers for pooled outputs across years
# =========================
pool_tenure <- list()
pool_tenure_origin <- list()  
pool_resid  <- list()
pool_origin <- list()

# =========================
# Main loop across years & geos
# =========================
for (geo_name in names(geos)) {
  geo_var <- geos[[geo_name]]

  for (yy in years_to_run) {
    cat("\n[INFO] Year:", yy, "| Geo:", geo_name, "\n")

    base <- build_base(ds, yy, geo_var)
    if (nrow(base) == 0) next

    cluster_stats <- compute_cluster_stats(base, MIN_CLUSTER)
    if (nrow(cluster_stats) < 10) next

    # keep only valid clusters
    base <- base %>% semi_join(cluster_stats %>% select(geo_level), by = "geo_level")

    # tenure + bins + baseline immigrant/nonimmigrant group
    base <- compute_tenure(base) %>% filter(!is.na(tenure))
    base <- base %>% mutate(
      tenure_bin = make_tenure_bins(tenure),
      group = factor(ifelse(immig_flag == 1L, "immigrant", "nonimmigrant"),
                     levels = c("nonimmigrant","immigrant"))
    )

    # =========================
    # Task 1: Avg patents over tenure (by group)
    # =========================
    t1 <- base %>%
      group_by(tenure_bin, group) %>%
      summarise(
        avg_patents = mean(n_patents, na.rm = TRUE),
        n = n_distinct(user_id),
        .groups="drop"
      ) %>%
      mutate(year = yy, geo = geo_name)

    pool_tenure[[paste0(geo_name,"_",yy)]] <- t1

    # =========================
    # Task 1b: Avg patents over tenure (origin-specific immigrants vs natives)
    # Natives restricted to clusters where that origin appears.
    # =========================
    base_o1 <- make_origin(base)

    for (o in ORIGINS_KEEP) {
      imm_o <- base_o1 %>% filter(immig_flag == 1L, origin == o)
      if (nrow(imm_o) == 0) next

      clusters_o <- unique(imm_o$geo_level)
      nat_o <- base_o1 %>% filter(immig_flag == 0L, geo_level %in% clusters_o)
      if (nrow(nat_o) == 0) next

      sub1 <- bind_rows(
        imm_o %>% mutate(type = "immigrant"),
        nat_o %>% mutate(type = "native")
      ) %>%
        mutate(type = factor(type, levels = c("native","immigrant")))

      t1b <- sub1 %>%
        group_by(tenure_bin, type) %>%
        summarise(
          avg_patents = mean(n_patents, na.rm = TRUE),
          n = n_distinct(user_id),
          .groups="drop"
        ) %>%
        mutate(year = yy, geo = geo_name, origin = o)

      pool_tenure_origin[[paste0(geo_name,"_",yy,"_",o)]] <- t1b
    }

    # =========================
    # Task 2: Residualized patents vs log(cluster size), by immigrant/nonimmigrant
    # Residualize at inventor level within year slice; then average residuals at cluster x group.
    # =========================
    base_res <- base %>%
      left_join(cluster_stats, by = "geo_level")

    m <- lm(n_patents ~ tenure + group, data = base_res)  # per-year slice
    base_res <- base_res %>% mutate(resid_pat = resid(m))

    t2 <- base_res %>%
      group_by(geo_level, group) %>%
      summarise(
        avg_resid_pat = mean(resid_pat, na.rm = TRUE),
        n_group = n_distinct(user_id),
        .groups="drop"
      ) %>%
      left_join(cluster_stats, by="geo_level") %>%
      mutate(year = yy, geo = geo_name)

    pool_resid[[paste0(geo_name,"_",yy)]] <- t2

    # =========================
    # Task 3: Origin-specific (immigrants from origin vs natives), tenure-adjusted
    # (weights/point sizes ALWAYS use overall cluster_size, not subgroup size)
    # =========================
    base_o <- make_origin(base_res)

    for (o in ORIGINS_KEEP) {
      imm_o <- base_o %>% filter(immig_flag == 1L, origin == o)
      if (nrow(imm_o) == 0) next

      clusters_o <- unique(imm_o$geo_level)
      nat_o <- base_o %>% filter(immig_flag == 0L, geo_level %in% clusters_o)

      sub <- bind_rows(
        imm_o %>% mutate(subgroup = paste0("imm_", o)),
        nat_o %>% mutate(subgroup = "native")
      ) %>%
        mutate(subgroup = factor(subgroup, levels = c("native", paste0("imm_", o))))

      if (n_distinct(sub$subgroup) < 2) next

      m3 <- lm(n_patents ~ tenure + subgroup, data = sub)
      sub <- sub %>% mutate(resid_pat = resid(m3))

      t3 <- sub %>%
        group_by(geo_level, subgroup) %>%
        summarise(
          avg_resid_pat = mean(resid_pat, na.rm = TRUE),
          n_group = n_distinct(user_id),   # kept for reference only (NOT used as weights)
          .groups="drop"
        ) %>%
        left_join(cluster_stats, by="geo_level") %>%
        mutate(year = yy, geo = geo_name, origin = o)

      pool_origin[[paste0(geo_name,"_",yy,"_",o)]] <- t3
    }
  }
}

# =========================
# Combine pooled outputs
# =========================
df_tenure <- bind_rows(pool_tenure)
df_tenure_origin <- bind_rows(pool_tenure_origin)  # NEW
df_resid  <- bind_rows(pool_resid)
df_origin <- bind_rows(pool_origin)

# =========================
# Save tables
# =========================
write_csv(df_tenure,        file.path(TABLES_DIR, "avg_patents_over_tenure.csv"))
write_csv(df_tenure_origin, file.path(TABLES_DIR, "avg_patents_over_tenure_by_origin.csv")) # NEW
write_csv(df_resid,         file.path(TABLES_DIR, "resid_patents_vs_logcluster.csv"))
write_csv(df_origin,        file.path(TABLES_DIR, "origin_resid_patents_vs_logcluster.csv"))

# =========================
# Plots
# =========================

# ---- Task 1 plot (pooled over years): avg patents over tenure
if (nrow(df_tenure) > 0) {

p1 <- ggplot(df_tenure, aes(x = tenure_bin, y = avg_patents, color = group, group = group)) +
  stat_summary(fun = mean, geom = "line", linewidth = 1.0) +
  stat_summary(fun = mean, geom = "point", alpha = 0.9) +
  labs(
    title = "Avg patents over tenure (immigrants vs nonimmigrants) — pooled across years",
    x = "Tenure bin", y = "Avg patents (inventor-year)",
    color = "Group"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))
ggsave(file.path(PLOTS_DIR, "avg_patents_over_tenure.png"), p1, width = 10.5, height = 6.2, dpi = 220)

}

# ---- Task 1b plot (pooled over years): origin immigrants vs natives over tenure
if (nrow(df_tenure_origin) > 0) {

p1b <- ggplot(df_tenure_origin, aes(x = tenure_bin, y = avg_patents, color = type, group = type)) +
  stat_summary(fun = mean, geom = "line", linewidth = 1.0) +
  stat_summary(fun = mean, geom = "point", alpha = 0.9) +
  facet_wrap(~ origin, scales = "free_y") +
  labs(
    title = "Avg patents over tenure: origin-specific immigrants vs natives — pooled across years",
    subtitle = "Natives restricted to clusters where that origin appears (in that year slice).",
    x = "Tenure bin", y = "Avg patents (inventor-year)",
    color = "Group"
  ) +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 35, hjust = 1))
ggsave(file.path(PLOTS_DIR, "avg_patents_over_tenure_by_origin.png"), p1b, width = 12.0, height = 8.0, dpi = 220)

}

# ---- Task 2 plot: residualized patents vs log cluster size
if (nrow(df_resid) > 0) {

  for (g in unique(df_resid$geo)) {
    for (yy in unique(df_resid$year)) {

      d <- df_resid %>% filter(geo == g, year == yy)
      if (nrow(d) == 0) next

      p2 <- ggplot(d, aes(x = log_cluster, y = avg_resid_pat, color = group)) +
        geom_point(aes(size = cluster_size), alpha = 0.25) +
        geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
        labs(
          title = paste0("Tenure-adjusted patents vs log(cluster size) — ", g, ", ", yy),
          subtitle = "Residuals from inventor-level regression on tenure (within year), averaged by cluster. Lines weighted by overall cluster_size.",
          x = "log(cluster size)", y = "Avg residual patents",
          color = "Group"
        ) +
        theme_minimal() +
        scale_size_continuous(range = c(0.6, 5.5), guide = "none")

      ggsave(
        file.path(PLOTS_DIR, paste0("resid_patents_vs_logcluster__", g, "__y", yy, ".png")),
        p2, width = 9.5, height = 6.2, dpi = 220
      )
    }
  }
}

# ---- Task 3 plot: origin-specific tenure-adjusted vs log cluster size
# IMPORTANT: point sizes + regression weights use overall cluster_size (NOT n_group)
if (nrow(df_origin) > 0) {

  for (g in unique(df_origin$geo)) {
    for (yy in unique(df_origin$year)) {
      for (o in unique(df_origin$origin)) {

        d <- df_origin %>% filter(geo == g, year == yy, origin == o)
        if (nrow(d) == 0) next
        if (n_distinct(d$subgroup) < 2) next

        p3 <- ggplot(d, aes(x = log_cluster, y = avg_resid_pat, color = subgroup)) +
          geom_point(aes(size = cluster_size), alpha = 0.25) +
          geom_smooth(method = "lm", aes(weight = cluster_size), se = FALSE) +
          labs(
            title = paste0("Tenure-adjusted patents vs log(cluster size): ", o, " vs natives — ", g, ", ", yy),
            subtitle = "Residuals computed within origin-specific sample. Lines weighted by overall cluster_size (not subgroup size).",
            x = "log(cluster size)", y = "Avg residual patents",
            color = "Group"
          ) +
          theme_minimal() +
          scale_size_continuous(range = c(0.6, 5.0), guide = "none")

        ggsave(
          file.path(PLOTS_DIR, paste0(str_replace_all(o, "[^A-Za-z0-9]+", "_"),
                                      "__", g, "__y", yy, ".png")),
          p3, width = 9.5, height = 6.2, dpi = 220
        )
      }
    }
  }
}

cat("\n[DONE]\nOutputs in:\n", OUT_DIR, "\n")
