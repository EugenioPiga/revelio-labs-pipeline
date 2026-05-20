#!/usr/bin/env Rscript
###############################################################################
# Gender ratio diagnostics vs firm effects + firm observables (US-only)
# Answers:
#   (1) firm effects (parent FE) vs male_share_10y
#   (2) male_share_10y vs observable firm characteristics
#
# Data: /labs/khanna/linkedin_202507/processed/inventor_year_merged
# Output: /home/epiga/revelio_labs/output/gender_ratio_firm_diagnostics
#
# Notes:
# - Uses duckdb (no arrow) to read parquet.
# - US-only sample: first_country == "United States"
# - Male share computed from au_sex_predicted ∈ {"Male","Female"} (others -> NA)
# - male_share_10y: average over cluster-year male share in last 10 years.
###############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("duckdb","DBI","data.table","dplyr","readr","stringr","fixest","ggplot2","broom")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(duckdb); library(DBI)
  library(data.table)
  library(dplyr); library(readr); library(stringr)
  library(fixest)
  library(ggplot2)
  library(broom)
})

data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))
set.seed(123)

ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")

# =========================
# 1) Paths + knobs
# =========================
INPUT_DIR <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged_v2"
OUT_BASE  <- "/home/epiga/revelio_labs/output/gender_ratio_firm_diagnostics"

dir.create(OUT_BASE, recursive = TRUE, showWarnings = FALSE)
DIRS <- list(
  tables  = file.path(OUT_BASE, "tables"),
  figs    = file.path(OUT_BASE, "figures"),
  models  = file.path(OUT_BASE, "models")
)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

US_COUNTRY <- "United States"

YEAR_START <- 2010
YEAR_END   <- 2024
LAST10_N <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END

TENURE_MAX  <- 50
MIN_CLUSTER <- 10  # minimum avg cluster size (last10y) to keep in firm-level dataset

# Core variables
GENDER_VAR <- "au_sex_predicted"
PARENT_VAR <- "first_parent_rcid"

# =========================
# 2) Read parquet via duckdb (schema-safe)
# =========================
ts_msg("Connecting duckdb (memory) ...")
con <- dbConnect(duckdb(), dbdir=":memory:")

ts_msg("Discovering available columns ...")
schema0 <- dbGetQuery(con, sprintf(
  "SELECT * FROM parquet_scan('%s/**/*.parquet') LIMIT 0",
  INPUT_DIR
))
avail_cols <- names(schema0)

need_cols <- c(
  "user_id","year","n_patents",
  "first_country",
  "first_startdate_edu","first_startdate_pos",
  PARENT_VAR,
  GENDER_VAR
)

keep_cols <- intersect(need_cols, avail_cols)
miss_cols <- setdiff(need_cols, keep_cols)

if (length(miss_cols) > 0) {
  ts_msg("WARNING: missing cols (will break if essential):", paste(miss_cols, collapse=", "))
}

select_sql <- paste(keep_cols, collapse=", ")

ts_msg("Reading parquet with duckdb ...")
sql <- sprintf("
  SELECT %s
  FROM parquet_scan('%s/**/*.parquet')
  WHERE year BETWEEN %d AND %d
", select_sql, INPUT_DIR, YEAR_START, YEAR_END)

df <- dbGetQuery(con, sql)
dbDisconnect(con, shutdown=TRUE)
ts_msg("Raw rows loaded:", nrow(df))

# =========================
# 3) Clean + US-only + tenure
# =========================
ts_msg("Cleaning + filtering US-only ...")
df <- df %>%
  mutate(
    user_id = as.character(user_id),
    year    = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    first_startdate_edu = as.character(first_startdate_edu),
    first_startdate_pos = as.character(first_startdate_pos),
    cluster_id = as.character(.data[[PARENT_VAR]]),
    sex_raw = str_trim(as.character(.data[[GENDER_VAR]]))
  ) %>%
  filter(first_country == US_COUNTRY) %>%
  filter(!is.na(cluster_id), cluster_id != "")

ts_msg("US-only rows:", nrow(df))

compute_tenure <- function(dd) {
  dd %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(first_startdate_edu, 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(first_startdate_pos, 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    ) %>%
    filter(!is.na(tenure), is.finite(tenure), is.finite(tenure_sq))
}

df <- compute_tenure(df)
ts_msg("After tenure filter:", nrow(df))

# gender coding: Male=1, Female=0, else NA
df <- df %>%
  mutate(
    male = dplyr::case_when(
      sex_raw == "Male"   ~ 1L,
      sex_raw == "Female" ~ 0L,
      TRUE ~ NA_integer_
    )
  )

# Convert to data.table for speed
dt <- as.data.table(df)
rm(df); gc()

# =========================
# 4) Build parent-level last-10y observables (gender ratio + firm chars)
# =========================
ts_msg("Building cluster-year composition stats ...")

# Composition at cluster-year: use unique users (avoid double counting if any duplicates)
dt_comp <- unique(dt[, .(user_id, year, cluster_id, male)])

# cluster-year: size + male share among non-missing + missing share
cy_gender <- dt_comp[, {
  n_inv <- uniqueN(user_id)
  n_known <- sum(!is.na(male))
  mshare <- if (n_known > 0) sum(male == 1L, na.rm = TRUE) / n_known else NA_real_
  miss_share <- if (n_inv > 0) 1 - (n_known / n_inv) else NA_real_
  list(
    cluster_size_y = n_inv,
    n_gender_known_y = n_known,
    male_share_y = mshare,
    gender_missing_share_y = miss_share
  )
}, by = .(cluster_id, year)]

# cluster-year: additional observables from inventor-year rows (tenure, patents)
cy_other <- dt[, .(
  mean_tenure_y  = mean(tenure, na.rm = TRUE),
  mean_patents_y = mean(n_patents, na.rm = TRUE),
  total_patents_y = sum(n_patents, na.rm = TRUE),
  n_user_year_y  = .N
), by = .(cluster_id, year)]

setkey(cy_gender, cluster_id, year)
setkey(cy_other,  cluster_id, year)
cy <- merge(cy_gender, cy_other, by = c("cluster_id","year"), all.x = TRUE)

# last-10y window
cy10 <- cy[year >= LAST10_START & year <= LAST10_END]

ts_msg("Aggregating parent-level last-10y characteristics ...")
firm_chars <- cy10[, .(
  cluster_size_10y = mean(cluster_size_y, na.rm = TRUE),
  male_share_10y   = mean(male_share_y, na.rm = TRUE),
  gender_missing_share_10y = mean(gender_missing_share_y, na.rm = TRUE),
  mean_tenure_10y  = mean(mean_tenure_y, na.rm = TRUE),
  mean_patents_10y = mean(mean_patents_y, na.rm = TRUE),
  total_patents_10y = sum(total_patents_y, na.rm = TRUE),
  years_used_10y   = .N
), by = .(cluster_id)]

firm_chars[, log_size_10y := ifelse(is.finite(cluster_size_10y) & cluster_size_10y > 0, log(cluster_size_10y), NA_real_)]

# keep reasonable clusters
firm_chars <- firm_chars[
  is.finite(cluster_size_10y) & cluster_size_10y >= MIN_CLUSTER &
  is.finite(male_share_10y)
]

ts_msg("Firm chars rows:", nrow(firm_chars))

# Save firm chars
fwrite(firm_chars, file.path(DIRS$tables, "firm_chars_last10y.csv"))

# =========================
# 5) Estimate baseline PPML to extract parent FE
# =========================
ts_msg("Estimating baseline PPML with parent FE: n_patents ~ tenure + tenure_sq | user_id + year + cluster_id ...")

# restrict estimation sample to firms in firm_chars (stable window)
setkey(firm_chars, cluster_id)
dt_est <- dt[firm_chars, on = "cluster_id", nomatch = 0]

# fixest likes factors for FE
dt_est[, user_id_f := as.factor(user_id)]
dt_est[, year_f := as.factor(year)]
dt_est[, cluster_f := as.factor(cluster_id)]

# baseline PPML (unweighted)
t0 <- Sys.time()
m_base <- fixest::fepois(
  n_patents ~ tenure + tenure_sq | user_id_f + year_f + cluster_f,
  data = dt_est,
  vcov = ~ cluster_id,
  notes = FALSE,
  warn  = FALSE
)
runtime <- round(as.numeric(Sys.time() - t0, units = "secs"), 2)
ts_msg("PPML runtime (sec):", runtime)

saveRDS(m_base, file.path(DIRS$models, "ppml_baseline_parentFE_unweighted.rds"))

# Extract parent FE (cluster_f block)
fe_list <- fixest::fixef(m_base)
if (!("cluster_f" %in% names(fe_list))) stop("Did not find cluster_f fixed effects in fixef().")

fe_parent <- fe_list[["cluster_f"]]
fe_df <- data.table(cluster_id = names(fe_parent), parent_fe = as.numeric(fe_parent))
fwrite(fe_df, file.path(DIRS$tables, "parent_fe_unweighted.csv"))

# Merge firm chars + parent FE
setkey(fe_df, cluster_id)
firm <- merge(firm_chars, fe_df, by = "cluster_id", all.x = TRUE)
firm <- firm[is.finite(parent_fe)]
fwrite(firm, file.path(DIRS$tables, "firm_chars_plus_parentFE.csv"))

ts_msg("Merged firm dataset rows:", nrow(firm))

# =========================
# 6) Answer Q1: firm FE vs male_share_10y
# =========================
ts_msg("Q1: parent_fe vs male_share_10y ...")

# correlations
corr1 <- data.table(
  metric = c("pearson", "spearman"),
  cor = c(
    suppressWarnings(cor(firm$parent_fe, firm$male_share_10y, use="complete.obs", method="pearson")),
    suppressWarnings(cor(firm$parent_fe, firm$male_share_10y, use="complete.obs", method="spearman"))
  )
)
fwrite(corr1, file.path(DIRS$tables, "corr_parentFE_vs_maleShare.csv"))

# firm-level regressions
r1 <- feols(parent_fe ~ male_share_10y, data = firm, vcov = "hetero")
r2 <- feols(parent_fe ~ male_share_10y + log_size_10y + mean_tenure_10y + gender_missing_share_10y, data = firm, vcov = "hetero")

reg_tab <- bind_rows(
  broom::tidy(r1) %>% mutate(model="parent_fe ~ male_share"),
  broom::tidy(r2) %>% mutate(model="parent_fe ~ male_share + log_size + mean_tenure + missing_gender")
)
write_csv(reg_tab, file.path(DIRS$tables, "reg_parentFE_on_maleShare.csv"))

# scatter + binned scatter
p_scatter <- ggplot(firm, aes(x = male_share_10y, y = parent_fe)) +
  geom_point(alpha = 0.12, size = 0.8) +
  geom_smooth(method = "lm", se = TRUE) +
  labs(
    title = "Parent fixed effects vs male share (last-10y)",
    x = "male_share_10y (parent-level, last-10y avg)",
    y = "parent FE (from baseline PPML: inventor FE + year FE + parent FE)"
  ) +
  theme_minimal()

ggsave(file.path(DIRS$figs, "scatter_parentFE_vs_maleShare.png"), p_scatter, width = 9, height = 6, dpi = 200)

# binned
firm_bin <- as.data.table(firm)
firm_bin <- firm_bin[is.finite(male_share_10y) & is.finite(parent_fe)]

# NOTE: ntile() is from dplyr, not data.table
firm_bin[, bin := dplyr::ntile(male_share_10y, 20)]

bin_df <- firm_bin[, .(
  male_share_bin = mean(male_share_10y, na.rm = TRUE),
  parent_fe_bin  = mean(parent_fe, na.rm = TRUE),
  n_firms = .N
), by = bin][order(bin)]

fwrite(bin_df, file.path(DIRS$tables, "bins_parentFE_vs_maleShare.csv"))

p_bin <- ggplot(bin_df, aes(x = male_share_bin, y = parent_fe_bin)) +
  geom_point() +
  geom_line() +
  labs(
    title = "Binned parent FE vs male share (20 bins)",
    x = "male_share_10y (bin mean)",
    y = "parent FE (bin mean)"
  ) +
  theme_minimal()

ggsave(file.path(DIRS$figs, "binned_parentFE_vs_maleShare.png"), p_bin, width = 9, height = 6, dpi = 200)

# =========================
# 7) Answer Q2: male_share_10y vs observable firm characteristics
# =========================
ts_msg("Q2: male_share_10y vs firm observables ...")

obs_vars <- c("log_size_10y","cluster_size_10y","mean_tenure_10y","mean_patents_10y","total_patents_10y","gender_missing_share_10y","years_used_10y","parent_fe")

corr_rows <- lapply(obs_vars, function(v) {
  x <- firm$male_share_10y
  y <- firm[[v]]
  data.table(
    var = v,
    pearson  = suppressWarnings(cor(x, y, use="complete.obs", method="pearson")),
    spearman = suppressWarnings(cor(x, y, use="complete.obs", method="spearman"))
  )
})
corr2 <- rbindlist(corr_rows)
fwrite(corr2, file.path(DIRS$tables, "corr_maleShare_vs_firm_observables.csv"))

# simple regressions of observables on male share (firm-level)
reg_obs <- lapply(obs_vars, function(v) {
  fml <- as.formula(paste0(v, " ~ male_share_10y"))
  m <- feols(fml, data = firm, vcov = "hetero")
  broom::tidy(m) %>% mutate(depvar = v)
})
reg_obs_tab <- bind_rows(reg_obs)
write_csv(reg_obs_tab, file.path(DIRS$tables, "reg_firm_observables_on_maleShare.csv"))

# quick scatter plots for a few key observables
plot_one <- function(yvar, fname) {
  p <- ggplot(firm, aes(x = male_share_10y, y = .data[[yvar]])) +
    geom_point(alpha = 0.12, size = 0.8) +
    geom_smooth(method = "lm", se = TRUE) +
    labs(
      title = paste0(yvar, " vs male share (last-10y)"),
      x = "male_share_10y",
      y = yvar
    ) +
    theme_minimal()
  ggsave(file.path(DIRS$figs, fname), p, width = 9, height = 6, dpi = 200)
}

plot_one("log_size_10y", "scatter_logSize_vs_maleShare.png")
plot_one("mean_tenure_10y", "scatter_meanTenure_vs_maleShare.png")
plot_one("mean_patents_10y", "scatter_meanPatents_vs_maleShare.png")
plot_one("gender_missing_share_10y", "scatter_missingGender_vs_maleShare.png")

# =========================
# 8) Write a short “answer summary” text file for Slack/email
# =========================
ts_msg("Writing summary text ...")

summary_path <- file.path(OUT_BASE, "SUMMARY.txt")
cat(
  "Gender ratio diagnostics (US-only, parent-level)\n",
  "================================================\n\n",
  sprintf("Data window: years %d-%d | last-10y window: %d-%d\n", YEAR_START, YEAR_END, LAST10_START, LAST10_END),
  sprintf("Kept firms: avg cluster_size_10y >= %d and finite male_share_10y\n\n", MIN_CLUSTER),

  "Q1) Firm effects vs gender ratio\n",
  "--------------------------------\n",
  "We estimate baseline PPML: n_patents ~ tenure + tenure_sq | inventor FE + year FE + parent FE\n",
  "Then merge parent FE with male_share_10y and compute correlations + firm-level regressions.\n\n",
  "See outputs:\n",
  "  tables/corr_parentFE_vs_maleShare.csv\n",
  "  tables/reg_parentFE_on_maleShare.csv\n",
  "  figures/scatter_parentFE_vs_maleShare.png\n",
  "  figures/binned_parentFE_vs_maleShare.png\n\n",

  "Q2) Male share vs observable firm characteristics\n",
  "-------------------------------------------------\n",
  "We compute parent-level last-10y observables:\n",
  "  cluster_size_10y, log_size_10y, mean_tenure_10y, mean_patents_10y, total_patents_10y,\n",
  "  gender_missing_share_10y, years_used_10y\n",
  "Then report correlation table + univariate regressions of each observable on male_share_10y.\n\n",
  "See outputs:\n",
  "  tables/corr_maleShare_vs_firm_observables.csv\n",
  "  tables/reg_firm_observables_on_maleShare.csv\n",
  "  figures/scatter_*_vs_maleShare.png\n\n",
  file = summary_path, sep = ""
)

ts_msg("DONE. Outputs in:", OUT_BASE)
