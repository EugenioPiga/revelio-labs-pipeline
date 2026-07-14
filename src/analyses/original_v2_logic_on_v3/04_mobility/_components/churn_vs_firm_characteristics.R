#!/usr/bin/env Rscript

suppressPackageStartupMessages({
  library(arrow)
  library(data.table)
  library(dplyr)
  library(ggplot2)
  library(readr)
  library(stringr)
  library(tidyr)
  library(scales)
})
options(arrow.skip_nul = TRUE)
options(bitmapType = "cairo")
set.seed(123)

# =========================================================
# Config
# =========================================================
INPUT_DIR <- Sys.getenv("REVELIO_V3_INPUT", unset = "")
OUT_DIR   <- Sys.getenv("REVELIO_V3_OUTPUT_DIR", unset = "")
if (!nzchar(INPUT_DIR) || !dir.exists(INPUT_DIR)) stop("[ERROR] REVELIO_V3_INPUT is unset or invalid.")
if (!nzchar(OUT_DIR)) stop("[ERROR] REVELIO_V3_OUTPUT_DIR is unset.")
YEAR_MIN  <- 2010L
YEAR_MAX  <- 2024L
US_ONLY   <- TRUE
US_COUNTRY <- "United States"
MIN_POINTS_PLOT <- 250L
TOP_THRESHOLDS <- c(0.75, 0.90, 0.95)
EVENT_WINDOW <- 5L
OBS_GE_MIN <- 5L

MOVE_DEFS <- c("consecutive", "lastobs")
RUN_EVENT_STUDIES <- TRUE
RUN_HETEROGENEITY <- TRUE
COMPUTE_LOO <- TRUE

PARENT_VAR <- "first_parent_rcid"
RCID_VAR   <- "first_rcid"
CITY_VAR   <- "first_city"
METRO_VAR  <- "first_metro_area"
STATE_VAR  <- "first_state"
REGION_VAR <- "first_region"
COUNTRY_VAR <- "first_country"

IMMIG_VAR <- "immig_first_deg_or_job_nonUS"
IMMIG_CANDIDATES <- c("immig_first_deg_or_job_nonUS", "immig1_post", "immig1_ever", "immig2_post", "immig2_ever")
SENIORITY_CANDIDATES <- c("avg_seniority", "min_seniority", "max_seniority")
COMP_CANDIDATES <- c("avg_salary", "avg_total_comp", "min_salary", "max_salary", "min_total_comp", "max_total_comp")
GEO_CANDIDATES <- c(CITY_VAR, METRO_VAR, STATE_VAR, REGION_VAR, COUNTRY_VAR)

# =========================================================
# Helpers
# =========================================================
ensure_dir <- function(x) dir.create(x, recursive = TRUE, showWarnings = FALSE)

audit_dir <- file.path(OUT_DIR, "audit")
panel_dir <- file.path(OUT_DIR, "mobility_panel")
od_dir    <- file.path(OUT_DIR, "origin_destination")
event_dir <- file.path(OUT_DIR, "event_studies")
het_dir   <- file.path(OUT_DIR, "heterogeneity")
fig_dir   <- file.path(OUT_DIR, "figures")
for (d in c(OUT_DIR, audit_dir, panel_dir, od_dir, event_dir, het_dir, fig_dir)) ensure_dir(d)

ts_msg <- function(...) {
  cat(sprintf("[%s] ", format(Sys.time(), "%Y-%m-%d %H:%M:%S")), ..., "\n")
}

nz_trim <- function(x) {
  x <- str_trim(as.character(x))
  x[x %in% c("", "NA", "NULL", "N/A")] <- NA_character_
  x
}

num_or_na <- function(x) suppressWarnings(as.numeric(x))

safe_share <- function(x) {
  if (all(is.na(x))) return(NA_real_)
  mean(x, na.rm = TRUE)
}

hhi_div <- function(x) {
  x <- x[!is.na(x) & x != ""]
  if (length(x) == 0) return(NA_real_)
  p <- as.numeric(table(x) / length(x))
  1 - sum(p^2)
}

safe_quantile <- function(x, probs) {
  x <- x[is.finite(x)]
  if (length(x) == 0) return(rep(NA_real_, length(probs)))
  as.numeric(quantile(x, probs = probs, na.rm = TRUE, names = FALSE))
}

bin_and_plot <- function(df, xvar, yvar, ttl, xlab = NULL, nbins = 20L) {
  z <- as.data.table(df)[is.finite(get(xvar)) & !is.na(get(yvar))]
  if (nrow(z) < MIN_POINTS_PLOT) return(NULL)
  cuts <- unique(safe_quantile(z[[xvar]], seq(0, 1, length.out = nbins + 1)))
  cuts <- cuts[is.finite(cuts)]
  if (length(cuts) < 5L) return(NULL)
  z[, bin := cut(get(xvar), breaks = cuts, include.lowest = TRUE, labels = FALSE)]
  b <- z[!is.na(bin), .(
    x_mean = mean(get(xvar), na.rm = TRUE),
    y_mean = mean(get(yvar), na.rm = TRUE),
    n = .N
  ), by = bin][order(x_mean)]
  ggplot(b, aes(x = x_mean, y = y_mean)) +
    geom_point(aes(size = n), alpha = 0.85) +
    geom_line() +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(title = ttl, x = ifelse(is.null(xlab), xvar, xlab), y = paste0("Pr(", yvar, ")")) +
    theme_minimal(base_size = 12)
}

save_plot <- function(g, path, width = 8.5, height = 5.5) {
  if (!is.null(g)) ggsave(path, g, width = width, height = height, dpi = 300)
}

safe_uniqueN <- function(x) uniqueN(x[!is.na(x) & x != ""])

cumulative_distinct <- function(x) {
  seen <- new.env(hash = TRUE, parent = emptyenv())
  out <- integer(length(x))
  n <- 0L
  for (i in seq_along(x)) {
    val <- x[[i]]
    if (is.na(val) || identical(val, "")) {
      out[[i]] <- n
    } else {
      key <- as.character(val)
      if (!exists(key, envir = seen, inherits = FALSE)) {
        assign(key, TRUE, envir = seen)
        n <- n + 1L
      }
      out[[i]] <- n
    }
  }
  out
}

loo_from_group <- function(sum_x, n, own_x) {
  out <- rep(NA_real_, length(sum_x))
  ok <- !is.na(sum_x) & !is.na(n) & n > 1L & !is.na(own_x)
  out[ok] <- (sum_x[ok] - own_x[ok]) / (n[ok] - 1)
  out
}

first_non_missing <- function(dt, cols) {
  hit <- cols[cols %in% names(dt)]
  if (length(hit) == 0) return(NULL)
  hit[1]
}

write_md <- function(path, lines) writeLines(lines, path)

# =========================================================
# Inspect schema
# =========================================================
ts_msg("Opening dataset:", INPUT_DIR)
ds <- open_dataset(INPUT_DIR, format = "parquet")
ds_names <- names(ds)

schema_fields <- ds$schema$fields
safe_field_type <- function(f) {
  tryCatch({
    if (!is.null(f$type$ToString)) {
      f$type$ToString()
    } else {
      paste(capture.output(print(f$type)), collapse = " ")
    }
  }, error = function(e) {
    paste(capture.output(print(f$type)), collapse = " ")
  })
}

schema_df <- data.frame(
  variable = vapply(schema_fields, function(f) f$name, character(1)),
  type = vapply(schema_fields, safe_field_type, character(1)),
  stringsAsFactors = FALSE
)
write_csv(schema_df, file.path(audit_dir, "schema_types.csv"))

required_core <- c("user_id", "year", "n_patents", PARENT_VAR, RCID_VAR, CITY_VAR, METRO_VAR,
                   STATE_VAR, REGION_VAR, COUNTRY_VAR)
missing_core <- setdiff(required_core, ds_names)
if (length(missing_core) > 0) stop(paste("Missing required columns:", paste(missing_core, collapse = ", ")))

selected_cols <- unique(c(
  "user_id", "year",
  "n_patents", "n_applications", "n_first_inventor", "n_us_citations", "user_linkedin_patent_count",
  "career_modal_section", "career_modal_class", "modal_section_y", "modal_class_y",
  PARENT_VAR, "last_parent_rcid", RCID_VAR, "last_rcid", "n_positions", "n_unique_companies",
  "au_prestige", "au_numconnections", "au_profile_title", "au_profile_summary",
  COMP_CANDIDATES,
  SENIORITY_CANDIDATES,
  CITY_VAR, "last_city", METRO_VAR, "last_metro_area", STATE_VAR, "last_state",
  COUNTRY_VAR, "last_country", REGION_VAR, "last_region", "first_location_raw", "last_location_raw",
  IMMIG_CANDIDATES,
  "first_university_country", "last_university_country", "edu_first_university_country", "edu_last_university_country",
  "first_university", "last_university", "first_degree", "last_degree", "first_field", "last_field",
  "has_education", "n_educations", "edu_first_university_name", "edu_last_university_name",
  "edu_first_degree", "edu_last_degree", "edu_first_field", "edu_last_field",
  "edu_first_start_year", "edu_last_end_year", "first_startdate_pos", "first_startdate_edu",
  "au_f_prob", "au_m_prob", "au_white_prob", "au_black_prob", "au_api_prob", "au_hispanic_prob",
  "au_native_prob", "au_multiple_prob", "au_sex_predicted", "au_ethnicity_predicted"
))
selected_cols <- selected_cols[selected_cols %in% ds_names]

ts_msg("Collecting analysis columns:", paste(selected_cols, collapse = ", "))
df <- ds %>%
  filter(year >= YEAR_MIN, year <= YEAR_MAX) %>%
  select(all_of(selected_cols)) %>%
  collect() %>%
  as.data.table()

# =========================================================
# Clean types
# =========================================================
char_cols <- names(df)[vapply(df, inherits, logical(1), what = c("character", "factor"))]
for (cc in char_cols) set(df, j = cc, value = nz_trim(df[[cc]]))

id_num_cols <- intersect(c("user_id", "year", "n_patents", "n_applications", "n_first_inventor", "n_us_citations",
                           "user_linkedin_patent_count", "n_positions", "n_unique_companies", "au_prestige",
                           "au_numconnections", "avg_salary", "min_salary", "max_salary", "avg_total_comp",
                           "min_total_comp", "max_total_comp", "avg_seniority", "min_seniority", "max_seniority",
                           "n_educations", "edu_first_start_year", "edu_last_end_year"), names(df))
for (cc in setdiff(id_num_cols, c("user_id"))) set(df, j = cc, value = num_or_na(df[[cc]]))
df[, user_id := as.character(user_id)]
df[, year := as.integer(year)]

if (US_ONLY && COUNTRY_VAR %in% names(df)) {
  ts_msg("Applying US-only filter using", COUNTRY_VAR)
  df <- df[!is.na(get(COUNTRY_VAR)) & get(COUNTRY_VAR) == US_COUNTRY]
}

if (!"n_patents" %in% names(df)) stop("n_patents missing after collection")
df[is.na(n_patents), n_patents := 0]
if ("n_applications" %in% names(df)) df[is.na(n_applications), n_applications := 0]
if ("n_first_inventor" %in% names(df)) df[is.na(n_first_inventor), n_first_inventor := 0]
if ("n_us_citations" %in% names(df)) df[is.na(n_us_citations), n_us_citations := 0]

# =========================================================
# Construct core personal variables
# =========================================================
imm_var <- first_non_missing(df, IMMIG_CANDIDATES)
if (!is.null(imm_var)) {
  df[, imm := as.integer(get(imm_var))]
} else {
  df[, imm := NA_integer_]
}

# education-country based origin proxy
if ("first_university_country" %in% names(df)) {
  df[, origin_country_proxy := fifelse(!is.na(first_university_country) & first_university_country != "", first_university_country,
                                       fifelse(!is.na(edu_first_university_country) & edu_first_university_country != "", edu_first_university_country, NA_character_))]
} else if ("edu_first_university_country" %in% names(df)) {
  df[, origin_country_proxy := edu_first_university_country]
} else {
  df[, origin_country_proxy := NA_character_]
}

# tenure proxy
if (all(c("first_startdate_edu", "first_startdate_pos") %in% names(df))) {
  df[, edu_year := num_or_na(substr(as.character(first_startdate_edu), 1, 4))]
  df[, pos_year := num_or_na(substr(as.character(first_startdate_pos), 1, 4))]
} else {
  if ("edu_first_start_year" %in% names(df)) df[, edu_year := num_or_na(edu_first_start_year)] else df[, edu_year := NA_real_]
  if ("first_startdate_pos" %in% names(df)) df[, pos_year := num_or_na(substr(as.character(first_startdate_pos), 1, 4))] else df[, pos_year := NA_real_]
}

df[, first_obs_year := min(year, na.rm = TRUE), by = user_id]
df[, first_patent_year := suppressWarnings(min(year[n_patents > 0], na.rm = TRUE)), by = user_id]
df[!is.finite(first_patent_year), first_patent_year := NA_real_]

df[, tenure_main := year - edu_year + 3]
df[is.na(tenure_main) | tenure_main < 0 | tenure_main > 60, tenure_main := year - pos_year]
df[tenure_main < 0 | tenure_main > 60, tenure_main := year - first_obs_year]
df[tenure_main < 0 | tenure_main > 60, tenure_main := NA_real_]

df[, patenting := as.integer(n_patents > 0)]
setorder(df, user_id, year)

# =========================================================
# Schema audit on analysis data
# =========================================================
audit_stats <- rbindlist(lapply(names(df), function(v) {
  x <- df[[v]]
  is_num <- is.numeric(x) || is.integer(x)
  data.table(
    variable = v,
    missing_rate = mean(is.na(x)),
    n_distinct = if (length(x) <= 5e6) safe_uniqueN(as.character(x)) else NA_integer_,
    min_value = if (is_num) suppressWarnings(min(x, na.rm = TRUE)) else NA_real_,
    max_value = if (is_num) suppressWarnings(max(x, na.rm = TRUE)) else NA_real_
  )
}), fill = TRUE)

schema_audit <- merge(as.data.table(schema_df), audit_stats, by = "variable", all.x = TRUE)
write_csv(as_tibble(schema_audit), file.path(audit_dir, "analysis_schema_audit.csv"))

support_vars <- intersect(c(imm_var, SENIORITY_CANDIDATES, "au_sex_predicted", "au_ethnicity_predicted",
                            "career_modal_section", "career_modal_class", "first_field", "first_university_country"), names(df))
support_list <- lapply(support_vars, function(v) df[, .N, by = get(v)][order(-N)][1:min(.N, 50)])
if (length(support_list) > 0) {
  support_tbl <- rbindlist(Map(function(v, tab) {
    tab2 <- copy(tab)
    setnames(tab2, "get", "value")
    tab2[, variable := v][]
  }, support_vars, support_list), fill = TRUE)
  write_csv(as_tibble(support_tbl), file.path(audit_dir, "support_tables_key_discrete.csv"))
}

# =========================================================
# Mobility panel
# =========================================================
dt <- copy(df)
setorder(dt, user_id, year)
dt[, lag_year := shift(year), by = user_id]
dt[, is_consecutive := as.integer(!is.na(lag_year) & year - lag_year == 1L)]

lag_map <- list(
  parent_id = PARENT_VAR,
  rcid_id   = RCID_VAR,
  city_id   = CITY_VAR,
  metro_id  = METRO_VAR,
  state_id  = STATE_VAR,
  region_id = REGION_VAR
)

for (nm in names(lag_map)) {
  vv <- lag_map[[nm]]
  dt[, (nm) := get(vv)]
  dt[, paste0(nm, "_lag") := shift(get(nm)), by = user_id]
}

personal_lag_vars <- intersect(c("n_patents", "n_applications", "n_first_inventor", "n_us_citations",
                                 "user_linkedin_patent_count", "avg_salary", "avg_total_comp", "avg_seniority",
                                 "min_seniority", "max_seniority", "tenure_main", "patenting", "imm"), names(dt))
for (vv in personal_lag_vars) dt[, paste0(vv, "_lag") := shift(get(vv)), by = user_id]

mk_change <- function(cur, lag) as.integer(!is.na(cur) & !is.na(lag) & cur != lag)

dt[, leave_parent_firm_lastobs := mk_change(parent_id, parent_id_lag)]
dt[, leave_rcid_lastobs        := mk_change(rcid_id, rcid_id_lag)]
dt[, leave_city_lastobs        := mk_change(city_id, city_id_lag)]
dt[, leave_metro_lastobs       := mk_change(metro_id, metro_id_lag)]
dt[, leave_state_lastobs       := mk_change(state_id, state_id_lag)]

for (v in c("leave_parent_firm", "leave_rcid", "leave_city", "leave_metro", "leave_state")) {
  dt[, paste0(v, "_consec") := as.integer(is_consecutive == 1L & get(paste0(v, "_lastobs")) == 1L)]
}

dt[, firm_only_move_consec := as.integer(leave_parent_firm_consec == 1L & leave_city_consec == 0L)]
dt[, city_only_move_consec := as.integer(leave_parent_firm_consec == 0L & leave_city_consec == 1L)]
dt[, metro_only_move_consec := as.integer(leave_parent_firm_consec == 0L & leave_metro_consec == 1L)]
dt[, joint_firm_city_move_consec := as.integer(leave_parent_firm_consec == 1L & leave_city_consec == 1L)]
dt[, joint_firm_metro_move_consec := as.integer(leave_parent_firm_consec == 1L & leave_metro_consec == 1L)]

dt[, firm_only_move_lastobs := as.integer(leave_parent_firm_lastobs == 1L & leave_city_lastobs == 0L)]
dt[, city_only_move_lastobs := as.integer(leave_parent_firm_lastobs == 0L & leave_city_lastobs == 1L)]
dt[, metro_only_move_lastobs := as.integer(leave_parent_firm_lastobs == 0L & leave_metro_lastobs == 1L)]
dt[, joint_firm_city_move_lastobs := as.integer(leave_parent_firm_lastobs == 1L & leave_city_lastobs == 1L)]
dt[, joint_firm_metro_move_lastobs := as.integer(leave_parent_firm_lastobs == 1L & leave_metro_lastobs == 1L)]

# spell indicators
for (nm in c("parent_id", "city_id", "metro_id")) {
  spell_var <- paste0(nm, "_spell")
  dt[, (spell_var) := rleid(get(nm)), by = user_id]
}

# exact cumulative counts
for (nm in c("parent_id", "city_id", "metro_id")) {
  outv <- paste0("cum_unique_", sub("_id$", "", nm))
  dt[, (outv) := cumulative_distinct(get(nm)), by = user_id]
}

# top inventor groups
inv_tot <- dt[, .(
  total_patents = sum(n_patents, na.rm = TRUE),
  total_citations = if ("n_us_citations" %in% names(dt)) sum(n_us_citations, na.rm = TRUE) else NA_real_,
  obs_n = .N
), by = user_id]
for (q in TOP_THRESHOLDS) {
  thr <- quantile(inv_tot$total_patents, probs = q, na.rm = TRUE)
  nm <- paste0("top_", gsub("\\.", "", format(q * 100, trim = TRUE)), "_patents")
  inv_tot[, (nm) := as.integer(total_patents >= thr)]
}
dt <- merge(dt, inv_tot, by = "user_id", all.x = TRUE)

# =========================================================
# Environment tables
# =========================================================
# Parent-year and rcid-year organization measures

group_env <- function(DT, idvar, prefix) {
  if (!idvar %in% names(DT)) return(NULL)
  out <- DT[!is.na(get(idvar)) & get(idvar) != "", .(
    n_inventors = .N,
    n_users = uniqueN(user_id),
    patents_sum = sum(n_patents, na.rm = TRUE),
    patents_mean = mean(n_patents, na.rm = TRUE),
    citations_sum = if ("n_us_citations" %in% names(DT)) sum(n_us_citations, na.rm = TRUE) else NA_real_,
    citations_mean = if ("n_us_citations" %in% names(DT)) mean(n_us_citations, na.rm = TRUE) else NA_real_,
    firstinv_mean = if ("n_first_inventor" %in% names(DT)) mean(n_first_inventor, na.rm = TRUE) else NA_real_,
    applications_mean = if ("n_applications" %in% names(DT)) mean(n_applications, na.rm = TRUE) else NA_real_,
    immig_share = safe_share(imm),
    avg_salary_env = if ("avg_salary" %in% names(DT)) mean(avg_salary, na.rm = TRUE) else NA_real_,
    avg_total_comp_env = if ("avg_total_comp" %in% names(DT)) mean(avg_total_comp, na.rm = TRUE) else NA_real_,
    avg_seniority_env = if ("avg_seniority" %in% names(DT)) mean(avg_seniority, na.rm = TRUE) else NA_real_,
    field_div = if ("career_modal_section" %in% names(DT)) hhi_div(career_modal_section) else NA_real_,
    class_div = if ("career_modal_class" %in% names(DT)) hhi_div(career_modal_class) else NA_real_,
    origin_div = if ("origin_country_proxy" %in% names(DT)) hhi_div(origin_country_proxy) else NA_real_,
    edu_country_div = if ("first_university_country" %in% names(DT)) hhi_div(first_university_country) else NA_real_
  ), by = c(idvar, "year")]
  setnames(out, old = c(idvar, setdiff(names(out), c(idvar, "year"))),
           new = c(idvar, paste0(prefix, "_", setdiff(names(out), c(idvar, "year")))))
  out
}

parent_env <- group_env(dt, "parent_id", "parent")
rcid_env   <- group_env(dt, "rcid_id", "rcid")
city_env   <- group_env(dt, "city_id", "city")
metro_env  <- group_env(dt, "metro_id", "metro")
state_env  <- group_env(dt, "state_id", "state")

if (COMPUTE_LOO) {
  # parent loo measures
  if (!is.null(parent_env)) {
    dt <- merge(dt, parent_env[, .(parent_id, year, parent_n_inventors, parent_patents_sum, parent_citations_sum)],
                by = c("parent_id", "year"), all.x = TRUE)
    dt[, parent_loo_patents := loo_from_group(parent_patents_sum, parent_n_inventors, n_patents)]
    if ("n_us_citations" %in% names(dt)) dt[, parent_loo_citations := loo_from_group(parent_citations_sum, parent_n_inventors, n_us_citations)]
  }
  if (!is.null(city_env)) {
    dt <- merge(dt, city_env[, .(city_id, year, city_n_inventors, city_patents_sum, city_citations_sum)],
                by = c("city_id", "year"), all.x = TRUE)
    dt[, city_loo_patents := loo_from_group(city_patents_sum, city_n_inventors, n_patents)]
    if ("n_us_citations" %in% names(dt)) dt[, city_loo_citations := loo_from_group(city_citations_sum, city_n_inventors, n_us_citations)]
  }
  if (!is.null(metro_env)) {
    dt <- merge(dt, metro_env[, .(metro_id, year, metro_n_inventors, metro_patents_sum, metro_citations_sum)],
                by = c("metro_id", "year"), all.x = TRUE)
    dt[, metro_loo_patents := loo_from_group(metro_patents_sum, metro_n_inventors, n_patents)]
    if ("n_us_citations" %in% names(dt)) dt[, metro_loo_citations := loo_from_group(metro_citations_sum, metro_n_inventors, n_us_citations)]
  }
}

# =========================================================
# Attach lagged origin characteristics (predictors of moving)
# =========================================================
ana <- copy(dt[!is.na(parent_id_lag) | !is.na(city_id_lag) | !is.na(metro_id_lag)])
ana[, year_lag := year - 1L]

merge_origin_env <- function(base, env, lag_id, prefix) {
  if (is.null(env)) return(base)
  keep <- names(env)
  val_cols <- setdiff(keep, c(lag_id, "year"))
  env2 <- copy(env)
  setnames(env2, lag_id, paste0(lag_id, "_lag_join"))
  setnames(env2, val_cols, paste0("origin_", val_cols))
  merge(base, env2, by.x = c(paste0(lag_id, "_lag"), "year_lag"), by.y = c(paste0(lag_id, "_lag_join"), "year"), all.x = TRUE)
}

ana <- merge_origin_env(ana, parent_env, "parent_id", "parent")
ana <- merge_origin_env(ana, rcid_env, "rcid_id", "rcid")
ana <- merge_origin_env(ana, city_env, "city_id", "city")
ana <- merge_origin_env(ana, metro_env, "metro_id", "metro")
ana <- merge_origin_env(ana, state_env, "state_id", "state")

# destination env current year
merge_dest_env <- function(base, env, idvar) {
  if (is.null(env)) return(base)
  val_cols <- setdiff(names(env), c(idvar, "year"))
  env2 <- copy(env)
  setnames(env2, val_cols, paste0("dest_", val_cols))
  merge(base, env2, by = c(idvar, "year"), all.x = TRUE)
}

ana <- merge_dest_env(ana, parent_env, "parent_id")
ana <- merge_dest_env(ana, rcid_env, "rcid_id")
ana <- merge_dest_env(ana, city_env, "city_id")
ana <- merge_dest_env(ana, metro_env, "metro_id")
ana <- merge_dest_env(ana, state_env, "state_id")

# Destination-origin deltas for interpretable margins
calc_delta <- function(DT, dest, origin, out) {
  if (all(c(dest, origin) %in% names(DT))) DT[, (out) := get(dest) - get(origin)]
}

calc_delta(ana, "dest_parent_n_inventors", "origin_parent_n_inventors", "delta_parent_size")
calc_delta(ana, "dest_parent_patents_mean", "origin_parent_patents_mean", "delta_parent_patents")
calc_delta(ana, "dest_parent_citations_mean", "origin_parent_citations_mean", "delta_parent_citations")
calc_delta(ana, "dest_city_n_inventors", "origin_city_n_inventors", "delta_city_size")
calc_delta(ana, "dest_metro_patents_mean", "origin_metro_patents_mean", "delta_metro_patents")
calc_delta(ana, "avg_salary", "avg_salary_lag", "delta_salary")
calc_delta(ana, "avg_total_comp", "avg_total_comp_lag", "delta_total_comp")
calc_delta(ana, "avg_seniority", "avg_seniority_lag", "delta_seniority")
calc_delta(ana, "dest_parent_immig_share", "origin_parent_immig_share", "delta_parent_immig_share")
calc_delta(ana, "dest_city_immig_share", "origin_city_immig_share", "delta_city_immig_share")
calc_delta(ana, "dest_parent_origin_div", "origin_parent_origin_div", "delta_parent_diversity")
calc_delta(ana, "dest_city_origin_div", "origin_city_origin_div", "delta_city_diversity")
calc_delta(ana, "dest_parent_avg_seniority_env", "origin_parent_avg_seniority_env", "delta_parent_env_seniority")
calc_delta(ana, "dest_parent_avg_salary_env", "origin_parent_avg_salary_env", "delta_parent_env_salary")

# =========================================================
# Save mobility panel (compact)
# =========================================================
panel_keep <- unique(c(
  "user_id", "year", "tenure_main", "imm", "avg_seniority", "avg_salary", "avg_total_comp",
  "n_patents", "n_us_citations", "patenting",
  "parent_id", "parent_id_lag", "rcid_id", "rcid_id_lag", "city_id", "city_id_lag", "metro_id", "metro_id_lag",
  grep("^leave_", names(dt), value = TRUE),
  grep("_move_", names(dt), value = TRUE),
  grep("^cum_unique_", names(dt), value = TRUE),
  grep("^top_", names(dt), value = TRUE)
))
write_csv(as_tibble(dt[, ..panel_keep]), file.path(panel_dir, "mobility_panel_compact.csv"))

# =========================================================
# Tables: move types and summary
# =========================================================
move_type_tbl <- rbindlist(list(
  dt[, .(move_type = "firm_only_consec", share = mean(firm_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "city_only_consec", share = mean(city_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "metro_only_consec", share = mean(metro_only_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_firm_city_consec", share = mean(joint_firm_city_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_firm_metro_consec", share = mean(joint_firm_metro_move_consec, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "firm_only_lastobs", share = mean(firm_only_move_lastobs, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "city_only_lastobs", share = mean(city_only_move_lastobs, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "metro_only_lastobs", share = mean(metro_only_move_lastobs, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_firm_city_lastobs", share = mean(joint_firm_city_move_lastobs, na.rm = TRUE), n = .N)],
  dt[, .(move_type = "joint_firm_metro_lastobs", share = mean(joint_firm_metro_move_lastobs, na.rm = TRUE), n = .N)]
), fill = TRUE)
write_csv(as_tibble(move_type_tbl), file.path(panel_dir, "move_type_shares.csv"))

# predictors of moving tables
predictor_vars <- intersect(c(
  "origin_parent_n_inventors", "origin_parent_patents_mean", "origin_parent_citations_mean",
  "origin_parent_avg_salary_env", "origin_parent_avg_total_comp_env", "origin_parent_avg_seniority_env",
  "origin_parent_immig_share", "origin_parent_origin_div", "origin_parent_field_div",
  "origin_city_n_inventors", "origin_city_patents_mean", "origin_city_citations_mean",
  "origin_city_immig_share", "origin_city_origin_div", "origin_metro_n_inventors", "origin_metro_patents_mean",
  "avg_salary_lag", "avg_total_comp_lag", "avg_seniority_lag", "n_patents_lag", "n_us_citations_lag", "tenure_main_lag"
), names(ana))

pred_summaries <- rbindlist(lapply(predictor_vars, function(v) {
  data.table(
    variable = v,
    move_rate_parent_consec = ana[is.finite(get(v)), mean(leave_parent_firm_consec, na.rm = TRUE)],
    move_rate_city_consec   = ana[is.finite(get(v)), mean(leave_city_consec, na.rm = TRUE)],
    p25 = ana[is.finite(get(v)), quantile(get(v), 0.25, na.rm = TRUE)],
    p50 = ana[is.finite(get(v)), quantile(get(v), 0.50, na.rm = TRUE)],
    p75 = ana[is.finite(get(v)), quantile(get(v), 0.75, na.rm = TRUE)],
    n = ana[is.finite(get(v)), .N]
  )
}), fill = TRUE)
write_csv(as_tibble(pred_summaries), file.path(panel_dir, "predictor_summaries.csv"))

# =========================================================
# Origin-destination descriptive tables
# =========================================================
delta_vars <- intersect(c(
  "delta_parent_size", "delta_parent_patents", "delta_parent_citations",
  "delta_city_size", "delta_metro_patents", "delta_salary", "delta_total_comp",
  "delta_seniority", "delta_parent_immig_share", "delta_city_immig_share",
  "delta_parent_diversity", "delta_city_diversity", "delta_parent_env_seniority", "delta_parent_env_salary"
), names(ana))

move_masks <- list(
  firm_only = ana$firm_only_move_consec == 1L,
  city_only = ana$city_only_move_consec == 1L,
  metro_only = ana$metro_only_move_consec == 1L,
  joint_firm_city = ana$joint_firm_city_move_consec == 1L,
  joint_firm_metro = ana$joint_firm_metro_move_consec == 1L
)

od_tbl <- rbindlist(lapply(names(move_masks), function(mn) {
  mask <- move_masks[[mn]]
  rbindlist(lapply(delta_vars, function(v) {
    x <- ana[mask & is.finite(get(v)), get(v)]
    data.table(
      move_type = mn,
      variable = v,
      mean = if (length(x)) mean(x, na.rm = TRUE) else NA_real_,
      median = if (length(x)) median(x, na.rm = TRUE) else NA_real_,
      p25 = if (length(x)) quantile(x, 0.25, na.rm = TRUE) else NA_real_,
      p75 = if (length(x)) quantile(x, 0.75, na.rm = TRUE) else NA_real_,
      n = length(x)
    )
  }), fill = TRUE)
}), fill = TRUE)
write_csv(as_tibble(od_tbl), file.path(od_dir, "origin_destination_deltas_by_move_type.csv"))

# =========================================================
# Figures
# =========================================================
plot_pairs <- list(
  list(x = "origin_parent_n_inventors", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged parent size"),
  list(x = "origin_parent_patents_mean", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged parent patents"),
  list(x = "origin_parent_citations_mean", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged parent citations"),
  list(x = "origin_parent_immig_share", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged parent immigrant share"),
  list(x = "origin_parent_origin_div", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged parent origin diversity"),
  list(x = "origin_city_n_inventors", y = "leave_city_consec", ttl = "Pr(city move) vs lagged city size"),
  list(x = "origin_metro_patents_mean", y = "leave_metro_consec", ttl = "Pr(metro move) vs lagged metro patents"),
  list(x = "avg_salary_lag", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged salary"),
  list(x = "avg_total_comp_lag", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged total comp"),
  list(x = "avg_seniority_lag", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged seniority"),
  list(x = "n_patents_lag", y = "leave_parent_firm_consec", ttl = "Pr(parent-firm move) vs lagged patents")
)
for (pp in plot_pairs) {
  if (all(c(pp$x, pp$y) %in% names(ana))) {
    g <- bin_and_plot(ana, pp$x, pp$y, pp$ttl)
    save_plot(g, file.path(fig_dir, paste0("move_vs_origin_", pp$x, "_", pp$y, ".png")))
  }
}

for (dv in delta_vars) {
  g <- ggplot(ana[firm_only_move_consec == 1L & is.finite(get(dv))], aes(x = .data[[dv]])) +
    geom_histogram(bins = 40) +
    labs(title = paste("Destination - origin:", dv, "(firm-only moves)"), x = dv, y = "Count") +
    theme_minimal(base_size = 12)
  save_plot(g, file.path(fig_dir, paste0("move_destination_minus_origin_", dv, "_firm_only.png")))
}

# Decomposition figure
move_type_plot <- ggplot(move_type_tbl[grepl("consec", move_type)], aes(x = reorder(move_type, share), y = share)) +
  geom_col() + coord_flip() +
  scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
  labs(title = "Shares of mobility event types (consecutive years)", x = NULL, y = "Share") +
  theme_minimal(base_size = 12)
save_plot(move_type_plot, file.path(fig_dir, "move_type_decomposition_consecutive.png"), 9, 6)

# =========================================================
# Event studies
# =========================================================
if (RUN_EVENT_STUDIES) {
  build_event <- function(DT, event_var, out_stub) {
    first_ev <- DT[get(event_var) == 1L, .(event_year = min(year, na.rm = TRUE)), by = user_id]
    if (nrow(first_ev) == 0) return(NULL)
    es <- merge(DT, first_ev, by = "user_id", all.x = FALSE)
    es[, event_time := year - event_year]
    es <- es[event_time >= -EVENT_WINDOW & event_time <= EVENT_WINDOW]
    metrics <- intersect(c("n_patents", "n_us_citations", "avg_salary", "avg_total_comp", "avg_seniority",
                           "parent_loo_patents", "metro_loo_patents", "city_loo_patents"), names(es))
    tab <- rbindlist(lapply(metrics, function(m) {
      es[, .(
        metric = m,
        mean = mean(get(m), na.rm = TRUE),
        n = .N
      ), by = event_time]
    }), fill = TRUE)
    write_csv(as_tibble(tab), file.path(event_dir, paste0("eventstudy_", out_stub, ".csv")))
    for (m in unique(tab$metric)) {
      sub <- tab[metric == m]
      g <- ggplot(sub, aes(x = event_time, y = mean)) +
        geom_line() + geom_point() + geom_vline(xintercept = 0, linetype = 2) +
        labs(title = paste(m, "around", out_stub), x = "Event time", y = m) +
        theme_minimal(base_size = 12)
      save_plot(g, file.path(fig_dir, paste0("move_eventstudy_", out_stub, "_", m, ".png")))
    }
    tab
  }
  build_event(dt, "leave_parent_firm_consec", "first_parent_move")
  build_event(dt, "leave_city_consec", "first_city_move")
  build_event(dt, "leave_metro_consec", "first_metro_move")
  build_event(dt, "joint_firm_city_move_consec", "first_joint_firm_city_move")
}

# =========================================================
# Heterogeneity
# =========================================================
if (RUN_HETEROGENEITY) {
  # seniority bands
  if ("avg_seniority" %in% names(dt)) {
    qs <- safe_quantile(dt$avg_seniority, c(1/3, 2/3))
    dt[, seniority_band := fifelse(avg_seniority <= qs[1], "low",
                            fifelse(avg_seniority <= qs[2], "mid", "high"))]
  }
  if (!is.null(imm_var)) dt[, immigrant_group := fifelse(imm == 1L, "immigrant", fifelse(imm == 0L, "non_immigrant", NA_character_))]
  dt[, productivity_band := fifelse(total_patents >= quantile(total_patents, 0.90, na.rm = TRUE), "top_decile",
                             fifelse(total_patents >= quantile(total_patents, 0.75, na.rm = TRUE), "top_quartile_to_decile", "bottom_75"))]
  dt[, career_stage := fifelse(!is.na(tenure_main) & tenure_main <= 10, "early",
                        fifelse(!is.na(tenure_main) & tenure_main <= 20, "mid", "late"))]

  het_groups <- intersect(c("immigrant_group", "seniority_band", "productivity_band", "career_stage",
                            "career_modal_section", "career_modal_class", "au_sex_predicted", "au_ethnicity_predicted"), names(dt))
  het_tbls <- lapply(het_groups, function(gv) {
    dt[!is.na(get(gv)), .(
      p_move_parent_consec = mean(leave_parent_firm_consec, na.rm = TRUE),
      p_move_city_consec = mean(leave_city_consec, na.rm = TRUE),
      p_joint_move_consec = mean(joint_firm_city_move_consec, na.rm = TRUE),
      mean_patents = mean(n_patents, na.rm = TRUE),
      mean_salary = if ("avg_salary" %in% names(dt)) mean(avg_salary, na.rm = TRUE) else NA_real_,
      mean_total_comp = if ("avg_total_comp" %in% names(dt)) mean(avg_total_comp, na.rm = TRUE) else NA_real_,
      mean_seniority = if ("avg_seniority" %in% names(dt)) mean(avg_seniority, na.rm = TRUE) else NA_real_,
      n = .N
    ), by = gv]
  })
  het_tbl <- rbindlist(Map(function(gv, tab) { tab[, grouping_var := gv][] }, het_groups, het_tbls), fill = TRUE)
  write_csv(as_tibble(het_tbl), file.path(het_dir, "heterogeneity_move_rates.csv"))

# destination upgrades by selected groupings
upgrade_groups <- character(0)
ana_up <- copy(ana)

# immigrant group
if (!is.null(imm_var) && imm_var %in% names(ana_up)) {
  ana_up[, immigrant_group := fifelse(get(imm_var) == 1L, "immigrant",
                               fifelse(get(imm_var) == 0L, "non_immigrant", NA_character_))]
  upgrade_groups <- c(upgrade_groups, "immigrant_group")
}

# productivity band
if ("total_patents" %in% names(ana_up)) {
  p90_tp <- quantile(dt$total_patents, 0.90, na.rm = TRUE)
  ana_up[, productivity_band := fifelse(total_patents >= p90_tp, "top_decile", "bottom_90")]
  upgrade_groups <- c(upgrade_groups, "productivity_band")
}

# seniority band
if ("avg_seniority" %in% names(ana_up)) {
  qs <- quantile(dt$avg_seniority, probs = c(1/3, 2/3), na.rm = TRUE)
  ana_up[, seniority_band := fifelse(avg_seniority <= qs[1], "low",
                              fifelse(avg_seniority <= qs[2], "mid", "high"))]
  upgrade_groups <- c(upgrade_groups, "seniority_band")
}

if (length(upgrade_groups) > 0) {
  up_tbl <- rbindlist(lapply(upgrade_groups, function(gv) {
    ana_up[firm_only_move_consec == 1L & !is.na(get(gv)), .(
      mean_delta_parent_patents = mean(delta_parent_patents, na.rm = TRUE),
      mean_delta_metro_patents  = mean(delta_metro_patents, na.rm = TRUE),
      mean_delta_salary         = mean(delta_salary, na.rm = TRUE),
      mean_delta_total_comp     = mean(delta_total_comp, na.rm = TRUE),
      mean_delta_seniority      = mean(delta_seniority, na.rm = TRUE),
      n = .N
    ), by = gv][, grouping_var := gv][]
  }), fill = TRUE)

  write_csv(
    as_tibble(up_tbl),
    file.path(het_dir, "heterogeneity_destination_upgrades_firm_only.csv")
  )
}

for (gv in intersect(c("immigrant_group", "seniority_band", "productivity_band"), names(ana_up))) {
  sub <- ana_up[!is.na(get(gv)), .(
    move_rate = mean(leave_parent_firm_consec, na.rm = TRUE),
    n = .N
  ), by = gv]

  g <- ggplot(sub, aes(x = .data[[gv]], y = move_rate)) +
    geom_col() +
    scale_y_continuous(labels = percent_format(accuracy = 0.1)) +
    labs(title = paste("Parent-firm move rate by", gv), x = gv, y = "Move rate") +
    theme_minimal(base_size = 12)

  save_plot(g, file.path(fig_dir, paste0("move_heterogeneity_", gv, ".png")))
}
}
# =========================================================
# Memo and output index
# =========================================================
interesting_lines <- c(
  "This script rewrites the original churn design into a broader mobility panel with separate parent-firm, rcid, city, metro, and state move definitions.",
  "Origin predictors use lagged origin characteristics. Destination-origin comparisons use current destination minus lagged origin characteristics.",
  "The script computes separate firm-only, city-only, metro-only, and joint move indicators, plus consecutive-year and last-observed versions.",
  "Environment measures include parent/year, rcid/year, city/year, metro/year, and state/year size, productivity, compensation, seniority, immigrant share, and diversity proxies.",
  "Leave-one-out parent/city/metro patent and citation intensity are included where feasible to reduce mechanical correlation.",
  "Event studies around first parent, city, metro, and joint moves are descriptive only and should not be given causal language.",
  "Compensation and seniority should be interpreted cautiously if missingness is high; check analysis_schema_audit.csv first.",
  "Predicted gender and ethnicity variables are included only as labeled predicted traits, not as validated demographic truth."
)
write_md(file.path(OUT_DIR, "move_motives_memo.md"), c(
  "# Move motives descriptive pipeline memo",
  "",
  "## Audit of original code",
  "- The original script defined firm churn from changes in `first_parent_rcid`, then related `Pr(leave_firm)` to lagged parent-firm size, immigrant share, average patents, and diversity proxies.",
  "- That is a useful starting point, but it mainly studies origin-firm churn rather than broader reasons for moving.",
  "- It did not separate firm-only from geography moves, did not compare destination to origin, and did not exploit compensation, seniority, rcid-level, or metro-level margins.",
  "",
  "## What changed",
  paste0("- ", interesting_lines),
  "",
  "## Interpretation discipline",
  "- Treat all outputs as descriptive patterns, not causal effects.",
  "- For predictors of moving, read only lagged origin-characteristic relationships.",
  "- For sorting/upgrading, read destination-minus-origin comparisons.",
  "- Check selection/composition carefully across move types and heterogeneity groups."
))

output_files <- list.files(OUT_DIR, recursive = TRUE)
write_md(file.path(OUT_DIR, "output_index.md"), c("# Output index", "", paste0("- `", output_files, "`")))

ts_msg("DONE. Outputs in:", OUT_DIR)
