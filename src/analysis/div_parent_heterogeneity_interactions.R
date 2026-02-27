##############################################################################
# HETEROGENEOUS EFFECTS (SEPARATE REGRESSIONS — NOT ALL TOGETHER):
#   (A) div × immigrant
#   (B) div × origin_group (topK origins + US + OTHER)
#   (C) div × same-origin share in parent (time-invariant, last10y)
#   (D) div × origin_bin5 (IMMIGRANTS ONLY; 5 frequency bins)
#
# PARENT ONLY — time-invariant diversity (last10y), time-invariant controls (size/imm)
# NO RDS
##############################################################################

# =========================
# 0) Libraries
# =========================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE, showWarnings = FALSE)
.libPaths(c(user_lib, .libPaths()))
pkgs <- c("arrow","dplyr","readr","stringr","data.table","fixest","tidyr","tibble")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
suppressPackageStartupMessages({
  library(arrow); library(dplyr); library(readr); library(stringr)
  library(data.table); library(fixest); library(tidyr); library(tibble)
})
set.seed(123)
data.table::setDTthreads(max(1L, parallel::detectCores() - 1L))

# =========================
# 1) Paths + knobs
# =========================
INPUT   <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_parent_heterogeneity"

DIRS <- list(
  tables       = file.path(OUT_DIR, "tables"),
  cluster_meas = file.path(OUT_DIR, "cluster_measures_timeinvariant10y")
)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
PARENT_VAR <- "first_parent_rcid"

YEAR_START <- 2010
YEAR_END   <- 2024

US_COUNTRY <- "United States"

LAST10_N <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END
YEARS_LAST10 <- LAST10_START:LAST10_END

MIN_CLUSTER <- 10
TENURE_MAX  <- 50
TOPK_GROUPS <- 20  # top origins for origin_group interactions

# =========================
# 2) Helpers
# =========================
ts_msg <- function(...) cat(sprintf("[%s]  ", format(Sys.time(), "%H:%M:%S")), ..., "\n")
need_cols <- function(nm, cols) {
  miss <- setdiff(cols, nm)
  if (length(miss) > 0) stop(paste0("[ERROR] Missing required columns: ", paste(miss, collapse = ", ")))
}
sig_stars <- function(p) {
  ifelse(!is.finite(p), "",
         ifelse(p < 0.01, "***",
                ifelse(p < 0.05, "**",
                       ifelse(p < 0.1, "*", ""))))
}
extract_fixest_terms <- function(m, model_tag) {
  ct <- fixest::coeftable(m)
  if (is.null(ct) || nrow(ct) == 0) {
    return(tibble(model=model_tag, term=character(), estimate=numeric(), se=numeric(), stat=numeric(), p=numeric(), stars=character()))
  }
  tibble(
    model    = model_tag,
    term     = rownames(ct),
    estimate = as.numeric(ct[,1]),
    se       = as.numeric(ct[,2]),
    stat     = as.numeric(ct[,3]),
    p        = as.numeric(ct[,4]),
    stars    = sig_stars(as.numeric(ct[,4]))
  )
}

# =========================
# 3) Derived variables
# =========================
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_edu), 1, 4))),
      pos_year = suppressWarnings(as.numeric(substr(as.character(first_startdate_pos), 1, 4))),
      tenure   = year - edu_year + 3,
      tenure   = ifelse(is.na(tenure) | tenure > TENURE_MAX, year - pos_year, tenure),
      tenure   = ifelse(tenure > TENURE_MAX | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    )
}

make_origin_country <- function(df) {
  df %>%
    mutate(
      uni_c  = str_trim(as.character(first_university_country)),
      pos_c  = str_trim(as.character(first_pos_country)),
      uni_ok = !is.na(uni_c) & uni_c != "" & uni_c != "United States",
      pos_ok = !is.na(pos_c) & pos_c != "" & pos_c != "United States",
      origin_country = case_when(
        .data[[IMMIG_VAR]] == 0L ~ "United States",
        .data[[IMMIG_VAR]] == 1L & uni_ok ~ uni_c,
        .data[[IMMIG_VAR]] == 1L & !uni_ok & pos_ok ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# =========================
# 4) Build time-invariant diversity + size/imm controls (last10y)
# =========================
cluster_year_div_stats_dt <- function(df, cluster_var, country_var, div_outname, min_cluster = MIN_CLUSTER) {
  keep <- c("user_id","year",IMMIG_VAR,cluster_var,country_var)
  dt <- as.data.table(df[, keep])
  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "cvar_raw")
  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]
  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year) & !is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]
  comp <- unique(dt[, .(user_id, year, cluster_id, imm, cvar_raw)])

  size_df <- comp[, .(cluster_size = uniqueN(user_id),
                      immig_share = mean(imm, na.rm = TRUE)),
                  by=.(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]

  tmp <- comp[!is.na(cvar_raw) & cvar_raw != "" & cvar_raw != "empty",
              .(n = uniqueN(user_id)), by = .(cluster_id, year, v=cvar_raw)]
  tmp[, tot := sum(n), by=.(cluster_id, year)]
  tmp[, share_sq := (n/tot)^2]
  div_df <- tmp[, .(div = 1 - sum(share_sq)), by=.(cluster_id, year)]
  setnames(div_df, "div", div_outname)

  out <- merge(size_df, div_df, by=c("cluster_id","year"), all.x=TRUE)
  out <- out[is.finite(get(div_outname))]
  as_tibble(out)
}

make_timeinvariant_parent_measures <- function(df_all) {
  df10 <- df_all %>% filter(year %in% YEARS_LAST10)

  origin_y <- cluster_year_div_stats_dt(df10, PARENT_VAR, "origin_country", "origin_div", MIN_CLUSTER)
  job_y    <- cluster_year_div_stats_dt(df10, PARENT_VAR, "first_pos_country", "job_div", MIN_CLUSTER) %>%
    select(cluster_id, year, job_div)
  edu_y    <- cluster_year_div_stats_dt(df10, PARENT_VAR, "first_university_country", "edu_div", MIN_CLUSTER) %>%
    select(cluster_id, year, edu_div)

  stats_y <- origin_y %>%
    left_join(job_y, by=c("cluster_id","year")) %>%
    left_join(edu_y, by=c("cluster_id","year"))
  write_csv(stats_y, file.path(DIRS$cluster_meas, paste0("PARENT__cluster_year__", LAST10_START, "_", LAST10_END, ".csv")))

  stats_c <- stats_y %>%
    group_by(cluster_id) %>%
    summarise(
      origin_div_10y     = mean(origin_div, na.rm=TRUE),
      job_div_10y        = mean(job_div, na.rm=TRUE),
      edu_div_10y        = mean(edu_div, na.rm=TRUE),
      cluster_size_10y   = mean(cluster_size, na.rm=TRUE),
      immig_share_10y    = mean(immig_share, na.rm=TRUE),
      .groups="drop"
    ) %>%
    mutate(immig_share_sq_10y = immig_share_10y^2) %>%
    filter(is.finite(origin_div_10y), is.finite(job_div_10y), is.finite(edu_div_10y),
           is.finite(cluster_size_10y), cluster_size_10y > 0,
           is.finite(immig_share_10y), is.finite(immig_share_sq_10y))

  write_csv(stats_c, file.path(DIRS$cluster_meas, paste0("PARENT__cluster_const__", LAST10_START, "_", LAST10_END, ".csv")))
  stats_c
}

# =========================
# 4b) Same-origin share in parent (time-invariant, last10y)
#     For each (parent, origin_country): average over last10y of:
#       share_{parent,year}(origin) = N_inventors(origin)/N_inventors(total)
# =========================
make_same_origin_share10y <- function(df_all) {
  df10 <- df_all %>% filter(year %in% YEARS_LAST10)

  keep <- c("user_id","year",PARENT_VAR,"origin_country")
  dt <- as.data.table(df10[, keep])
  setnames(dt, PARENT_VAR, "cluster_raw")
  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]
  dt <- unique(dt[!is.na(cluster_id) & cluster_id != "" &
                    !is.na(year) & !is.na(user_id) &
                    !is.na(origin_country) & origin_country != "" &
                    origin_country != "empty",
                  .(user_id, year, cluster_id, origin_country)])

  # totals per cluster-year
  tot <- dt[, .(tot = uniqueN(user_id)), by=.(cluster_id, year)]
  # origin counts per cluster-year
  cnt <- dt[, .(n = uniqueN(user_id)), by=.(cluster_id, year, origin_country)]
  sh  <- merge(cnt, tot, by=c("cluster_id","year"), all.x=TRUE)
  sh[, same_origin_share_y := fifelse(tot > 0, n / tot, NA_real_)]

  # time-invariant: average over years
  sh10 <- sh[, .(same_origin_share_10y = mean(same_origin_share_y, na.rm=TRUE)),
             by=.(cluster_id, origin_country)]
  sh10 <- sh10[is.finite(same_origin_share_10y)]
  as_tibble(sh10)
}

# =========================
# 5) Load Arrow + df
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format="parquet")

need_cols(names(ds), c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR, PARENT_VAR
))

# first_pos_country from FULL panel
ts_msg("Computing first_pos_country from FULL panel...")
ds_pos <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(
    pos_country_y = if_else(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = if_else(pos_country_y == "", NA_character_, pos_country_y)
  ) %>%
  filter(!is.na(user_id), !is.na(year), !is.na(pos_country_y))

ds_first_year <- ds_pos %>% group_by(user_id) %>% summarise(first_pos_year=min(year, na.rm=TRUE), .groups="drop")
first_pos_tbl <- ds_pos %>%
  inner_join(ds_first_year, by="user_id") %>%
  filter(year == first_pos_year) %>%
  select(user_id, first_pos_country = pos_country_y) %>%
  collect() %>%
  mutate(user_id=as.character(user_id), first_pos_country=str_trim(as.character(first_pos_country))) %>%
  distinct(user_id, .keep_all=TRUE)

ts_msg("Collecting 2010–2024...")
df <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(user_id, year, n_patents, first_country,
         first_university_country, first_startdate_edu, first_startdate_pos,
         !!sym(IMMIG_VAR), !!sym(PARENT_VAR)) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]]),
    first_university_country = str_trim(as.character(first_university_country))
  ) %>%
  left_join(first_pos_tbl, by="user_id")

ts_msg("Deriving tenure + origin_country...")
df <- df %>% compute_tenure() %>% filter(!is.na(tenure)) %>% make_origin_country()

# =========================
# 6) origin_group (topK among immigrants, plus US, else OTHER)
# =========================
ts_msg("Building origin_group (topK + US + OTHER) ...")
df <- df %>% filter(!is.na(first_country), first_country == US_COUNTRY)
df10 <- df %>% filter(year %in% YEARS_LAST10)

top_orig <- df10 %>%
  filter(.data[[IMMIG_VAR]] == 1L, !is.na(origin_country), origin_country != "") %>%
  count(origin_country, sort=TRUE) %>%
  slice_head(n = TOPK_GROUPS) %>%
  pull(origin_country)

df <- df %>%
  mutate(
    origin_group = case_when(
      .data[[IMMIG_VAR]] == 0L ~ "United States",
      .data[[IMMIG_VAR]] == 1L & origin_country %in% top_orig ~ origin_country,
      .data[[IMMIG_VAR]] == 1L ~ "OTHER",
      TRUE ~ NA_character_
    ),
    imm = as.integer(.data[[IMMIG_VAR]]),
    origin_group = as.factor(origin_group)
  )

# =========================
# 6b) origin_bin5 (IMMIGRANTS ONLY)
#     5 bins by immigrant-origin frequency ranks in last10y:
#       bin1: rank 1
#       bin2: rank 2-5
#       bin3: rank 6-20
#       bin4: rank 21-100
#       bin5: rank 101+
# =========================
ts_msg("Building origin_bin5 (immigrants only; 5 bins)...")
orig_rank <- df10 %>%
  filter(.data[[IMMIG_VAR]] == 1L, !is.na(origin_country), origin_country != "") %>%
  count(origin_country, sort=TRUE) %>%
  mutate(rank = row_number())

rank_map <- orig_rank %>%
  mutate(origin_bin5 = case_when(
    rank == 1 ~ "BIN_1",
    rank >= 2  & rank <= 5   ~ "BIN_2_5",
    rank >= 6  & rank <= 20  ~ "BIN_6_20",
    rank >= 21 & rank <= 100 ~ "BIN_21_100",
    rank >= 101              ~ "BIN_101P",
    TRUE ~ NA_character_
  )) %>%
  select(origin_country, origin_bin5)

df <- df %>%
  left_join(rank_map, by="origin_country") %>%
  mutate(origin_bin5 = as.factor(origin_bin5))

# =========================
# 7) Build time-invariant parent measures + same-origin-share
# =========================
ts_msg("Building time-invariant parent measures (last10y)...")
meas <- make_timeinvariant_parent_measures(df)

ts_msg("Building same-origin share (parent × origin) time-invariant (last10y)...")
same_share <- make_same_origin_share10y(df)

# =========================
# 8) Run heterogeneity regressions (SEPARATE)
# =========================
run_code <- function(df_all, meas, same_share) {

  dd <- df_all %>%
    mutate(cluster_id = as.character(.data[[PARENT_VAR]])) %>%
    left_join(meas, by="cluster_id") %>%
    # merge same-origin share at (parent, origin_country)
    left_join(same_share, by=c("cluster_id","origin_country")) %>%
    filter(!is.na(cluster_id), cluster_id != "",
           is.finite(cluster_size_10y), cluster_size_10y > 0,
           is.finite(immig_share_10y), is.finite(immig_share_sq_10y),
           is.finite(tenure), is.finite(tenure_sq),
           !is.na(n_patents), n_patents >= 0,
           !is.na(origin_group)) %>%
    mutate(
      user_id   = as.factor(user_id),
      year_fe   = as.factor(year),
      cluster_id= as.factor(cluster_id),
      imm       = as.integer(imm)
    )

  # optional centering for interpretability in C)
  dd <- dd %>%
    mutate(
      same_origin_share_c_10y = ifelse(is.finite(same_origin_share_10y),
                                       same_origin_share_10y - mean(same_origin_share_10y, na.rm=TRUE),
                                       NA_real_)
    )

  SPECS <- list(origin="origin_div_10y", edu="edu_div_10y", job="job_div_10y")

  # Heterogeneity menus (each is run in its own regression)
  HET_SPECS <- list(
    HET_IMM = function(div) {
      paste0(div, " + ", div, ":imm")
    },
    HET_ORIGIN_FINE = function(div) {
      paste0(div, " + ", div, ":origin_group")
    },
    HET_SAME_SHARE = function(div) {
      # requires same_origin_share_c_10y
      paste0(div, " + ", div, ":same_origin_share_c_10y")
    },
    HET_ORIGIN_BIN5_IMM_ONLY = function(div) {
      # immigrants only; uses origin_bin5 (5 bins)
      paste0(div, " + ", div, ":origin_bin5")
    }
  )

  out <- list()

  for (spec_nm in names(SPECS)) {
    divvar <- SPECS[[spec_nm]]
    dspec_base <- dd %>% filter(is.finite(.data[[divvar]]))

    for (het_nm in names(HET_SPECS)) {

      dspec <- dspec_base

      # For BIN5: immigrants only, and need origin_bin5
      if (het_nm == "HET_ORIGIN_BIN5_IMM_ONLY") {
        dspec <- dspec %>%
          filter(imm == 1L, !is.na(origin_bin5))
      }

      # For SAME_SHARE: need share available
      if (het_nm == "HET_SAME_SHARE") {
        dspec <- dspec %>%
          filter(is.finite(same_origin_share_c_10y))
      }

      if (nrow(dspec) == 0) next
      ts_msg("Spec:", spec_nm, "| Het:", het_nm, "| N =", nrow(dspec))

      rhs_het <- HET_SPECS[[het_nm]](divvar)
      rhs_base <- paste0(rhs_het, " + cluster_size_10y + tenure + tenure_sq")

      # Main: PPML with inventor FE + year FE
      m <- fepois(
        as.formula(paste0("n_patents ~ ", rhs_base, " | user_id + year_fe")),
        data = dspec, vcov = ~cluster_id, notes=FALSE, warn=FALSE
      )

      # Robustness: PPML no inventor FE, keep year FE
      m0 <- fepois(
        as.formula(paste0("n_patents ~ ", rhs_base, " | year_fe")),
        data = dspec, vcov = ~cluster_id, notes=FALSE, warn=FALSE
      )

      tag1 <- paste0("PARENT__", spec_nm, "__", het_nm, "__PPML_inventorFE")
      tag0 <- paste0("PARENT__", spec_nm, "__", het_nm, "__PPML_noInventorFE")

      tab <- bind_rows(
        extract_fixest_terms(m,  tag1),
        extract_fixest_terms(m0, tag0)
      ) %>%
        mutate(
          cluster_level="PARENT",
          spec=spec_nm,
          controls=het_nm,
          n_obs=nrow(dspec)
        )

      out[[paste(spec_nm, het_nm, sep="__")]] <- tab
    }
  }

  tab_all <- bind_rows(out)

  write_csv(tab_all, file.path(DIRS$tables, "PARENT__coef_table_HETEROGENEITY_SEPARATE.csv"))
  tab_all
}

ts_msg("Running heterogeneity models (separate regressions)...")
tab_het <- run_code(df, meas, same_share)
ts_msg("DONE. Outputs in:", OUT_DIR)
