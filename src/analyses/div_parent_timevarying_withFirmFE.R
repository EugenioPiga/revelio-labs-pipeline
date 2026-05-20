##############################################################################
# TIME-VARYING DIVERSITY (cluster-year) + TIME-INVARIANT controls (10y)
# PARENT ONLY — NO RDS
#
# Diversity is time-varying: origin_div, edu_div, job_div computed per parent-year.
# Controls remain time-invariant: cluster_size_10y, immig_share_10y, immig_share_sq_10y (+ shares).
#
# Also tries versions with "firm FE" if first_rcid exists:
#   add | first_rcid as FE (in addition to inventor + year FE)
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
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_timevarying_PARENT"

DIRS <- list(
  tables       = file.path(OUT_DIR, "tables"),
  cluster_meas = file.path(OUT_DIR, "cluster_measures"),
  fe           = file.path(OUT_DIR, "fixed_effects_cluster")
)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
PARENT_VAR <- "first_parent_rcid"
FIRM_VAR_CANDIDATE <- "first_rcid"  # optional for "firm FE"

YEAR_START <- 2010
YEAR_END   <- 2024

US_COUNTRY <- "United States"

TOPK_ORIGINS <- 30
INCLUDE_MISSING_AS_CATEGORY <- FALSE

LAST10_N <- 10
LAST10_START <- YEAR_END - LAST10_N + 1
LAST10_END   <- YEAR_END
YEARS_LAST10 <- LAST10_START:LAST10_END

MIN_CLUSTER <- 10
TENURE_MAX  <- 50

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
    return(tibble(model = model_tag, term = character(0), estimate = numeric(0),
                  se = numeric(0), stat = numeric(0), p = numeric(0), stars = character(0)))
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

safe_name <- function(x) {
  x <- gsub("[^A-Za-z0-9]+", "_", x)
  x <- gsub("^_+|_+$", "", x)
  ifelse(nchar(x) == 0, "X", x)
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
# 4) Time-varying diversity (cluster-year)
# =========================
cluster_year_div_stats_dt <- function(df, cluster_var, country_var, div_outname, min_cluster = MIN_CLUSTER) {
  keep <- c("user_id","year",IMMIG_VAR,cluster_var,country_var)
  dt <- as.data.table(df[, keep])

  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "cvar_raw")

  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]

  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year)]
  dt <- dt[!is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]

  comp <- unique(dt[, .(user_id, year, cluster_id, imm, cvar_raw)])

  size_df <- comp[, .(
    cluster_size = uniqueN(user_id),
    immig_share  = mean(imm, na.rm = TRUE)
  ), by = .(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]

  tmp <- comp[!is.na(cvar_raw) & cvar_raw != "" & cvar_raw != "empty",
              .(n = uniqueN(user_id)),
              by = .(cluster_id, year, v = cvar_raw)]
  tmp[, tot := sum(n), by = .(cluster_id, year)]
  tmp[, share_sq := (n / tot)^2]
  div_df <- tmp[, .(div = 1 - sum(share_sq)), by = .(cluster_id, year)]
  setnames(div_df, "div", div_outname)

  out <- merge(size_df, div_df, by = c("cluster_id","year"), all.x = TRUE)
  out <- out[is.finite(get(div_outname))]
  as_tibble(out)
}

# =========================
# 5) Time-invariant controls (last10y size + imm share + shares)
# =========================
build_comp_dt <- function(df, cluster_var, country_var) {
  keep <- c("user_id","year", IMMIG_VAR, cluster_var, country_var)
  dt <- as.data.table(df[, keep])
  setnames(dt, cluster_var, "cluster_raw")
  setnames(dt, country_var, "cvar_raw")
  dt[, cluster_id := as.character(cluster_raw)]
  dt[, cluster_raw := NULL]
  dt <- dt[!is.na(cluster_id) & cluster_id != ""]
  dt <- dt[!is.na(year) & !is.na(user_id)]
  dt[, imm := as.integer(get(IMMIG_VAR))]
  dt[, cvar_raw := trimws(as.character(cvar_raw))]
  dt[cvar_raw == "" | cvar_raw == "empty", cvar_raw := NA_character_]

  pick1 <- function(x) {
    x <- x[!is.na(x) & x != "" & x != "empty"]
    if (length(x) == 0) NA_character_ else x[1]
  }
  dt[, .(imm = imm[1], cvar_raw = pick1(cvar_raw)), by = .(user_id, year, cluster_id)]
}

cluster_const_composition_last10 <- function(df10, cluster_var, country_var, prefix, topk = TOPK_ORIGINS) {
  comp <- build_comp_dt(df10, cluster_var, country_var)

  size_y <- comp[, .(cluster_size = uniqueN(user_id)), by = .(cluster_id, year)]
  size_y <- size_y[size_y$cluster_size >= MIN_CLUSTER]
  comp <- merge(comp, size_y[, .(cluster_id, year)], by = c("cluster_id","year"), all = FALSE)

  if (INCLUDE_MISSING_AS_CATEGORY) {
    comp[, v := fifelse(is.na(cvar_raw), "MISSING", cvar_raw)]
  } else {
    comp <- comp[!is.na(cvar_raw)]
    comp[, v := cvar_raw]
  }

  cnt <- comp[, .(n = .N), by = .(cluster_id, v)]
  glob <- cnt[, .(N = sum(n)), by = v][order(-N)]

  if (is.finite(topk)) {
    top_levels <- glob[v != "MISSING"][1:min(topk, .N), v]
  } else {
    top_levels <- glob[v != "MISSING", v]
  }

  cnt[, v2 := fifelse(v %in% top_levels | v == "MISSING", v, "OTHER")]
  cnt2 <- cnt[, .(n = sum(n)), by = .(cluster_id, v2)]
  cnt2[, tot := sum(n), by = cluster_id]
  cnt2[, share := fifelse(tot > 0, n / tot, NA_real_)]

  wide <- dcast(cnt2, cluster_id ~ v2, value.var = "share", fill = 0)
  share_cols <- setdiff(names(wide), "cluster_id")
  new_names <- paste0("share_", prefix, "_", sapply(share_cols, safe_name))
  setnames(wide, share_cols, new_names)
  as_tibble(wide)
}

make_timeinvariant_controls <- function(df_all, cluster_var, label) {
  df10 <- df_all %>% filter(year %in% YEARS_LAST10)

  # time-invariant size + imm share from last10y cluster-year size table (overall)
  size10 <- df10 %>%
    select(user_id, year, !!sym(IMMIG_VAR), !!sym(cluster_var)) %>%
    mutate(cluster_id = as.character(.data[[cluster_var]]),
           imm = as.integer(.data[[IMMIG_VAR]])) %>%
    filter(!is.na(cluster_id), cluster_id != "") %>%
    distinct(user_id, year, cluster_id, imm) %>%
    group_by(cluster_id, year) %>%
    summarise(cluster_size = n_distinct(user_id), immig_share = mean(imm, na.rm = TRUE), .groups = "drop") %>%
    filter(cluster_size >= MIN_CLUSTER) %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      immig_share_10y  = mean(immig_share, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      cluster_level = label,
      log_size_10y = ifelse(cluster_size_10y > 0, log(cluster_size_10y), NA_real_),
      immig_share_sq_10y = immig_share_10y^2
    )

  # shares (Stage B)
  shares_origin <- cluster_const_composition_last10(df10, cluster_var, "origin_country", "origin", TOPK_ORIGINS)
  shares_job    <- cluster_const_composition_last10(df10, cluster_var, "first_pos_country", "job", TOPK_ORIGINS)
  shares_edu    <- cluster_const_composition_last10(df10, cluster_var, "first_university_country", "edu", TOPK_ORIGINS)

  write_csv(size10, file.path(DIRS$cluster_meas, paste0(label, "__controls_const__", LAST10_START, "_", LAST10_END, ".csv")))
  write_csv(shares_origin, file.path(DIRS$cluster_meas, paste0(label, "__shares_origin__", LAST10_START, "_", LAST10_END, ".csv")))
  write_csv(shares_job,    file.path(DIRS$cluster_meas, paste0(label, "__shares_job__",    LAST10_START, "_", LAST10_END, ".csv")))
  write_csv(shares_edu,    file.path(DIRS$cluster_meas, paste0(label, "__shares_edu__",    LAST10_START, "_", LAST10_END, ".csv")))

  list(const = size10, shares = list(origin = shares_origin, job = shares_job, edu = shares_edu))
}

# =========================
# 6) Load Arrow + build df
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")

# Try to include firm var if exists
ds_cols <- names(ds)
has_firm <- FIRM_VAR_CANDIDATE %in% ds_cols

required <- c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR, PARENT_VAR
)
if (has_firm) required <- c(required, FIRM_VAR_CANDIDATE)
need_cols(ds_cols, required)

# first_pos_country on full panel
ts_msg("Computing first_pos_country from FULL panel...")
ds_pos <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(
    pos_country_y = if_else(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = if_else(pos_country_y == "", NA_character_, pos_country_y)
  ) %>%
  filter(!is.na(user_id), !is.na(year), !is.na(pos_country_y))

ds_first_year <- ds_pos %>% group_by(user_id) %>% summarise(first_pos_year = min(year, na.rm = TRUE), .groups="drop")
first_pos_tbl <- ds_pos %>%
  inner_join(ds_first_year, by="user_id") %>%
  filter(year == first_pos_year) %>%
  select(user_id, first_pos_country = pos_country_y) %>%
  collect() %>%
  mutate(user_id = as.character(user_id),
         first_pos_country = str_trim(as.character(first_pos_country))) %>%
  distinct(user_id, .keep_all = TRUE)

ts_msg("Collecting regression window 2010–2024...")
sel <- c("user_id","year","n_patents","first_country","first_university_country","first_startdate_edu","first_startdate_pos",IMMIG_VAR,PARENT_VAR)
if (has_firm) sel <- c(sel, FIRM_VAR_CANDIDATE)

df <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(all_of(sel)) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    year = as.integer(year),
    n_patents = as.numeric(n_patents),
    first_country = str_trim(as.character(first_country)),
    !!IMMIG_VAR := as.integer(.data[[IMMIG_VAR]]),
    first_university_country = str_trim(as.character(first_university_country)),
    !!PARENT_VAR := as.character(.data[[PARENT_VAR]])
  ) %>%
  left_join(first_pos_tbl, by="user_id")

df <- df %>% filter(!is.na(first_country), first_country == US_COUNTRY)

if (has_firm) {
  df <- df %>% mutate(!!FIRM_VAR_CANDIDATE := as.character(.data[[FIRM_VAR_CANDIDATE]]))
}

ts_msg("Deriving tenure + origin_country...")
df <- df %>% compute_tenure() %>% filter(!is.na(tenure)) %>% make_origin_country()

# =========================
# 7) Build time-varying diversity tables + time-invariant controls
# =========================
ts_msg("Computing time-varying diversity tables...")
div_origin_y <- cluster_year_div_stats_dt(df, PARENT_VAR, "origin_country", "origin_div_y")
div_job_y    <- cluster_year_div_stats_dt(df, PARENT_VAR, "first_pos_country", "job_div_y") %>%
  select(cluster_id, year, job_div_y)
div_edu_y    <- cluster_year_div_stats_dt(df, PARENT_VAR, "first_university_country", "edu_div_y") %>%
  select(cluster_id, year, edu_div_y)

div_y <- div_origin_y %>%
  left_join(div_job_y, by=c("cluster_id","year")) %>%
  left_join(div_edu_y, by=c("cluster_id","year"))

write_csv(div_y, file.path(DIRS$cluster_meas, "PARENT__cluster_year_timevarying_diversity_2010_2024.csv"))

ts_msg("Computing time-invariant controls (last10y)...")
controls_parent <- make_timeinvariant_controls(df, PARENT_VAR, "PARENT")

# =========================
# 8) Run models (time-varying diversity)
# =========================
run_models_parent <- function(df_all, div_y, controls_obj, has_firm) {

  const  <- controls_obj$const
  shares <- controls_obj$shares

  dd <- df_all %>%
    mutate(cluster_id = as.character(.data[[PARENT_VAR]])) %>%
    filter(!is.na(cluster_id), cluster_id != "",
           !is.na(user_id), user_id != "",
           !is.na(year), year >= YEAR_START, year <= YEAR_END,
           !is.na(n_patents), n_patents >= 0,
           is.finite(tenure), is.finite(tenure_sq)) %>%
    left_join(div_y, by=c("cluster_id","year")) %>%
    left_join(const, by="cluster_id") %>%
    mutate(
      user_id    = as.factor(user_id),
      year_fe    = as.factor(year),
      cluster_id = as.factor(cluster_id)
    )

  if (has_firm) dd <- dd %>% mutate(first_rcid_fe = as.factor(.data[[FIRM_VAR_CANDIDATE]]))

  SPECS <- list(origin="origin_div_y", edu="edu_div_y", job="job_div_y")
  all_tabs <- list()

  ts_msg("Stage A (base/imm/imm2) starting for: PARENT (time-varying div)")

  for (spec_nm in names(SPECS)) {
    divvar <- SPECS[[spec_nm]]

    dspec <- dd %>% filter(is.finite(.data[[divvar]]), is.finite(cluster_size_10y), is.finite(immig_share_10y))
    ts_msg("Spec:", spec_nm, "| N =", nrow(dspec))
    if (nrow(dspec) == 0) next

    RHS_base <- paste0(divvar, " + cluster_size_10y + tenure + tenure_sq")
    CONTROL_A <- list(
      base = RHS_base,
      imm  = paste0(RHS_base, " + immig_share_10y"),
      imm2 = paste0(RHS_base, " + immig_share_10y + immig_share_sq_10y")
    )

    for (ctrl_tag in names(CONTROL_A)) {
      rhs <- CONTROL_A[[ctrl_tag]]
      ts_msg("  ->", spec_nm, "|", ctrl_tag)

      # baseline FE set
      fe1 <- "user_id + year_fe"
      fe0 <- "year_fe"

      # optionally add firm FE (if available)
      fe1_firm <- if (has_firm) paste0(fe1, " + first_rcid_fe") else fe1
      fe0_firm <- if (has_firm) paste0(fe0, " + first_rcid_fe") else fe0

      # PPML/OLS without firm FE
      m_ppml_fe <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
      m_ppml_nofe <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
      m_ols_fe <- feols(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
      m_ols_nofe <- feols(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)

      tab <- bind_rows(
        extract_fixest_terms(m_ppml_fe,   paste0("PARENT__", spec_nm, "__", ctrl_tag, "__PPML_inventorFE")),
        extract_fixest_terms(m_ppml_nofe, paste0("PARENT__", spec_nm, "__", ctrl_tag, "__PPML_noInventorFE")),
        extract_fixest_terms(m_ols_fe,    paste0("PARENT__", spec_nm, "__", ctrl_tag, "__OLS_inventorFE")),
        extract_fixest_terms(m_ols_nofe,  paste0("PARENT__", spec_nm, "__", ctrl_tag, "__OLS_noInventorFE"))
      ) %>% mutate(cluster_level="PARENT", spec=spec_nm, controls=ctrl_tag, n_obs=nrow(dspec), timevarying_div=TRUE, firmFE=FALSE)

      all_tabs[[paste0(spec_nm,"__",ctrl_tag,"__nofirmFE")]] <- tab

      # versions WITH firm FE (if available)
      if (has_firm) {
        m_ppml_fe_f <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1_firm)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
        m_ppml_nofe_f <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0_firm)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
        m_ols_fe_f <- feols(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1_firm)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)
        m_ols_nofe_f <- feols(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0_firm)), data=dspec, vcov=~cluster_id, notes=FALSE, warn=FALSE)

        tab_f <- bind_rows(
          extract_fixest_terms(m_ppml_fe_f,   paste0("PARENT__", spec_nm, "__", ctrl_tag, "__PPML_inventorFE_firmFE")),
          extract_fixest_terms(m_ppml_nofe_f, paste0("PARENT__", spec_nm, "__", ctrl_tag, "__PPML_noInventorFE_firmFE")),
          extract_fixest_terms(m_ols_fe_f,    paste0("PARENT__", spec_nm, "__", ctrl_tag, "__OLS_inventorFE_firmFE")),
          extract_fixest_terms(m_ols_nofe_f,  paste0("PARENT__", spec_nm, "__", ctrl_tag, "__OLS_noInventorFE_firmFE"))
        ) %>% mutate(cluster_level="PARENT", spec=spec_nm, controls=ctrl_tag, n_obs=nrow(dspec), timevarying_div=TRUE, firmFE=TRUE)

        all_tabs[[paste0(spec_nm,"__",ctrl_tag,"__firmFE")]] <- tab_f
      }
    }

    write_csv(bind_rows(all_tabs), file.path(DIRS$tables, "PARENT__coef_table_PROGRESS.csv"))
  }

  tab_stageA <- bind_rows(all_tabs)
  write_csv(tab_stageA, file.path(DIRS$tables, "PARENT__coef_table_STAGEA.csv"))
  ts_msg("Stage A saved.")

  # Stage B imm_shares (optional; can be huge; leaving it in but you can comment out)
  ts_msg("Stage B (imm_shares) starting (time-varying div) ...")

  for (spec_nm in names(SPECS)) {
    divvar <- SPECS[[spec_nm]]
    dspec <- dd %>% filter(is.finite(.data[[divvar]]), is.finite(cluster_size_10y), is.finite(immig_share_10y))
    if (nrow(dspec) == 0) next

    shares_tbl <- shares[[spec_nm]]
    if (is.null(shares_tbl) || nrow(shares_tbl) == 0) next

    share_cols <- setdiff(names(shares_tbl), "cluster_id")
    dspec_s <- dspec %>% left_join(shares_tbl, by="cluster_id")
    if (length(share_cols) > 0) dspec_s <- dspec_s %>% mutate(across(all_of(share_cols), ~ ifelse(is.na(.x), 0, .x)))
    share_cols_use <- if (length(share_cols) >= 2) share_cols[-length(share_cols)] else share_cols

    rhs <- paste0(divvar, " + cluster_size_10y + tenure + tenure_sq + immig_share_10y",
                  if (length(share_cols_use) > 0) paste0(" + ", paste(share_cols_use, collapse=" + ")) else "")

    fe1 <- "user_id + year_fe"
    fe0 <- "year_fe"
    fe1_firm <- if (has_firm) paste0(fe1, " + first_rcid_fe") else fe1
    fe0_firm <- if (has_firm) paste0(fe0, " + first_rcid_fe") else fe0

    m_ppml_fe <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1)), data=dspec_s, vcov=~cluster_id, notes=FALSE, warn=FALSE)
    m_ppml_nofe <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0)), data=dspec_s, vcov=~cluster_id, notes=FALSE, warn=FALSE)

    tab <- bind_rows(
      extract_fixest_terms(m_ppml_fe,   paste0("PARENT__", spec_nm, "__imm_shares__PPML_inventorFE")),
      extract_fixest_terms(m_ppml_nofe, paste0("PARENT__", spec_nm, "__imm_shares__PPML_noInventorFE"))
    ) %>% mutate(cluster_level="PARENT", spec=spec_nm, controls="imm_shares", n_obs=nrow(dspec_s), timevarying_div=TRUE, firmFE=FALSE)

    all_tabs[[paste0(spec_nm,"__imm_shares__nofirmFE")]] <- tab

    if (has_firm) {
      m_ppml_fe_f <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe1_firm)), data=dspec_s, vcov=~cluster_id, notes=FALSE, warn=FALSE)
      m_ppml_nofe_f <- fepois(as.formula(paste0("n_patents ~ ", rhs, " | ", fe0_firm)), data=dspec_s, vcov=~cluster_id, notes=FALSE, warn=FALSE)

      tab_f <- bind_rows(
        extract_fixest_terms(m_ppml_fe_f,   paste0("PARENT__", spec_nm, "__imm_shares__PPML_inventorFE_firmFE")),
        extract_fixest_terms(m_ppml_nofe_f, paste0("PARENT__", spec_nm, "__imm_shares__PPML_noInventorFE_firmFE"))
      ) %>% mutate(cluster_level="PARENT", spec=spec_nm, controls="imm_shares", n_obs=nrow(dspec_s), timevarying_div=TRUE, firmFE=TRUE)

      all_tabs[[paste0(spec_nm,"__imm_shares__firmFE")]] <- tab_f
    }

    write_csv(bind_rows(all_tabs), file.path(DIRS$tables, "PARENT__coef_table_PROGRESS.csv"))
  }

  tab_all <- bind_rows(all_tabs)
  write_csv(tab_all, file.path(DIRS$tables, "PARENT__coef_table_ALL.csv"))
  tab_all
}

ts_msg("Running models...")
tab2 <- run_models_parent(df, div_y, controls_parent, has_firm)
ts_msg("DONE. Outputs in:", OUT_DIR)
##############################################################################
