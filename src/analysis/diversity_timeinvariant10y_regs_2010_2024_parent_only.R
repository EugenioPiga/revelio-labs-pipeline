##############################################################################
# TIME-INVARIANT (LAST-10Y) DIVERSITY + SIZE — PARENT ONLY — NO RDS
#
#   - Compute cluster measures on FULL panel (incl. abroad years),
#     then filter regression sample to US inventor-years only:
#       first_country == "United States"
#   - Run both UNWEIGHTED and WEIGHTED regressions:
#       weights = cluster_size_10y
#
# Models (spec ∈ {origin, job, edu}):
#   Stage A: base / imm / imm2
#   Stage B: imm_shares (topK + OTHER)
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
OUT_DIR <- "/home/epiga/revelio_labs/output/diversity_timeinvariant10y_PARENT"

DIRS <- list(
  tables       = file.path(OUT_DIR, "tables"),
  fe           = file.path(OUT_DIR, "fixed_effects_cluster"),
  cluster_meas = file.path(OUT_DIR, "cluster_measures_timeinvariant10y")
)
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)
lapply(DIRS, dir.create, recursive = TRUE, showWarnings = FALSE)

IMMIG_VAR  <- "immig_first_deg_or_job_nonUS"
PARENT_VAR <- "first_parent_rcid"

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
# 3) Derived variables (row-level)
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
      uni_ok = !is.na(uni_c) & uni_c != "" & uni_c != US_COUNTRY,
      pos_ok = !is.na(pos_c) & pos_c != "" & pos_c != US_COUNTRY,
      origin_country = case_when(
        .data[[IMMIG_VAR]] == 0L ~ US_COUNTRY,
        .data[[IMMIG_VAR]] == 1L & uni_ok ~ uni_c,
        .data[[IMMIG_VAR]] == 1L & !uni_ok & pos_ok ~ pos_c,
        TRUE ~ NA_character_
      )
    )
}

# =========================
# 4) Composition shares (time-invariant, last10)  [Stage B controls]
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

  # Restrict to cluster-years meeting MIN_CLUSTER
  size_y <- comp[, .(cluster_size = uniqueN(user_id)), by = .(cluster_id, year)]
  size_y <- size_y[cluster_size >= MIN_CLUSTER]
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

# =========================
# 5) Diversity: OVERALL only (cluster-year)
# =========================
cluster_year_div_overall <- function(df, cluster_var, country_var, div_prefix, min_cluster = MIN_CLUSTER) {

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

  # cluster-year size + immigrant share (computed on panel provided)
  size_df <- comp[, .(cluster_size = uniqueN(user_id),
                      immig_share = mean(imm, na.rm = TRUE)),
                  by = .(cluster_id, year)]
  size_df <- size_df[cluster_size >= min_cluster]

  # diversity (1 - HHI) on all inventors (overall)
  xdt <- comp[!is.na(cvar_raw) & cvar_raw != "" & cvar_raw != "empty"]
  if (nrow(xdt) == 0) {
    out <- size_df
    out[, paste0(div_prefix, "_div_all") := NA_real_]
    return(as_tibble(out))
  }

  tmp <- xdt[, .(n = uniqueN(user_id)), by = .(cluster_id, year, v = cvar_raw)]
  tmp[, tot := sum(n), by = .(cluster_id, year)]
  tmp[, share_sq := (n / tot)^2]
  div_df <- tmp[, .(div = 1 - sum(share_sq)), by = .(cluster_id, year)]
  setnames(div_df, "div", paste0(div_prefix, "_div_all"))

  out <- merge(size_df, div_df, by = c("cluster_id","year"), all.x = TRUE)
  as_tibble(out)
}

compute_cluster_bundle_overall <- function(df10, cluster_var) {
  ts_msg("Computing PARENT cluster-year diversity (OVERALL only) ...")

  origin <- cluster_year_div_overall(df10, cluster_var, "origin_country", "origin", MIN_CLUSTER)

  job <- cluster_year_div_overall(df10, cluster_var, "first_pos_country", "job", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("job_"))

  edu <- cluster_year_div_overall(df10, cluster_var, "first_university_country", "edu", MIN_CLUSTER) %>%
    select(cluster_id, year, starts_with("edu_"))

  origin %>%
    left_join(job, by = c("cluster_id","year")) %>%
    left_join(edu, by = c("cluster_id","year"))
}

make_timeinvariant_measures_overall <- function(df_all_fullpanel, cluster_var, label) {
  ts_msg("Building time-invariant (last10y) measures for:", label, "(FULL PANEL)")

  df10 <- df_all_fullpanel %>% filter(year %in% YEARS_LAST10)

  stats_y <- compute_cluster_bundle_overall(df10, cluster_var)
  write_csv(stats_y, file.path(DIRS$cluster_meas, paste0(label, "__cluster_year__", LAST10_START, "_", LAST10_END, ".csv")))

  # collapse to cluster constants
  stats_c <- stats_y %>%
    group_by(cluster_id) %>%
    summarise(
      cluster_size_10y = mean(cluster_size, na.rm = TRUE),
      immig_share_10y  = mean(immig_share, na.rm = TRUE),

      origin_div_10y = mean(origin_div_all, na.rm = TRUE),
      job_div_10y    = mean(job_div_all, na.rm = TRUE),
      edu_div_10y    = mean(edu_div_all, na.rm = TRUE),

      years_used_10y = dplyr::n(),
      .groups = "drop"
    ) %>%
    mutate(
      cluster_level      = label,
      log_size_10y       = ifelse(is.finite(cluster_size_10y) & cluster_size_10y > 0, log(cluster_size_10y), NA_real_),
      immig_share_sq_10y = immig_share_10y^2
    ) %>%
    filter(
      is.finite(cluster_size_10y), cluster_size_10y > 0,
      is.finite(immig_share_10y),
      is.finite(origin_div_10y), is.finite(job_div_10y), is.finite(edu_div_10y)
    )

  write_csv(stats_c, file.path(DIRS$cluster_meas, paste0(label, "__cluster_const__", LAST10_START, "_", LAST10_END, ".csv")))

  # shares controls (Stage B)
  ts_msg("Computing time-invariant share controls (last10y) for:", label, "(FULL PANEL)")
  shares_origin <- cluster_const_composition_last10(df10, cluster_var, "origin_country", "origin", TOPK_ORIGINS)
  shares_job    <- cluster_const_composition_last10(df10, cluster_var, "first_pos_country", "job", TOPK_ORIGINS)
  shares_edu    <- cluster_const_composition_last10(df10, cluster_var, "first_university_country", "edu", TOPK_ORIGINS)

  write_csv(shares_origin, file.path(DIRS$cluster_meas, paste0(label, "__shares_origin__", LAST10_START, "_", LAST10_END, ".csv")))
  write_csv(shares_job,    file.path(DIRS$cluster_meas, paste0(label, "__shares_job__",    LAST10_START, "_", LAST10_END, ".csv")))
  write_csv(shares_edu,    file.path(DIRS$cluster_meas, paste0(label, "__shares_edu__",    LAST10_START, "_", LAST10_END, ".csv")))

  list(const = stats_c, shares = list(origin = shares_origin, job = shares_job, edu = shares_edu))
}

# =========================
# 6) Load Arrow + build df (FULL PANEL first)
# =========================
ts_msg("Opening Arrow dataset...")
ds <- open_dataset(INPUT, format = "parquet")

need_cols(names(ds), c(
  "user_id","year","n_patents",
  "first_country","last_country",
  "first_university_country",
  "first_startdate_edu","first_startdate_pos",
  IMMIG_VAR, PARENT_VAR
))

# first_pos_country computed from FULL panel
ts_msg("Computing first_pos_country from FULL panel (before year filtering)...")
ds_pos <- ds %>%
  select(user_id, year, first_country, last_country) %>%
  mutate(
    pos_country_y = if_else(!is.na(last_country) & last_country != "", last_country, first_country),
    pos_country_y = if_else(pos_country_y == "", NA_character_, pos_country_y)
  ) %>%
  filter(!is.na(user_id), !is.na(year), !is.na(pos_country_y))

ds_first_year <- ds_pos %>%
  group_by(user_id) %>%
  summarise(first_pos_year = min(year, na.rm = TRUE), .groups = "drop")

first_pos_tbl <- ds_pos %>%
  inner_join(ds_first_year, by = "user_id") %>%
  filter(year == first_pos_year) %>%
  select(user_id, first_pos_country = pos_country_y) %>%
  collect() %>%
  mutate(
    user_id = as.character(user_id),
    first_pos_country = str_trim(as.character(first_pos_country))
  ) %>%
  distinct(user_id, .keep_all = TRUE)

# Collect FULL PANEL in regression window (NO US FILTER HERE)
ts_msg("Collecting FULL PANEL years:", YEAR_START, "to", YEAR_END, "(NO US FILTER)")
df_full <- ds %>%
  filter(year >= YEAR_START, year <= YEAR_END) %>%
  select(
    user_id, year, n_patents, first_country,
    first_university_country,
    first_startdate_edu, first_startdate_pos,
    !!sym(IMMIG_VAR), !!sym(PARENT_VAR)
  ) %>%
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
  left_join(first_pos_tbl, by = "user_id")

# Derive origin_country + tenure on FULL PANEL (still no US filter)
ts_msg("Deriving origin_country + tenure on FULL PANEL...")
df_full <- df_full %>%
  make_origin_country() %>%
  compute_tenure()

# =========================
# 7) Build time-invariant measures on FULL PANEL (incl abroad years)
# =========================
ts_msg("Last-10y window is:", LAST10_START, "-", LAST10_END)
meas_parent <- make_timeinvariant_measures_overall(df_full, PARENT_VAR, "PARENT")

# =========================
# 8) Run models on US inventor-years ONLY (first_country == US)
# =========================
run_models_parent_code1 <- function(df_fullpanel, cluster_var, label, meas_obj) {
  ts_msg("=== RUNNING MODELS FOR:", label, "===")

  meas_const  <- meas_obj$const
  meas_shares <- meas_obj$shares

  # Regression sample: US inventor-years only
  ts_msg("Applying regression-only filter: first_country == US_COUNTRY")
  df_us <- df_fullpanel %>% filter(!is.na(first_country), first_country == US_COUNTRY)

  dd <- df_us %>%
    mutate(cluster_id = as.character(.data[[cluster_var]])) %>%
    filter(
      !is.na(cluster_id), cluster_id != "",
      !is.na(user_id), user_id != "",
      !is.na(year), year >= YEAR_START, year <= YEAR_END,
      !is.na(n_patents), n_patents >= 0,
      is.finite(tenure), is.finite(tenure_sq)
    ) %>%
    left_join(meas_const, by = "cluster_id") %>%
    mutate(
      user_id    = as.factor(user_id),
      year_fe    = as.factor(year),
      cluster_id = as.factor(cluster_id)
    )

  # Diversity variables (overall only)
  SPECS <- list(origin = "origin_div_10y", edu = "edu_div_10y", job = "job_div_10y")

  # Weighting options: run both
  WEIGHTS <- list(
    unweighted = NULL,
    weighted   = ~ cluster_size_10y
  )

  all_tabs <- list()

  # ----------------------------
  # STAGE A: base / imm / imm2
  # ----------------------------
  ts_msg("Stage A (base/imm/imm2) starting for:", label)

  for (spec_nm in names(SPECS)) {
    div_var <- SPECS[[spec_nm]]

    RHS_base <- paste0(div_var, " + cluster_size_10y + tenure + tenure_sq")

    dspec <- dd %>%
      filter(
        is.finite(.data[[div_var]]),
        is.finite(cluster_size_10y),
        is.finite(immig_share_10y)
      )

    ts_msg("Spec:", spec_nm, "| Stage A N =", nrow(dspec))
    if (nrow(dspec) == 0) next

    CONTROL_A <- list(
      base = RHS_base,
      imm  = paste0(RHS_base, " + immig_share_10y"),
      imm2 = paste0(RHS_base, " + immig_share_10y + immig_share_sq_10y")
    )

    for (ctrl_tag in names(CONTROL_A)) {
      rhs <- CONTROL_A[[ctrl_tag]]

      for (wtag in names(WEIGHTS)) {
        wform <- WEIGHTS[[wtag]]
        ts_msg("  ->", spec_nm, "|", ctrl_tag, "|", wtag, "| N =", nrow(dspec))

        m_ppml_fe <- fixest::fepois(
          as.formula(paste0("n_patents ~ ", rhs, " | user_id + year_fe")),
          data = dspec, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
        )
        m_ppml_nofe <- fixest::fepois(
          as.formula(paste0("n_patents ~ ", rhs, " | year_fe")),
          data = dspec, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
        )
        m_ols_fe <- fixest::feols(
          as.formula(paste0("n_patents ~ ", rhs, " | user_id + year_fe")),
          data = dspec, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
        )
        m_ols_nofe <- fixest::feols(
          as.formula(paste0("n_patents ~ ", rhs, " | year_fe")),
          data = dspec, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
        )

        tab <- bind_rows(
          extract_fixest_terms(m_ppml_fe,   paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__PPML_inventorFE")),
          extract_fixest_terms(m_ppml_nofe, paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__PPML_noInventorFE")),
          extract_fixest_terms(m_ols_fe,    paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__OLS_inventorFE")),
          extract_fixest_terms(m_ols_nofe,  paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__OLS_noInventorFE"))
        ) %>%
          mutate(
            cluster_level = label,
            spec = spec_nm,
            controls = ctrl_tag,
            weight = wtag,
            n_obs = nrow(dspec)
          )

        all_tabs[[paste0(spec_nm, "__", ctrl_tag, "__", wtag)]] <- tab
      }

      write_csv(bind_rows(all_tabs), file.path(DIRS$tables, paste0(label, "__coef_table_PROGRESS.csv")))
    }
  }

  tab_stageA <- bind_rows(all_tabs)
  write_csv(tab_stageA, file.path(DIRS$tables, paste0(label, "__coef_table_STAGEA_nonshare.csv")))
  ts_msg("Stage A complete and saved for:", label)

  # ----------------------------
  # STAGE B: imm_shares (slow)
  # ----------------------------
  ts_msg("Stage B (imm_shares) starting for:", label, "(may be slow)")

  for (spec_nm in names(SPECS)) {
    div_var <- SPECS[[spec_nm]]
    RHS_base <- paste0(div_var, " + cluster_size_10y + tenure + tenure_sq")

    dspec <- dd %>%
      filter(
        is.finite(.data[[div_var]]),
        is.finite(cluster_size_10y),
        is.finite(immig_share_10y)
      )
    if (nrow(dspec) == 0) next

    shares_tbl <- meas_shares[[spec_nm]]
    if (is.null(shares_tbl) || nrow(shares_tbl) == 0) {
      ts_msg("Spec:", spec_nm, "| shares table missing/empty — skipping imm_shares.")
      next
    }

    share_cols <- setdiff(names(shares_tbl), "cluster_id")
    dspec_s <- dspec %>% left_join(shares_tbl, by = "cluster_id")

    if (length(share_cols) > 0) {
      dspec_s <- dspec_s %>% mutate(across(all_of(share_cols), ~ ifelse(is.na(.x), 0, .x)))
    }

    # drop last share col to avoid collinearity (baseline share = omitted category)
    share_cols_use <- if (length(share_cols) >= 2) share_cols[-length(share_cols)] else share_cols

    rhs_shares <- paste0(
      RHS_base, " + immig_share_10y",
      if (length(share_cols_use) > 0) paste0(" + ", paste(share_cols_use, collapse = " + ")) else ""
    )

    ctrl_tag <- "imm_shares"

    for (wtag in names(WEIGHTS)) {
      wform <- WEIGHTS[[wtag]]
      ts_msg("  ->", spec_nm, "|", ctrl_tag, "|", wtag,
             "| N =", nrow(dspec_s), "| #share controls =", length(share_cols_use))

      m_ppml_fe <- fixest::fepois(
        as.formula(paste0("n_patents ~ ", rhs_shares, " | user_id + year_fe")),
        data = dspec_s, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
      )
      m_ppml_nofe <- fixest::fepois(
        as.formula(paste0("n_patents ~ ", rhs_shares, " | year_fe")),
        data = dspec_s, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
      )
      m_ols_fe <- fixest::feols(
        as.formula(paste0("n_patents ~ ", rhs_shares, " | user_id + year_fe")),
        data = dspec_s, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
      )
      m_ols_nofe <- fixest::feols(
        as.formula(paste0("n_patents ~ ", rhs_shares, " | year_fe")),
        data = dspec_s, vcov = ~cluster_id, weights = wform, notes = FALSE, warn = FALSE
      )

      tab <- bind_rows(
        extract_fixest_terms(m_ppml_fe,   paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__PPML_inventorFE")),
        extract_fixest_terms(m_ppml_nofe, paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__PPML_noInventorFE")),
        extract_fixest_terms(m_ols_fe,    paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__OLS_inventorFE")),
        extract_fixest_terms(m_ols_nofe,  paste0(label, "__", spec_nm, "__", ctrl_tag, "__", wtag, "__OLS_noInventorFE"))
      ) %>%
        mutate(
          cluster_level = label,
          spec = spec_nm,
          controls = ctrl_tag,
          weight = wtag,
          n_obs = nrow(dspec_s)
        )

      all_tabs[[paste0(spec_nm, "__", ctrl_tag, "__", wtag)]] <- tab
      write_csv(bind_rows(all_tabs), file.path(DIRS$tables, paste0(label, "__coef_table_PROGRESS.csv")))
    }
  }

  tab_all <- bind_rows(all_tabs)
  write_csv(tab_all, file.path(DIRS$tables, paste0(label, "__coef_table_ALLSPECS_ALLCONTROLS.csv")))

  # Baseline PPML cluster FE (unweighted)
  ts_msg("Estimating baseline PPML with cluster FE for:", label, "(UNWEIGHTED)")
  m_base <- fixest::fepois(
    as.formula("n_patents ~ tenure + tenure_sq | user_id + year_fe + cluster_id"),
    data = dd, notes = FALSE, warn = FALSE
  )

  fe_cluster <- fixest::fixef(m_base)[["cluster_id"]]
  if (!is.null(fe_cluster)) {
    write_csv(
      tibble(cluster_id = names(fe_cluster), fe_value = as.numeric(fe_cluster)),
      file.path(DIRS$fe, paste0("FE_cluster__", label, "__PPMLbaseline.csv"))
    )
  }

  list(data_n = nrow(dd), coef_table = tab_all)
}

# =========================
# 9) Run
# =========================
ts_msg("Running regressions for PARENT only...")
out_parent <- run_models_parent_code1(df_full, PARENT_VAR, "PARENT", meas_parent)

write_csv(out_parent$coef_table, file.path(DIRS$tables, "PARENT__coef_table.csv"))
ts_msg("DONE. Outputs in:", OUT_DIR)
##############################################################################
