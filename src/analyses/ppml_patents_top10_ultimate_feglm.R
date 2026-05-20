#!/usr/bin/env Rscript
###############################################################################
# Unified PPML Runner — All FE Specs × All Geographies
# Author: Eugenio — 2025-10-22 (optimized version)
###############################################################################

# ============================
# Setup
# ============================
user_lib <- "~/R/library"
if (!dir.exists(user_lib)) dir.create(user_lib, recursive = TRUE)
.libPaths(c(user_lib, .libPaths()))

pkgs <- c("arrow", "dplyr", "readr", "alpaca", "lubridate", "broom")
for (p in pkgs) if (!requireNamespace(p, quietly = TRUE)) {
  install.packages(p, repos = "https://cloud.r-project.org", lib = user_lib)
}
if (!requireNamespace("remotes", quietly = TRUE))
  install.packages("remotes", repos = "https://cloud.r-project.org", lib = user_lib)
if (!requireNamespace("alpaca", quietly = TRUE)) {
  remotes::install_github("amrei-stammann/alpaca", lib = user_lib)
}

library(arrow)
library(dplyr)
library(readr)
library(alpaca)

INPUT <- "/labs/khanna/linkedin_202507/processed/inventor_year_merged"
OUT_DIR <- "/home/epiga/revelio_labs/output/regressions/ppml_loop_runs"
dir.create(OUT_DIR, recursive = TRUE, showWarnings = FALSE)

cat("\n=======================================\n")
cat("[INFO] Loading data once from parquet\n")
cat("=======================================\n\n")

cat("\n[DEBUG] Columns in dataset BEFORE select():\n")
print(open_dataset(INPUT, format = "parquet")$schema)

df0 <- open_dataset(INPUT, format = "parquet") %>%
  select(
    user_id, n_patents, year,
    first_country,
    first_startdate_edu, first_startdate_pos,
    first_state, first_city, first_metro_area,
    first_rcid, first_parent_rcid
  ) %>%
  collect() %>%
  filter(first_country == "United States")

cat("[INFO] Data loaded:", nrow(df0), "rows\n")

# ============================
# Restrict to Top 10% Inventors
# ============================
cat("[INFO] Computing top 10% inventors...\n")

inv_tot <- df0 %>%
  group_by(user_id) %>%
  summarise(total = sum(n_patents, na.rm = TRUE), .groups = "drop")

p90 <- quantile(inv_tot$total, 0.90, na.rm = TRUE)
top10 <- inv_tot %>% filter(total >= p90) %>% pull(user_id)

df0 <- df0 %>% filter(user_id %in% top10)
cat("[INFO] Remaining after top 10% filter:", nrow(df0), "\n\n")

# ============================
# Tenure construction function
# ============================
compute_tenure <- function(df) {
  df %>%
    mutate(
      edu_year = as.numeric(substr(first_startdate_edu, 1, 4)),
      pos_year = as.numeric(substr(first_startdate_pos, 1, 4)),
      tenure = year - edu_year + 3,
      tenure = ifelse(is.na(tenure) | tenure > 50, year - pos_year, tenure),
      tenure = ifelse(tenure > 50 | tenure < 0, NA, tenure),
      tenure_sq = tenure^2
    ) %>%
    filter(!is.na(tenure))
}

df0 <- compute_tenure(df0)

# ============================
# Config: Geographies & Firm FE Specs
# ============================
geos <- list(
  state = "first_state",
  city  = "first_city",
  metro = "first_metro_area"
)

firm_fes <- list(
  rcid    = "first_rcid",
  parent  = "first_parent_rcid",
  nofirm  = NULL
)

# ============================
# Main runner function
# ============================
run_model <- function(df, geo_name, geo_var, firm_name, firm_var) {

  cat("\n=====================================================\n")
  cat("[INFO] Running:", geo_name, "| Firm FE:", firm_name, "\n")
  cat("=====================================================\n")

  # ---------------------------------
  # Build FE formula
  # ---------------------------------
  fe_terms <- c("user_id", geo_var, "year")
  if (!is.null(firm_var) && !is.na(firm_var) && firm_var != "") {
    fe_terms <- c(fe_terms, firm_var)
  }

  formula <- as.formula(
    paste0("n_patents ~ tenure + tenure_sq | ",
           paste(fe_terms, collapse = " + "))
  )

  # ---------------------------------
  # Output file naming
  # ---------------------------------
  tag <- paste(geo_name, firm_name, sep = "_")
  OUT_BASE <- file.path(OUT_DIR, paste0("ppml_top10_", tag))
  FE_DIR   <- file.path(OUT_DIR, "fe", tag)
  dir.create(FE_DIR, recursive = TRUE, showWarnings = FALSE)

  # ---------------------------------
  # Run PPML
  # ---------------------------------
  cat("[INFO] Estimating PPML...\n")
  t0 <- Sys.time()

  model <- alpaca::feglm(
    formula = formula,
    data = df,
    family = poisson(),
    control = alpaca::feglmControl(
      dev.tol    = 1e-8,
      center.tol = 1e-8,
      iter.max   = 80,
      trace      = FALSE
    )
  )

  runtime <- round(as.numeric(Sys.time() - t0, units = "secs"), 2)
  cat("[INFO] Runtime:", runtime, "seconds\n")

  saveRDS(model, paste0(OUT_BASE, ".rds"))

  sink(paste0(OUT_BASE, "_summary.txt"))
  print(summary(model))
  cat("\nRuntime:", runtime, "seconds\n")
  sink()

  # ============================================================
  # 1. Extract Fixed Effects AND prepare them for merging
  # ============================================================
  cat("[INFO] Extracting and preparing fixed effects...\n")

  fe_list   <- alpaca::getFEs(model)
  fe_frames <- list()

  for (fe_name in names(fe_list)) {

    # clean name in case of dots
    clean_name <- gsub("\\.", "_", fe_name)

    fe_df <- tibble(
      level = names(fe_list[[fe_name]]),
      fe    = as.numeric(fe_list[[fe_name]])
    )

    # Save raw FE as before
    write_csv(
      fe_df,
      file.path(FE_DIR, paste0("fe_", tag, "_", fe_name, ".csv"))
    )

    # Prepare for merging: key column and fe_ column
    names(fe_df) <- c(clean_name, paste0("fe_", clean_name))
    fe_frames[[clean_name]] <- fe_df
  }

  # ============================================================
  # 2. Prediction on estimation sample
  # ============================================================
  cat("[INFO] Building decomposition dataset...\n")

  yhat <- predict(model, type = "response")

  est <- model$model
  if (is.null(est) || nrow(est) == 0) {
    est <- model$data
  }
  if (is.null(est) || nrow(est) == 0) {
    stop("[ERROR] alpaca did not retain the estimation sample")
  }
  if (length(yhat) != nrow(est)) {
    stop("[ERROR] Prediction vector length mismatch with estimation sample")
  }

  est$yhat <- yhat

  # ============================================================
  # 3. Harmonize join keys
  # ============================================================
  join_keys <- c("user_id", geo_var, "year")

  df_join  <- df  %>% mutate(across(all_of(join_keys), as.character))
  est_join <- est %>% mutate(across(all_of(join_keys), as.character))

  # ============================================================
  # 4. Merge predictions back into full df
  #    IMPORTANT: only bring yhat, NOT tenure/tenure_sq
  # ============================================================
  dec <- df_join %>%
    left_join(
      est_join %>% select(all_of(join_keys), yhat),
      by = join_keys
    )

  # At this point:
  # - tenure, tenure_sq come from df_join (computed by compute_tenure(df0))
  # - yhat comes from est_join

  # ============================================================
  # 5. Harmonize FE join key types
  # ============================================================
  for (key in names(fe_frames)) {

    # FE df: make sure join key is character
    fe_frames[[key]][[key]] <- as.character(fe_frames[[key]][[key]])

    # dec: if column exists, cast to character
    if (key %in% names(dec)) {
      dec[[key]] <- as.character(dec[[key]])
    }
  }

  # ============================================================
  # 6. Merge FE into decomposition dataset
  # ============================================================
  cat("[INFO] Merging FE into decomposition dataset...\n")

  for (key in names(fe_frames)) {
    if (key %in% names(dec)) {
      dec <- dec %>%
        left_join(fe_frames[[key]], by = key)
    }
  }

  # ============================================================
  # 7. Final selection
  # ============================================================
  dec <- dec %>%
    select(
      user_id, year, n_patents,
      tenure, tenure_sq,
      all_of(geo_var),
      yhat,
      starts_with("fe_")   # include all fixed effects
    )

  # ============================================================
  # 8. Save decomposition dataset
  # ============================================================
  write_csv(dec, paste0(OUT_BASE, "_decomposition.csv"))
  cat("[INFO] Decomposition saved for:", tag, "\n")
}


# ============================
# RUN ALL SPECIFICATIONS
# ============================

for (geo_name in names(geos)) {
  for (firm_name in names(firm_fes)) {
    run_model(
      df  = df0,
      geo_name = geo_name,
      geo_var  = geos[[geo_name]],
      firm_name = firm_name,
      firm_var  = firm_fes[[firm_name]]
    )
  }
}

cat("\n==============================\n")
cat("[INFO] ALL MODELS COMPLETED\n")
cat("==============================\n\n")
