#!/usr/bin/env Rscript
# Fast unit checks for shared helpers. No Revelio data are read.

script <- sub("^--file=", "", commandArgs(trailingOnly = FALSE)[grepl("^--file=", commandArgs(trailingOnly = FALSE))][1])
root <- Sys.getenv("REVELIO_ANALYSES_ROOT", unset = normalizePath(file.path(dirname(script), ".."), mustWork = FALSE))
source(file.path(root, "00_config", "R", "common_v3.R"))
v3_bootstrap(c("data.table"))

dt <- data.table::data.table(
  user_id = c("a", "a", "b", "b", "c"),
  year = c(2010L, 2011L, 2010L, 2011L, 2010L),
  first_startdate_edu = c("2007-09-01", "2007-09-01", NA, NA, NA),
  first_startdate_pos = c("2008-01-01", "2008-01-01", "2009-01-01", "2009-01-01", NA)
)
v3_compute_tenure(dt, 50L)

stopifnot(
  identical(as.integer(dt[user_id == "a", round(tenure)]), c(6L, 7L)),
  identical(as.integer(dt[user_id == "b", round(tenure)]), c(1L, 2L)),
  identical(as.integer(dt[user_id == "c", round(tenure)]), 0L)
)

bins <- as.character(v3_tenure_bin(c(0, 5, 6, 10, 11, 50), 50L, 5L))
stopifnot(identical(bins, c("0-5", "0-5", "6-10", "6-10", "11-15", "46-50")))

v3_assert_unique_user_year(dt)
cat("[PASS] Shared helper synthetic checks completed.\n")
