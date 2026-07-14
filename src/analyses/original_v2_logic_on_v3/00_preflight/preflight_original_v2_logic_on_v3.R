#!/usr/bin/env Rscript
suppressPackageStartupMessages(library(arrow))
core <- Sys.getenv("REVELIO_V3_INPUT")
colleague <- Sys.getenv("REVELIO_V3_COLLEAGUE_INPUT")
density <- Sys.getenv("REVELIO_V3_DENSITY_CROSSWALK")
out <- file.path(Sys.getenv("REVELIO_V3_OUTPUT_ROOT"), "preflight")
dir.create(out, recursive = TRUE, showWarnings = FALSE)
check <- function(label, path, needed) {
  if (!dir.exists(path)) stop("[ERROR] Missing ", label, ": ", path)
  ds <- arrow::open_dataset(path, format = "parquet")
  nm <- names(ds)
  data.frame(
    dataset = label,
    path = path,
    n_columns = length(nm),
    n_required = length(needed),
    missing = paste(setdiff(needed, nm), collapse = ";"),
    pass = length(setdiff(needed, nm)) == 0,
    stringsAsFactors = FALSE
  )
}
core_need <- c("user_id", "year", "n_patents", "n_us_citations", "first_country", "last_university_country", "first_startdate_edu", "first_startdate_pos", "first_parent_rcid", "first_rcid", "first_city", "first_state", "first_metro_area")
col_need <- c("user_id", "year", "n_patents", "first_country", "last_university_country", "first_startdate_edu", "first_startdate_pos", "n_cite_links_total", "n_cited_patents_total")
ans <- rbind(check("core_v3", core, core_need), check("colleague_v3", colleague, col_need))
ans$density_crosswalk <- density
ans$density_crosswalk_exists <- file.exists(density)
utils::write.csv(ans, file.path(out, "preflight.csv"), row.names = FALSE)
print(ans)
if (!all(ans$pass) || !all(ans$density_crosswalk_exists)) stop("[ERROR] Original-logic V3 preflight failed.")
message("[PASS] Original V2-logic on V3 preflight completed: ", out)
