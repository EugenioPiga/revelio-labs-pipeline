#!/usr/bin/env bash
set -euo pipefail
set +H

DATE_PREFIX="202507"
DEST="$HOME/revelio_lab/$DATE_PREFIX"
PREFIXES=(
  revelio_patents_inventor_matches
  revelio_patents_assignee_matches
  academic_company_ref
  academic_individual_position
  academic_individual_user_education
  academic_individual_user_skill
  academic_individual_user
)

printf "%-40s %10s %10s\n" "Dataset" "Local" "Remote"
printf "%-40s %10s %10s\n" "-------" "-----" "------"

for P in "${PREFIXES[@]}"; do
  LOCAL_DIR="$DEST/$P"
  if [[ -d "$LOCAL_DIR" ]]; then
    NLOCAL=$(find "$LOCAL_DIR" -type f | wc -l)
    NREMOTE=$(aws s3 ls "s3://revelio-client-standard/${DATE_PREFIX}/$P/" --recursive | wc -l)
    printf "%-40s %10d %10d\n" "$P" "$NLOCAL" "$NREMOTE"
  else
    printf "%-40s %10s %10s\n" "$P" "MISSING" "—"
  fi
done
