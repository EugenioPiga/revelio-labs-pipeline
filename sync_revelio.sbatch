#!/usr/bin/env bash
#SBATCH --job-name=sync_revelio
#SBATCH --time=02:00:00
#SBATCH --mem=8G
#SBATCH --cpus-per-task=1
#SBATCH --output=sync_revelio_%j.out

set -euo pipefail

# ─── Edit this for the month you want (YYYYMM) ───────────────────────────
DATE_PREFIX="202507"
# ────────────────────────────────────────────────────────────────────────

# Install AWS CLI if missing
if ! command -v aws &>/dev/null; then
  pip install --upgrade --user awscli
  export PATH=$HOME/.local/bin:$PATH
fi

# Configure AWS if needed
if [[ ! -f $HOME/.aws/credentials || ! -f $HOME/.aws/config ]]; then
  aws configure
fi

DEST="$HOME/revelio_lab/$DATE_PREFIX"
mkdir -p "$DEST"

PREFIXES=(
  revelio_patents_inventor_matches
  revelio_patents_assignee_matches
  academic_company_ref
  academic_individual_position
  academic_individual_user_education
  academic_individual_user_skill
  academic_individual_user
)

for P in "${PREFIXES[@]}"; do
  aws s3 sync \
    "s3://revelio-client-standard/${DATE_PREFIX}/$P/" \
    "$DEST/$P/"
done
