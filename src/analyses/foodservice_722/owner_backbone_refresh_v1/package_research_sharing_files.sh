#!/bin/bash

set -eo pipefail

BASE="/home/epiga/revelio_labs"
RUN_ID="owner_backbone_refresh_20260715_021137"

PROJECT="$BASE/foodservice_722_owner_backbone_refresh_v1"
FINAL_ROOT="$BASE/output/foodservice_722_owner_backbone_refresh_v1/final_linkage/$RUN_ID"

METADATA_SOURCE="$PROJECT/research_sharing_metadata"
SHARE_ROOT="$BASE/share/${RUN_ID}_research_sharing_package"

STAGING="$SHARE_ROOT/staging"
CODE_STAGE="$STAGING/code"
META_STAGE="$STAGING/metadata"

command -v rsync >/dev/null
command -v tar >/dev/null
command -v zstd >/dev/null
command -v sha256sum >/dev/null

if [[ -e "$SHARE_ROOT" ]]; then
    echo "ERROR: package already exists:"
    echo "$SHARE_ROOT"
    exit 20
fi

test -f "$FINAL_ROOT/_FINAL_LINKAGE_COMPLETE"
test -f "$FINAL_ROOT/exact_baseline/spells/_SUCCESS"
test -f "$FINAL_ROOT/exact_baseline/people/_SUCCESS"
test -f "$FINAL_ROOT/exact_plus_fuzzy/spells/_SUCCESS"
test -f "$FINAL_ROOT/exact_plus_fuzzy/people/_SUCCESS"
test -f "$FINAL_ROOT/crosswalks/exact_baseline/_SUCCESS"
test -f "$FINAL_ROOT/crosswalks/exact_plus_fuzzy/_SUCCESS"

test -f "$METADATA_SOURCE/README.md"
test -f "$METADATA_SOURCE/INPUT_DEPENDENCIES.txt"

mkdir -p \
    "$CODE_STAGE" \
    "$META_STAGE"

copy_code_directory() {
    local source="$1"
    local destination="$2"
    local label="$3"

    if [[ ! -d "$source" ]]; then
        echo "OPTIONAL DIRECTORY NOT FOUND: $label"
        echo "$source"
        return 0
    fi

    echo "Copying: $label"

    mkdir -p "$destination"

    rsync \
        -a \
        --prune-empty-dirs \
        --include='*/' \
        --include='*.py' \
        --include='*.sh' \
        --include='*.sbatch' \
        --include='*.R' \
        --include='*.r' \
        --include='*.do' \
        --include='*.sql' \
        --include='*.md' \
        --include='*.txt' \
        --include='*.json' \
        --include='*.yml' \
        --include='*.yaml' \
        --include='*.toml' \
        --include='Makefile' \
        --exclude='__pycache__/' \
        --exclude='*.pyc' \
        --exclude='logs/' \
        --exclude='*.out' \
        --exclude='*.err' \
        --exclude='*.parquet' \
        --exclude='*.csv' \
        --exclude='*.zip' \
        --exclude='*.tar' \
        --exclude='*.tar.gz' \
        --exclude='*.tar.zst' \
        --exclude='*' \
        "$source/" \
        "$destination/"
}

copy_code_directory \
    "$BASE/foodservice_722_owner_backbone_refresh_v1" \
    "$CODE_STAGE/foodservice_722_owner_backbone_refresh_v1" \
    "Final Owner and backbone-refresh pipeline"

copy_code_directory \
    "$BASE/foodservice_722_backbone_match_v1" \
    "$CODE_STAGE/foodservice_722_backbone_match_v1" \
    "Original backbone matching pipeline"

copy_code_directory \
    "$BASE/founder_foodservice_722_owner_v1" \
    "$CODE_STAGE/founder_foodservice_722_owner_v1" \
    "Owner-title construction and validation"

copy_code_directory \
    "$BASE/pipeline_v3_spell_integrity" \
    "$CODE_STAGE/pipeline_v3_spell_integrity" \
    "Corrected V3 spell and origin pipeline"

copy_code_directory \
    "$BASE/foodservice_722_histories_v1" \
    "$CODE_STAGE/foodservice_722_histories_v1" \
    "Food-service history construction"

cp \
    "$METADATA_SOURCE/README.md" \
    "$META_STAGE/README.md"

cp \
    "$METADATA_SOURCE/INPUT_DEPENDENCIES.txt" \
    "$META_STAGE/INPUT_DEPENDENCIES.txt"

cp \
    "$FINAL_ROOT/final_linkage_manifest.json" \
    "$META_STAGE/final_linkage_manifest.json"

cp \
    "$FINAL_ROOT/founder_owner_counts.json" \
    "$META_STAGE/founder_owner_counts.json"

(
    cd "$STAGING"

    find code metadata \
        -type f \
        -printf '%P\n' |
    sort \
        > "$META_STAGE/PACKAGED_CODE_FILE_INVENTORY.txt"
)

(
    cd "$STAGING"

    find code metadata \
        -type f \
        ! -name 'SOURCE_FILE_SHA256.txt' \
        -print0 |
    sort -z |
    xargs -0 sha256sum \
        > "$META_STAGE/SOURCE_FILE_SHA256.txt"
)

EXACT_ARCHIVE="$SHARE_ROOT/restaurant_linkedin_exact_baseline_${RUN_ID}.tar.zst"

tar \
    -C "$FINAL_ROOT" \
    -I 'zstd -T4 -6' \
    -cf "$EXACT_ARCHIVE" \
    exact_baseline \
    crosswalks/exact_baseline \
    tables \
    final_linkage_manifest.json \
    founder_owner_counts.json

FUZZY_ARCHIVE="$SHARE_ROOT/restaurant_linkedin_exact_plus_fuzzy_${RUN_ID}.tar.zst"

tar \
    -C "$FINAL_ROOT" \
    -I 'zstd -T4 -6' \
    -cf "$FUZZY_ARCHIVE" \
    exact_plus_fuzzy \
    crosswalks/exact_plus_fuzzy \
    tables \
    final_linkage_manifest.json \
    founder_owner_counts.json

CODE_ARCHIVE="$SHARE_ROOT/restaurant_linkedin_reproduction_code_${RUN_ID}.tar.zst"

tar \
    -C "$STAGING" \
    -I 'zstd -T4 -6' \
    -cf "$CODE_ARCHIVE" \
    code \
    metadata

cp "$META_STAGE/README.md" "$SHARE_ROOT/README.md"

cp \
    "$META_STAGE/INPUT_DEPENDENCIES.txt" \
    "$SHARE_ROOT/INPUT_DEPENDENCIES.txt"

cp \
    "$META_STAGE/PACKAGED_CODE_FILE_INVENTORY.txt" \
    "$SHARE_ROOT/PACKAGED_CODE_FILE_INVENTORY.txt"

cp \
    "$META_STAGE/SOURCE_FILE_SHA256.txt" \
    "$SHARE_ROOT/SOURCE_FILE_SHA256.txt"

cp \
    "$META_STAGE/final_linkage_manifest.json" \
    "$SHARE_ROOT/final_linkage_manifest.json"

cp \
    "$META_STAGE/founder_owner_counts.json" \
    "$SHARE_ROOT/founder_owner_counts.json"

cd "$SHARE_ROOT"

sha256sum \
    "restaurant_linkedin_exact_baseline_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_exact_plus_fuzzy_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_reproduction_code_${RUN_ID}.tar.zst" \
    README.md \
    INPUT_DEPENDENCIES.txt \
    final_linkage_manifest.json \
    founder_owner_counts.json \
    > SHA256SUMS.txt

stat \
    -c '%s %n' \
    "restaurant_linkedin_exact_baseline_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_exact_plus_fuzzy_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_reproduction_code_${RUN_ID}.tar.zst" \
    > ARCHIVE_SIZES_BYTES.txt

du \
    -h \
    "restaurant_linkedin_exact_baseline_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_exact_plus_fuzzy_${RUN_ID}.tar.zst" \
    "restaurant_linkedin_reproduction_code_${RUN_ID}.tar.zst" \
    > ARCHIVE_SIZES_HUMAN_READABLE.txt

rm -rf "$STAGING"

touch "$SHARE_ROOT/_PACKAGE_COMPLETE"

echo
echo "RESEARCH SHARING PACKAGE COMPLETE"
echo "SHARE_ROOT=$SHARE_ROOT"

echo
echo "=== Archives ==="
ls -lh "$SHARE_ROOT"/*.tar.zst

echo
echo "=== Exact archive sizes ==="
cat "$SHARE_ROOT/ARCHIVE_SIZES_BYTES.txt"

echo
echo "=== SHA-256 checksums ==="
cat "$SHARE_ROOT/SHA256SUMS.txt"
